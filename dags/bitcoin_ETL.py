from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import variable

from datetime import datetime, timedelta
from typing import Final
import io
import logging
import pandas as pd

from plugins import bitcoin_utils

# 기본 설정
default_args = {
    'owner': 'sanghyeok_boo',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

def upload_to_s3(source: str, bucket: str, aws_conn_id: str, logical_date: datetime):
    logging.info(f"upload_to_s3, target: {source}")
    # logical_date 문자열을 datetime 객체로 변환
    if isinstance(logical_date, str):
        logical_date = datetime.fromisoformat(logical_date)

    if source == 'binance':
        end = logical_date-timedelta(days=1)
        end_timestamp = int(end.timestamp() * 1000) # 밀리초 타임스탬프로 변환환
        df = bitcoin_utils.get_binance_data(end=end_timestamp)
    elif source == 'upbit':
        df = bitcoin_utils.get_upbit_data(end=logical_date)
    elif source == 'yfinance':
        df = bitcoin_utils.get_yfinance_data(start=logical_date-timedelta(days=7), end=logical_date)
    else:
        raise ValueError(f"Unknown source: {source}")

    # CSV로 변환
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # S3 업로드
    date = logical_date.strftime("%Y%m%d")
    key = f"bitcoin/{source}/{source}_data_{date}.csv"
    hook = S3Hook(aws_conn_id=aws_conn_id)
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True  # 동일 파일명 교체
    )
    print(f"Uploaded data from {source} to s3://{bucket}/{key}")
    


with DAG(
    dag_id='etl_bitcoin_to_s3_redshift',
    default_args=default_args,
    description='ETL pipeline to fetch Bitcoin data and upload to S3 and Redshift',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 20),
    catchup=True,
    max_active_runs = 1,
    max_active_tasks = 1,
    tags=['ETL', 'bitcoin'],
) as dag:
    # 데이터 소스 및 설정
    DATA_SOURCES = ['binance', 'upbit', 'yfinance']
    BUCKET_NAME: Final = 'team1bkt'
    AWS_CONN_ID: Final = 'aws_conn_id'
    REDSHIFT_CONN_ID: Final = 'redshift_dev_db'
    SCHEMA: Final = 'raw_data'
    TABLE: Final = 'crypto_price'

    for source in DATA_SOURCES:
        # S3 업로드 태스크 생성
        api_to_s3_task = PythonOperator(
            task_id=f'upload_{source}_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'source': source,
                'bucket': BUCKET_NAME,
                'aws_conn_id': AWS_CONN_ID,
                'logical_date' : '{{ data_interval_start }}',
            },
        )
        # redshift table UPSERT 태스크 생성
        s3_to_redshift = S3ToRedshiftOperator(
            task_id=f'upload_{source}_to_redshift',
            s3_bucket=BUCKET_NAME,
            s3_key=f"bitcoin/{source}/{source}_data_{{{{ data_interval_start.strftime('%Y%m%d') }}}}.csv",
            schema=SCHEMA,
            table=TABLE,
            copy_options=['csv', 'IGNOREHEADER 1'], # CSV 형식 및 헤더 무시
            redshift_conn_id=REDSHIFT_CONN_ID,
            aws_conn_id=AWS_CONN_ID,
            method='UPSERT',
            upsert_keys=['record_date', 'platform'],
        )

        # DAG Task Flow
        api_to_s3_task >> s3_to_redshift