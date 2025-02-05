from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import yfinance as yf
import pandas as pd
import os

# 기본 DAG 설정
default_args = {
    'owner': 'JiYeon',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='Gold_Dag_redshift',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',  # 매일 실행
    catchup=False,
    max_active_runs=1,
) as dag:
    # 조회할 티커, S3 버킷, Redshift 관련 변수 설정
    tickers = ["NEM", "GLD", "GDX"]
    s3_bucket = "team1bkt"          # 실제 S3 버킷 이름으로 변경
    redshift_schema = "raw_data"        # Redshift 스키마
    redshift_table = "gold"             # Redshift 테이블 (누적 테이블)

    @task
    def fetch_and_upload_yfinance_data(run_date: str) -> list:
        """
        최근 30일치 데이터를 각 티커별로 조회한 후,
        필요한 컬럼(Date, Open, High, Low, Close, Volume)만 남기고,
        각 티커의 데이터를 합쳐 날짜별로 분리하여 CSV 파일을 생성합니다.
        생성된 CSV 파일은 S3 경로: yfinance/{날짜}.csv 로 업로드되며,
        이미 해당 날짜의 파일이 있다면 업로드를 건너뜁니다.
        업로드된(새로운) 파일들의 S3 key 목록(리스트)을 반환합니다.
        """
        s3_hook = S3Hook(aws_conn_id="aws_conn_id1")
        dfs = []
        for ticker in tickers:
            # 최근 30일치 일별 데이터 조회 (period="1mo", interval="1d")
            df = yf.Ticker(ticker).history(period="1mo", interval="1d")
            df.reset_index(inplace=True)
            # 필요한 컬럼 선택 후, Redshift 테이블 스키마에 맞게 컬럼명 변경
            df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df['ticker'] = ticker
            df.rename(columns={
                'Date': 'date',
                'Open': 'open_price',
                'High': 'high_price',
                'Low': 'low_price',
                'Close': 'close_price',
                'Volume': 'volume'
            }, inplace=True)
            dfs.append(df)
        
        # 모든 티커의 데이터를 하나로 결합
        combined_df = pd.concat(dfs, ignore_index=True)
        # date 컬럼을 문자열(YYYY-MM-DD)로 변환 (시간 정보 제거)
        combined_df['date'] = pd.to_datetime(combined_df['date']).dt.strftime("%Y-%m-%d")
        # 고유 날짜 목록 (정렬)
        unique_dates = sorted(combined_df['date'].unique())
        uploaded_keys = []
        for date_str in unique_dates:
            # S3 키: yfinance/{날짜}.csv (즉, yfinance/ 폴더 바로 아래)
            s3_key = f"yfinance_gold/{date_str}.csv"
            # S3에 이미 해당 키가 존재하는지 확인
            if s3_hook.check_for_key(key=s3_key, bucket_name=s3_bucket):
                # 이미 파일이 존재하면 업로드 건너뜀
                continue
            # 해당 날짜의 데이터만 필터링
            df_date = combined_df[combined_df['date'] == date_str]
            # 로컬 임시 CSV 파일 생성 (예: /tmp/yfinance_2025-02-03.csv)
            local_file = f"/tmp/yfinance_gold_{date_str}.csv"
            df_date.to_csv(local_file, index=False)
            # S3에 파일 업로드
            s3_hook.load_file(
                filename=local_file,
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True
            )
            # 로컬 임시 파일 삭제
            if os.path.exists(local_file):
                os.remove(local_file)
            uploaded_keys.append(s3_key)
        return uploaded_keys

    # 실행일(예: "2025-02-04")은 Airflow 템플릿 변수로 전달 (여기선 run_date로 사용하지만 S3 키는 날짜별로 생성됨)
    execution_date = "{{ ds }}"
    new_uploaded_keys = fetch_and_upload_yfinance_data(execution_date)

    # 동적 매핑을 사용하여, 업로드된 각 CSV 파일(신규 파일만)을 Redshift에 적재하는 태스크 생성
    redshift_tasks = S3ToRedshiftOperator.partial(
        task_id="s3_to_redshift_task",  # 필수 task_id 지정
        s3_bucket=s3_bucket,
        schema=redshift_schema,
        table=redshift_table,
        copy_options=["csv", "IGNOREHEADER 1"],
        method='APPEND',  # 기존 데이터에 추가
        redshift_conn_id='redshift_conn_id',
        aws_conn_id='aws_conn_id1'
    ).expand(
        s3_key=new_uploaded_keys
    )
