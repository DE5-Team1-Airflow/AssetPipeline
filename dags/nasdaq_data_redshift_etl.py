from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import yfinance as yf
import pandas as pd
import os
import logging
import requests_cache
from requests_ratelimiter import LimiterSession
from pyrate_limiter import RequestRate, Duration, Limiter
from airflow.utils.trigger_rule import TriggerRule

REDSHIFT_TABLE = "stock_price"
REDSHIFT_SCHEMA = "raw_data"
TEMP_TABLE = f"{REDSHIFT_TABLE}_temp"

# AWS 연결 설정
S3_BUCKET = "team1bkt" 
S3_KEY_PREFIX = "nasdaq" 
REDSHIFT_CONN_ID = "proj3-group-rs"
AWS_ACCESS_KEY = Variable.get("aws_project3_access_key") 
AWS_SECRET_KEY = Variable.get("aws_project3_secret_key") 
IAM_ROLE = Variable.get('project3_rs_iam')


BATCH_SIZE = 30 # 한번에 수집할 ticker Batch 수
TICKER_LIST = 'nasdaq_tickers' # Variable에서 가지고 올 Ticker list

# 캐시 및 요청 제한 설정
def setup_session():
    session = requests_cache.CachedSession('yfinance.cache')
    rate = RequestRate(5, Duration.SECOND * 1)  # 1초당 최대 5개의 요청
    limiter = Limiter(rate)
    return LimiterSession(limiter=limiter, session=session)


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    conn = hook.get_conn()
    return conn.cursor()

# 데이터를 가지고 오기 위한 시작일,마지막일 계산
def get_execution_date_range(input_date):
    input_date = input_date[:10] #입력받은 string에서 년,월,일 까지만 추출
    input_date = datetime.strptime(input_date, '%Y-%m-%d')
    if input_date.weekday() in [3,4]:  # Thursday,Friday
        start_date = input_date - timedelta(days=3)
        end_date = input_date

    else:  # Monday, Tuesday, Wednesday
        start_date = input_date - timedelta(days=5)
        end_date = input_date       
    return start_date, end_date

# List of tickers to fetch
def get_tickers():
    # Fetch the Variable using the key 'nasdaq'
    ticker_list = Variable.get(TICKER_LIST, deserialize_json=True)
    # Ensure the fetched data is valid
    if ticker_list and isinstance(ticker_list, list):
        return ticker_list
    else:
        raise ValueError("Tickers variable is missing or improperly formatted.")

# Ticker 데이터 추출 후 S3에 저장
def collect_and_upload_to_s3(tickers, data_interval_end, **kwargs):
    logging.info(f'length of tickers: {len(tickers)}')
    start_date, end_date = get_execution_date_range(data_interval_end)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    logging.info(f'Collecting data from {start_date_str} to the day before {end_date_str}')
    custom_session = setup_session() #데이터 수집량 추가 설정 함수
    batch_size = BATCH_SIZE  # 한 번에 batch size 만큼 ticker 처리

    all_data = []
    try:
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            data = yf.download(
                tickers=batch,
                start=start_date_str,
                end=end_date_str,
                group_by='ticker',
                auto_adjust=False,
                session=custom_session
            )
            all_data.append(data)
        #다운로드 받은 데이터를를 뭉친 DataFrame으로 저장
        combined_all_data = pd.concat(all_data) 

        # 티커 별로 DataFrame 분리 후 행별로 결합을 위한 리스트
        stacked_df = []
        for ticker in tickers:
            logging.info(f'Processing data for {ticker}')
            ticker_data = combined_all_data[ticker].copy()
            ticker_data['Ticker'] = ticker  # Ticker 명이 있는 컬럼 추가
            stacked_df.append(ticker_data)

        combined_data = pd.concat(stacked_df)
        combined_data.dropna(axis=0, how='any', inplace=True)
        combined_data['Date'] = combined_data.index
        final_data = combined_data[['Ticker', 'Date', 'Open', 'High', 
                                    'Low', 'Close', 'Volume']]
        final_data.columns = ['ticker', 'date', 'open_price', 
                                'highest_price', 'lowest_price', 
                                'closing_price', 'volume']
        final_data.reset_index(drop=True, inplace=True)
        final_data['volume'] = final_data['volume'].astype(int)
        final_data['highest_price'] = final_data['highest_price'].apply(lambda x: round(x,2))
        final_data['open_price'] = final_data['open_price'].apply(lambda x: round(x,2))
        final_data['lowest_price'] = final_data['lowest_price'].apply(lambda x: round(x,2))
        final_data['closing_price'] = final_data['closing_price'].apply(lambda x: round(x,2))
        # S3로 저장
        s3_key = f"{S3_KEY_PREFIX}/{end_date_str}-nasdaq.csv"
        s3_location = f"s3://{S3_BUCKET}/{s3_key}"
        final_data.to_csv(
            s3_location,
            index=False,
            storage_options={"key": AWS_ACCESS_KEY, 
                            "secret": AWS_SECRET_KEY,
                            "s3_additional_kwargs":{"region_name": "us-west-2"},}
        )
        kwargs['ti'].xcom_push(key='end_date', 
                                value=end_date_str)
    
    except Exception as e:
        logging.error(f"Failed to fetch and upload data: {e}")
        raise

# Redshift 테이블에 Upsert하기
def upsert_to_redshift(**kwargs):

    end_date = kwargs['ti'].xcom_pull(
        key='end_date',
        task_ids='collect_and_save_to_s3',
    )

    logging.info(f"End Date: {end_date} received.")
    if not end_date:
        raise ValueError("end_date is missing from XCom.")


    #Redshift에 연결    
    cur = get_Redshift_connection()
    try:
        # 임시 테이블 생성 쿼리
        TEMP_TABLE = f"{REDSHIFT_TABLE}_nasdaq_temp"
        temp_table_sql = f"""
                            CREATE TEMP TABLE {TEMP_TABLE} (LIKE {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE});
                        """
        
        # 임시 테이블 생성
        try:
            cur.execute("BEGIN;")
            cur.execute(temp_table_sql)
            cur.execute("COMMIT;")
            logging.info(f"Temporary table {TEMP_TABLE} created.")
        except Exception as e:
            logging.error(f"Error during table creation operation: {e}")
            cur.execute("ROLLBACK;")
            raise

        # S3에서 임시 테이블로 적제하는 쿼리
        temp_table_copy_sql = f"""
                                COPY {TEMP_TABLE}
                                FROM 's3://{S3_BUCKET}/{S3_KEY_PREFIX}/{end_date}-nasdaq.csv'
                                IAM_ROLE '{IAM_ROLE}'
                                DELIMITER ',' 
                                DATEFORMAT 'auto' 
                                TIMEFORMAT 'auto' 
                                IGNOREHEADER 1;
                                """
        
        # UPSERT 작업
        upsert_sql = f"""
        DELETE FROM {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}
        USING {TEMP_TABLE}
        WHERE {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}.ticker = {TEMP_TABLE}.ticker
        AND {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}.record_date = {TEMP_TABLE}.record_date;

        INSERT INTO {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}
        SELECT * FROM {TEMP_TABLE};
        """
        cur.execute("BEGIN;")
        cur.execute(temp_table_copy_sql)
        cur.execute("COMMIT;")
        cur.execute("BEGIN;")
        cur.execute(upsert_sql)
        cur.execute("COMMIT;")
        logging.info(f"Data upserted into {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}.")
    except Exception as e:
        logging.error(f"Error during Redshift upsert operation: {e}")
        cur.execute("ROLLBACK;")
        raise

# DAG configuration
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "wait_for_downstream": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

default_dag = DAG(
    dag_id="nasdaq_etl_dags",
    default_args=DEFAULT_ARGS,
    description="Collect ticker data from yfinance and load to Redshift",
    schedule_interval="0 0 * * 1-5",
    start_date=datetime(2024,12,24),
    catchup=True,
    max_active_runs=1,
    tags=["nasdaq", "s3", "redshift","etl"],
)

# yfinance에서 수집한 데이터 s3에 저장하는 Task
collect_data_task = PythonOperator(
    task_id="collect_and_save_to_s3",
    python_callable=collect_and_upload_to_s3,
    op_kwargs={
        "tickers": get_tickers(),
        "data_interval_end": "{{ data_interval_end }}"
    },
    dag=default_dag,
)

# 새로 추출한 데이터를 Redshift에 적재하는 Task
upsert_data_task = PythonOperator(
    task_id="upsert_to_redshift",
    python_callable=upsert_to_redshift,
    dag=default_dag,
    provide_context=True,
)

trigger_nasdaq_dashboard_dag = TriggerDagRunOperator(
    task_id="trigger_nasdaq_dashboard_dag",
    trigger_dag_id="nasdaq_elt_tasks",
    wait_for_completion=False,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

collect_data_task >> upsert_data_task >> trigger_nasdaq_dashboard_dag