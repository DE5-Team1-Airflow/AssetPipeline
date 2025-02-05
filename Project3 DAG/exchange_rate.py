from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import logging

# Redshift 연결 함수
def get_redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_conn_id')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# 데이터 수집 작업 (최근 30일 데이터만 가져오기)
@task
def get_historical_prices(symbols):
    records = []
    start_date = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')  # 최근 30일 기준

    for symbol in symbols:
        ticker = yf.Ticker(symbol)
        data = ticker.history(start=start_date).reset_index()  # 최근 30일 데이터 가져오기

        # KRW 제거하여 currency 변경
        currency = symbol.replace("=X", "").replace("KRW", "")

        # currency 컬럼 추가 후 중복 제거 (같은 currency에 대한 동일 날짜 중복 제거)
        data["Currency"] = currency
        data = data.drop_duplicates(subset=["Date", "Currency"], keep="first")

        for _, row in data.iterrows():
            date = row["Date"].strftime('%Y-%m-%d')  # 날짜를 'YYYY-MM-DD' 문자열로 변환
            open_price = round(row["Open"], 2) if not pd.isna(row["Open"]) else None
            high_price = round(row["High"], 2) if not pd.isna(row["High"]) else None
            low_price = round(row["Low"], 2) if not pd.isna(row["Low"]) else None
            close_price = round(row["Close"], 2) if not pd.isna(row["Close"]) else None

            records.append([date, currency, open_price, high_price, low_price, close_price])
    return records

# 데이터 검증 작업
@task
def validate_records(records):
    for record in records:
        if len(record) != 6:
            raise ValueError(f"Invalid record length: {record}")
        if not isinstance(record[0], str):
            raise ValueError(f"Invalid date format: {record[0]}")  # date가 문자열인지 확인
        if not isinstance(record[1], str):
            raise ValueError(f"Invalid currency format: {record[1]}")
    return records

# 데이터 로드 작업 (중복 방지 로직 추가)
@task
def load_data(schema, table, records):
    logging.info("Load started")
    cur = get_redshift_connection()
    try:
        cur.execute("BEGIN;")
        
        # 기존 테이블과 일치하도록 `date` 컬럼을 TEXT 형식으로 변경
        cur.execute(f"""
            CREATE TEMP TABLE temp_exchange_rates (
                date TEXT,  -- 날짜를 'YYYY-MM-DD' 문자열로 저장
                currency VARCHAR(10),
                open_price DECIMAL(10, 2),
                high_price DECIMAL(10, 2),
                low_price DECIMAL(10, 2),
                close_price DECIMAL(10, 2)
            );
        """)
        logging.info("Temporary table created")

        # 데이터 삽입
        insert_query = """
            INSERT INTO temp_exchange_rates (date, currency, open_price, high_price, low_price, close_price)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        cur.executemany(insert_query, records)
        logging.info(f"Inserted {len(records)} records into temporary table")

        # 기존 데이터 중복 제거 후 삽입 (같은 날짜 & 같은 currency 중복 제거)
        cur.execute(f"""
            DELETE FROM {schema}.{table}
            USING temp_exchange_rates
            WHERE {schema}.{table}.date = temp_exchange_rates.date
            AND {schema}.{table}.currency = temp_exchange_rates.currency;
            
            INSERT INTO {schema}.{table} (date, currency, open_price, high_price, low_price, close_price)
            SELECT DISTINCT date, currency, open_price, high_price, low_price, close_price
            FROM temp_exchange_rates;
        """)
        logging.info("Data inserted into original table with duplicate handling")

        cur.execute("COMMIT;")
        logging.info("Load completed successfully")
    except Exception as error:
        logging.error(f"Error loading data: {error}")
        cur.execute("ROLLBACK;")
        raise

# DAG 정의
with DAG(
    dag_id='UpdateExchangeRates1',
    start_date=datetime(2024, 12, 30),
    catchup=False,
    tags=['API'],
    schedule='0 10 * * *'
) as dag:
    
    symbols = ["JPYKRW=X", "USDKRW=X", "EURKRW=X"]

    exchange_rate_records = get_historical_prices(symbols)
    validated_records = validate_records(exchange_rate_records)
    load_data("raw_data", "exchange_rates", validated_records)
