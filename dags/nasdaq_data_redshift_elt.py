from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
import logging


# Redshift 연결 설정
ANALYTICS_SCHEMA = "analysis"
NASDAQ_ANALYSIS_TBL = "nasdaq_analysis"
NASDAQ_VOLUME_SECTOR = "nasdaq_sector_volume"
REDSHIFT_CONN_ID = "proj3-group-rs" 


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    conn = hook.get_conn()
    return conn.cursor()


def create_nasdaq_profit_analysis():
    #Redshift에 연결
    cur = get_Redshift_connection()
    try:
        drop_table_sql = f"""DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{NASDAQ_ANALYSIS_TBL}; """
        profit_table_ctas_sql = f"""
                CREATE TABLE {ANALYTICS_SCHEMA}.{NASDAQ_ANALYSIS_TBL} AS (
                    SELECT 
                        sp.record_date record_date,
                        sp.ticker ticker,  
                        sector,
                        sp.close_price - sp.open_price profit,
                        ((sp.close_price - sp.open_price) / sp.open_price) * 100 daily_return
                    FROM raw_data.stock_price sp
                    JOIN raw_data.stock_ticker st ON sp.ticker = st.ticker
                    JOIN raw_data.weight wi ON wi.ticker = st.ticker
                    WHERE wi.ticker_type = 'NASDAQ');
                    """
        logging.info("Creating Redshift table for NASDAQ profit analysis....")

        cur.execute("BEGIN;")
        cur.execute(drop_table_sql)
        cur.execute("COMMIT;")
        cur.execute("BEGIN;")
        cur.execute(profit_table_ctas_sql)
        cur.execute("COMMIT;")
        logging.info(f"NASDAQ profit table {ANALYTICS_SCHEMA}.{NASDAQ_ANALYSIS_TBL} created successfully.")
    except Exception as e:
        logging.error(f"Error during {ANALYTICS_SCHEMA}.{NASDAQ_ANALYSIS_TBL} table creation: {e}")
        cur.execute("ROLLBACK;")
        raise

def create_nasdaq_volume_table():
    try:
        cur = get_Redshift_connection()

        drop_table_sql = f"""DROP TABLE IF EXISTS {ANALYTICS_SCHEMA}.{NASDAQ_VOLUME_SECTOR}; """
        volume_table_ctas_sql = f"""
                            CREATE TABLE {ANALYTICS_SCHEMA}.{NASDAQ_VOLUME_SECTOR} AS (
                                SELECT  
                                    sp.record_date record_date,
                                    sp.ticker ticker, 
                                    sector,
                                    sp.close_price close_price,
                                    sp.trade_volume trade_volume,
                                    weight
                                FROM raw_data.stock_price sp
                                JOIN raw_data.stock_ticker st ON sp.ticker = st.ticker
                                JOIN raw_data.weight wi ON wi.ticker = st.ticker
                                WHERE wi.ticker_type = 'NASDAQ');
                            """
        logging.info("Creating Redshift table for NASDAQ trade volume analysis....")
        cur.execute("BEGIN;")
        cur.execute(drop_table_sql)
        cur.execute("COMMIT;")
        cur.execute("BEGIN;")
        cur.execute(volume_table_ctas_sql)
        cur.execute("COMMIT;")
        logging.info(f"NASDAQ volume calculation table {ANALYTICS_SCHEMA}.{NASDAQ_VOLUME_SECTOR} created successfully.")
    except Exception as e:
        logging.error(f"Error during {ANALYTICS_SCHEMA}.{NASDAQ_VOLUME_SECTOR} table creation: {e}")
        cur.execute("ROLLBACK;")
        raise

# DAG configuration
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="nasdaq_elt_dag",
    default_args=DEFAULT_ARGS,
    description="ELT from Redshift for data visualization",
    schedule_interval="0 0 * * 1-5",
    start_date= datetime(2025, 1, 31),
    max_active_runs=1,
    catchup=False,
    tags=["nasdaq", "s3", "redshift","elt"],
) as dag:

    #nasdaq 이익 분석을 위한 table 생성 Task
    nasdaq_profit_analysis_table_task = PythonOperator(
        task_id="create_nasdaq_profit_analysis_table",
        python_callable=create_nasdaq_profit_analysis,
    )

    #거래량 분석을 위한 table 생성 Task
    nasdaq_volume_table_task = PythonOperator(
        task_id="create_nasdaq_volume_analysis_table",
        python_callable=create_nasdaq_volume_table,
    )

    create_analysis_table = DummyOperator(task_id="create_tables_for_analysis_schema")

create_analysis_table >> [nasdaq_profit_analysis_table_task,nasdaq_volume_table_task]