from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow import AirflowException

import logging
import os
from glob import glob
from datetime import datetime, timedelta

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    return hook.get_conn().cursor()
def load_all_jsons_into_list(path_to_json):

    configs = []
    for f_name in glob(path_to_json+ '/*.py'):
        # logging.info(f_name)
        with open(f_name) as f:
            dict_text = f.read()
            try:
                dict = eval(dict_text)
            except Exception as e:
                logging.info(str(e))
                raise
            else:
                configs.append(dict)

    return configs
def find(table_name, table_confs):
    """
    scan through table_confs and see if there is a table matching table_name
    """
    for table in table_confs:
        if table.get("table") == table_name:
            return table

    return None

# 기본 설정
default_args = {
    'owner': 'sanghyeok_boo',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(
    dag_id='bitcoin_elt',
    default_args=default_args,
    schedule_interval='@once',
    max_active_runs=1,
    catchup=False,
    start_date=datetime(2025, 1, 20),
    tags=['ELT', 'bitcoin']
)
def bitcoin_elt_dag():
    dag_root_path = os.path.dirname(os.path.abspath(__file__))
    tables_load = [
        'btcusd_summary',
        'btc_latest_price',
        'btc_platform_summary'
    ]

    table_confs = load_all_jsons_into_list(dag_root_path + "/config/")

    trigger_dag=TriggerDagRunOperator(
        task_id='trigger_dag',
        trigger_dag_id='etl_bitcoin_to_s3_redshift',
        wait_for_completion=True, # 트리거된 dag가 종료되어야 현재 task trigeer_dag가 success 처리 됨
        poke_interval=10,
    )

    @task
    def create_summary_task(table_confs: dict):
        if table_confs is None:
            raise AirflowException('table_confs in creat_summary_task() is None')
        
        table, schema, select_sql = table_confs['table'], table_confs['schema'], table_confs['sql']
        logging.info(f"Schema: {schema}")
        logging.info(f"Table: {table}")
        logging.info(f"Executing SQL: {select_sql}")

        cur = get_Redshift_connection()

        try:
            # 임시 테이블 생성
            sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};
                    CREATE TABLE {schema}.temp_{table} AS {select_sql};"""
            cur.execute(sql)

            # 레코드 개수 검증
            cur.execute(f"""SELECT COUNT(1) FROM {schema}.temp_{table}""")
            count = cur.fetchone()[0]
            if count == 0:
                raise ValueError(f"{schema}.{table} didn't have any record")

            # 테이블 교체
            sql = f"""DROP TABLE IF EXISTS {schema}.{table};
                    ALTER TABLE {schema}.temp_{table} RENAME TO {table};
                    COMMIT;"""
            logging.info(f"Executing: {sql}")
            cur.execute(sql)

        except Exception as e:
            cur.execute("ROLLBACK")
            logging.error('Failed to execute SQL. Completed ROLLBACK!')
            raise AirflowException(str(e))

        finally:
            cur.close()
    # override를 통한 task_id 명시
    trigger_dag >> [create_summary_task
                        .override(task_id=f'create_summary_{table_name}')
                        (find(table_name, table_confs)) for table_name in tables_load]

dag = bitcoin_elt_dag()