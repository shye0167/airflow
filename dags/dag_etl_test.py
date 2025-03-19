from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 19),
    'retries': 1,
}

def extract_from_mysql():
    """ MySQL에서 데이터를 추출하여 DataFrame으로 반환 """
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')  # Airflow UI에서 연결을 설정해야 합니다.
    sql = "SELECT * FROM d_area;"
    df = mysql_hook.get_pandas_df(sql)
    return df.to_dict()  # XCom으로 전송

def transform_data(**kwargs):
    """ 데이터를 변환 (현재는 아무 작업도 하지 않고 그대로 반환) """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract')
    return data  # 변환 없이 그대로 반환

def load_to_postgres(**kwargs):
    """ PostgreSQL로 변환된 데이터를 적재 """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform')
    df = pd.DataFrame.from_dict(data)
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')  # Airflow UI에서 설정 필요
    engine = postgres_hook.get_sqlalchemy_engine()
    
    df.to_sql('destination_table', con=engine, if_exists='replace', index=False)
    
# DAG 정의
with DAG(
    dagid='dag_etl_test',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_from_mysql,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_to_postgres,
        provide_context=True,
    )

    # Task 순서 지정
    extract_task >> transform_task >> load_task
