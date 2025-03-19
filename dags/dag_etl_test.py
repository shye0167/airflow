from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 3, 14, tz="UTC"),
    
}

def extract_from_mysql():
    """ MySQL에서 데이터를 추출하여 DataFrame으로 반환 """
    # MySQL 연결 설정
    mysql_conn = mysql.connector.connect(
        host="localhost",  # MySQL 서버 호스트명
        user="airflow",    # MySQL 사용자명
        password="airflow",# MySQL 비밀번호
        database="airflow_db"  # 데이터베이스명
    )
    
    sql = "SELECT * FROM d_area;"
    df = pd.read_sql(sql, mysql_conn)
    mysql_conn.close()  # 연결 종료
    return df.to_dict()  # XCom으로 전송

def transform_data(**kwargs):
    """ 데이터를 변환 (예제에서는 단순 변환) """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract')
    df = pd.DataFrame.from_dict(data)
    # 예제 변환: 특정 컬럼 값 변경
    df['new_column'] = df['existing_column'] * 2  # 예제 변환
    return df.to_dict()

def load_to_postgres(**kwargs):
    """ PostgreSQL로 변환된 데이터를 적재 """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform')
    df = pd.DataFrame.from_dict(data)
    
    # PostgreSQL 연결 설정
    postgres_conn_str = 'postgresql://airflow:airflow@localhost:5432/shkim'  # PostgreSQL URI
    engine = create_engine(postgres_conn_str)
    
    df.to_sql('d_area', con=engine, if_exists='replace', index=False)

# DAG 정의
with DAG(
    dag_id ='dags_etl_test',
    default_args=default_args,
    # schedule_interval='@daily',
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
