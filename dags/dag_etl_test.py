from airflow import DAG
from airflow.operators.python import PythonOperator
from mysql.connector import connect
import pandas as pd
from psycopg2 import connect as pg_connect
from datetime import datetime

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 19),
    'retries': 1,
}

def extract_from_mysql():
    """ MySQL에서 데이터를 추출하여 DataFrame으로 반환 """
    # MySQL 연결 정보 하드코딩
    mysql_conn = connect(
        host='localhost',  # MySQL 서버 호스트
        user='airflow',    # MySQL 사용자명
        password='airflow', # MySQL 비밀번호
        database='airflow_db'  # 데이터베이스 이름
    )
    sql = "SELECT * FROM D_AREA;"
    df = pd.read_sql(sql, mysql_conn)
    mysql_conn.close()
    return df.to_dict()  # XCom으로 전송

def transform_data(**kwargs):
    """ 데이터를 변환 (예제에서는 변환을 단순히 pass) """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract')
    df = pd.DataFrame.from_dict(data)
    # 여기에 데이터 변환 로직을 추가할 수 있습니다.
    # 예시: df['new_column'] = df['existing_column'] * 2  # 예시 변환
    return df.to_dict()

def load_to_postgres(**kwargs):
    """ PostgreSQL로 변환된 데이터를 적재 """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform')
    df = pd.DataFrame.from_dict(data)
    
    # PostgreSQL 연결 정보 하드코딩
    pg_conn = pg_connect(
        host='localhost',   # PostgreSQL 서버 호스트
        dbname='shkim',     # PostgreSQL 데이터베이스 이름
        user='shkim',       # PostgreSQL 사용자명
        password='shkim'    # PostgreSQL 비밀번호
    )
    cursor = pg_conn.cursor()

    # 데이터프레임을 PostgreSQL 테이블에 삽입
    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO d_area (AREA_ID, AREA_NM, SORT_ORDER, DATA_WORK_DTTM) VALUES (%s, %s, %s, %s)",
            (row['AREA_ID'], row['AREA_NM'], row['SORT_ORDER'], row['DATA_WORK_DTTM'])
        )

    pg_conn.commit()
    cursor.close()
    pg_conn.close()

# DAG 정의
with DAG(
    'mysql_to_postgres_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_from_mysql,
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True,
        dag=dag,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_to_postgres,
        provide_context=True,
        dag=dag,
    )

    # Task 순서 지정
    extract_task >> transform_task >> load_task
