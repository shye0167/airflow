from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import psycopg2
import cx_Oracle

# 1. Extract: 오라클에서 데이터 추출
def extract_data():
    dsn = cx_Oracle.makedsn("165.244.90.33", 1525, service_name="GLDBDEV")
    conn = cx_Oracle.connect(user="CIDSDM", password="CIDSDM123!#", dsn=dsn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM CIDSDM.D_AREA")
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data

# 2. Transform: 데이터 변환
def transform_data(ti):
    raw_data = ti.xcom_pull(task_ids='extract')
    transformed_data = [{"id": row[0], "value": row[1]} for row in raw_data]
    return transformed_data

# 3. Load: PostgreSQL에 적재
def load_data(ti):
    transformed_data = ti.xcom_pull(task_ids='transform')
    conn = psycopg2.connect(
        host="172.28.0.3",
        database="shkim",
        user="shkim",
        password="shkim"
    )
    cur = conn.cursor()
    for row in transformed_data:
        cur.execute("INSERT INTO your_postgres_table (id, value) VALUES (%s, %s)", (row["id"], row["value"]))
    conn.commit()
    cur.close()
    conn.close()
    print("Data successfully loaded into PostgreSQL")

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 17),
    'retries': 1,
}

dag = DAG(
    dag_id='oracle_to_postgres_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL process from Oracle to PostgreSQL',
    tags=['etl'],
)

# `op_kwargs`에서 `ti`를 주입하는 방식이 Airflow 2.x에서 작동하도록 수정
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    op_kwargs={'ti': '{{ task_instance }}'},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    op_kwargs={'ti': '{{ task_instance }}'},
    dag=dag,
)

# DAG 실행 순서
extract_task >> transform_task >> load_task
