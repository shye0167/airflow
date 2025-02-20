from airflow import DAG
import pendulum
# import datetime
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
