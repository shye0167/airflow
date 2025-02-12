from airflow import DAG
import datetime
import pendulum
import airflow.operators.bash import BashOperator

with DAG(
    # 직관적으로 찾기위해 DAG id랑 python file명이랑 일치시키는게 좋음.
    dag_id="dag_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whomai",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2