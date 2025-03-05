from config.on_failure_callback_to_kakao import on_failure_callback_to_kakao
from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator


with DAG (
    # 직관적으로 찾기위해 DAG id랑 python file명이랑 일치시키는게 좋음.
    dag_id = 'dags_on_failure_callback_to_kakao',
    schedule="*/20 * * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args= {
        'on_failure_callback' : on_failure_callback_to_kakao,
        'execution_timeout' : timedelta(seconds=60)
    }
) as dag:

    task_slp_90 = BashOperator(
        task_id="task_slp_90",
        bash_command="sleep 90",
    )

    task_ext_1 = BashOperator(
        trigger_rule='all_done',
        task_id="task_ext_1",
        bash_command="exit 1",
    )

    task_slp_90 >> task_ext_1