import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator  # airflow>=2.0

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bonus_test',
    default_args=default_args,
    start_date=pendulum.datetime(2025, 2, 25, tz='Asia/Jakarta'),
    schedule_interval="0 * * * *",   # jalan tiap jam
    catchup=False,
    max_active_runs=1,
    tags=["dataengineer"],
)

lion_parcell_bonus_test_stg = BashOperator(
    task_id='lion_parcell_bonus_test_stg',
    bash_command='python /opt/airflow/public/lion_parcell_bonus_test_stg.py || exit 3',
    dag=dag,
)

lion_parcell_bonus_test = BashOperator(
    task_id='lion_parcell_bonus_test',
    bash_command='python /opt/airflow/public/lion_parcell_bonus_test.py || exit 3',
    dag=dag,
)

lion_parcell_bonus_test_stg >> lion_parcell_bonus_test
