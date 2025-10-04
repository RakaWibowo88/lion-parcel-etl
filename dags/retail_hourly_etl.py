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
    'retail_hourly_etl',
    default_args=default_args,
    start_date=pendulum.datetime(2025, 2, 25, tz='Asia/Jakarta'),
    schedule_interval="0 * * * *",   # jalan tiap jam
    catchup=False,
    max_active_runs=1,
    tags=["dataengineer"],
)

source_transaction_lion_parcel = BashOperator(
    task_id='source_transaction_lion_parcel',
    bash_command='python /opt/airflow/public/source_transaction_lion_parcel.py || exit 3',
    dag=dag,
)

retail_transactions = BashOperator(
    task_id='retail_transactions',
    bash_command='python /opt/airflow/public/retail_transactions.py || exit 3',
    dag=dag,
)

source_transaction_lion_parcel >> retail_transactions
