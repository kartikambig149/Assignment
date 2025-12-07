from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
with DAG('fetch_stock_daily', start_date=datetime(2025,1,1), schedule_interval='@hourly', catchup=False):
    BashOperator(task_id='fetch', bash_command='python /opt/airflow/scripts/fetch_and_store.py')
