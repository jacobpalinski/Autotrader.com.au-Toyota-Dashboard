from datetime import date, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from scripts.autotrader_extract import *

default_args = {
    'owner': 'airflow',
    'start_date': date.today(),
    'retries': 1,
    'retry_delay': timedelta(seconds = 30)
}

with DAG( dag_id = 'Autotrader_ETL_DAG', default_args = default_args, schedule_interval = timedelta(1), catchup = False) as dag:
    extract = PythonOperator(task_id = 'extract_listings', python_callable = extract_listings)
    success = BashOperator(task_id = 'success', bash_command = 'echo "Test successful"')
    extract >> success
    


