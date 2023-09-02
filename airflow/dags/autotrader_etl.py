from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from scripts.autotrader_extract import *

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today(),
    'retries': 1,
    'retry_delay': timedelta(seconds = 30)
}

with DAG(dag_id = 'Autotrader_ETL_DAG', default_args = default_args, schedule_interval = timedelta(1), catchup = False) as dag:
    extract = PythonOperator(
        task_id = 'extract_listings', 
        python_callable = extract_listings, 
        op_kwargs = {'current_date': current_date})
    create_listings_raw_table = BigQueryCreateEmptyTableOperator(
        task_id = 'create_listings_raw_table',
        dataset_id = 'autotrader_staging',
        table_id = 'listings_raw',
        project_id = 'autotrader-toyota-dashboard',
        schema_fields = [
            {'name': 'price', 'type': 'STRING'},
            {'name': 'odometer', 'type': 'STRING'},
            {'name': 'year', 'type': 'STRING'},
            {'name': 'model', 'type': 'STRING'},
            {'name': 'type', 'type': 'STRING'},
            {'name': 'suburb', 'type': 'STRING'},
            {'name': 'state', 'type': 'STRING'}
        ],
        gcp_conn_id = 'google_cloud'
    )
    insert_raw_data_into_listings_raw = GCSToBigQueryOperator(
        task_id = 'insert_raw_data_into_listings_raw',
        bucket = 'autotrader-raw',
        source_objects = f'autotrader-raw-{current_date}.json',
        destination_project_dataset_table = 'autotrader-toyota-dashboard.autotrader_staging.listings_raw',
        schema_fields = [
            {'name': 'price', 'type': 'STRING'},
            {'name': 'odometer', 'type': 'STRING'},
            {'name': 'year', 'type': 'STRING'},
            {'name': 'model', 'type': 'STRING'},
            {'name': 'type', 'type': 'STRING'},
            {'name': 'suburb', 'type': 'STRING'},
            {'name': 'state', 'type': 'STRING'}
        ],
        source_format = 'NEWLINE_DELIMITED_JSON',
        write_disposition = 'WRITE_TRUNCATE',
        gcp_conn_id = 'google_cloud'
    )
    remove_nulls = BigQueryExecuteQueryOperator(
        task_id = 'remove_nulls',
        sql = ''' DELETE autotrader-toyota-dashboard.autotrader_staging.listings_raw
        WHERE
        NOT EXISTS (
        SELECT
        *
        FROM
        autotrader-toyota-dashboard.autotrader_staging.listings_raw
        WHERE
        NOT ( price IS NULL
        OR odometer IS NULL
        OR year IS NULL
        OR MODEL IS NULL
        OR type IS NULL
        OR suburb IS NULL
        OR state IS NULL ));''',
        gcp_conn_id = 'google_cloud'
        )


    extract >> create_listings_raw_table >> insert_raw_data_into_listings_raw >> remove_nulls
    


