import pendulum
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig
from scripts.autotrader_extract import current_date, extract_listings

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today(),
    'retries': 0,
    'retry_delay': timedelta(seconds = 60)
}

with DAG(dag_id = 'Autotrader_ETL_DAG', default_args = default_args, schedule_interval = timedelta(1), catchup = False) as dag:
    extract = PythonOperator(
        task_id = 'extract_listings', 
        python_callable = extract_listings, 
        op_kwargs = {'current_date': current_date})
    create_listings_raw_table = BigQueryCreateEmptyTableOperator(
        task_id = 'create_listings_raw_table',
        dataset_id = 'autotrader_raw',
        table_id = 'listings_raw',
        project_id = 'autotrader-toyota-dashboard',
        schema_fields = [
            {'name': 'date', 'type': 'STRING'},
            {'name': 'price', 'type': 'STRING'},
            {'name': 'odometer', 'type': 'STRING'},
            {'name': 'year', 'type': 'STRING'},
            {'name': 'car_model', 'type': 'STRING'},
            {'name': 'type', 'type': 'STRING'},
            {'name': 'suburb', 'type': 'STRING'},
            {'name': 'state', 'type': 'STRING'}
        ],
        gcp_conn_id = 'google_cloud'
    )
    create_australian_suburbs_table = BigQueryCreateEmptyTableOperator(
        task_id = 'create_australian_suburbs_table',
        dataset_id = 'autotrader_raw',
        table_id = 'australian_suburbs',
        project_id = 'autotrader-toyota-dashboard',
        schema_fields = [
            {'name': 'id', 'type': 'INT64'},
            {'name': 'suburb', 'type': 'STRING'},
            {'name': 'state', 'type': 'STRING'},
            {'name': 'latitude', 'type': 'FLOAT64'},
            {'name': 'longitude', 'type': 'FLOAT64'}
        ],
        gcp_conn_id = 'google_cloud'
    )
    empty1 = EmptyOperator(task_id = 'empty1')
    insert_raw_data_into_listings_raw = GCSToBigQueryOperator(
        task_id = 'insert_raw_data_into_listings_raw',
        bucket = 'autotrader-raw',
        source_objects = f'autotrader-raw-{current_date}.json',
        destination_project_dataset_table = 'autotrader-toyota-dashboard.autotrader_raw.listings_raw',
        schema_fields = [
            {'name': 'date', 'type': 'STRING'},
            {'name': 'price', 'type': 'STRING'},
            {'name': 'odometer', 'type': 'STRING'},
            {'name': 'year', 'type': 'STRING'},
            {'name': 'car_model', 'type': 'STRING'},
            {'name': 'type', 'type': 'STRING'},
            {'name': 'suburb', 'type': 'STRING'},
            {'name': 'state', 'type': 'STRING'}
        ],
        source_format = 'NEWLINE_DELIMITED_JSON',
        write_disposition = 'WRITE_TRUNCATE',
        gcp_conn_id = 'google_cloud'
    )
    insert_csv_data_into_australian_suburbs_table = GCSToBigQueryOperator(
        task_id = 'insert_csv_data_into_australian_suburbs_table',
        bucket = 'autotrader-raw',
        source_objects = 'australian_suburbs.csv',
        destination_project_dataset_table = 'autotrader-toyota-dashboard.autotrader_raw.australian_suburbs',
        schema_fields = [
            {'name': 'id', 'type': 'INT64'},
            {'name': 'suburb', 'type': 'STRING'},
            {'name': 'state', 'type': 'STRING'},
            {'name': 'latitude', 'type': 'FLOAT64'},
            {'name': 'longitude', 'type': 'FLOAT64'}
        ],
        source_format = 'CSV',
        write_disposition = 'WRITE_TRUNCATE',
        gcp_conn_id = 'google_cloud'
    )
    empty2 = EmptyOperator(task_id = 'empty2')
    staging = DbtTaskGroup(
        group_id='staging',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/staging']
        )
    )
    warehouse1 = DbtTaskGroup(
        group_id='warehouse',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/warehouse']
        )
    )
    analytics = DbtTaskGroup(
        group_id='analytics',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/analytics']
        )
    )
    extract >> [create_listings_raw_table, create_australian_suburbs_table] >> empty1 >> [insert_raw_data_into_listings_raw, insert_csv_data_into_australian_suburbs_table] \
    >> empty2 >> staging >> warehouse1 >> analytics
    


