from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
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
            {'name': 'car_model', 'type': 'STRING'},
            {'name': 'type', 'type': 'STRING'},
            {'name': 'suburb', 'type': 'STRING'},
            {'name': 'state', 'type': 'STRING'}
        ],
        gcp_conn_id = 'google_cloud'
    )
    create_australian_suburbs_table = BigQueryCreateEmptyTableOperator(
        task_id = 'create_australian_suburbs_table',
        dataset_id = 'autotrader_staging',
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
        destination_project_dataset_table = 'autotrader-toyota-dashboard.autotrader_staging.listings_raw',
        schema_fields = [
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
        destination_project_dataset_table = 'autotrader-toyota-dashboard.autotrader_staging.australian_suburbs',
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
    remove_nulls = BigQueryExecuteQueryOperator(
        task_id = 'remove_nulls',
        sql = '''DELETE autotrader-toyota-dashboard.autotrader_staging.listings_raw
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
        OR car_model IS NULL
        OR type IS NULL
        OR suburb IS NULL
        OR state IS NULL 
        ))
        OR odometer = '';''',
        gcp_conn_id = 'google_cloud',
        use_legacy_sql = False
        )
    uppercase_columns = BigQueryExecuteQueryOperator(
        task_id = 'uppercase_columns',
        sql = '''UPDATE autotrader-toyota-dashboard.autotrader_staging.listings_raw
        SET car_model = UPPER(car_model),
        type = UPPER(type),
        suburb = UPPER(suburb)
        WHERE car_model IN (
        SELECT
        car_model
        FROM autotrader-toyota-dashboard.autotrader_staging.listings_raw
        )
        AND type IN (
        SELECT
        type
        FROM autotrader-toyota-dashboard.autotrader_staging.listings_raw
        )
        AND suburb IN (
        SELECT
        suburb
        FROM autotrader-toyota-dashboard.autotrader_staging.listings_raw
        );''',
        gcp_conn_id = 'google_cloud',
        use_legacy_sql = False)
    format_columns = BigQueryExecuteQueryOperator(
        task_id = 'format_columns',
        sql = '''UPDATE autotrader-toyota-dashboard.autotrader_staging.listings_raw
        SET price = REPLACE(REPLACE(price, '$', ''), ',', ''),
        odometer = REPLACE(REPLACE(odometer, 'km', ''), ',', ''),
        car_model = REPLACE(car_model, type, ''),
        type = CASE WHEN type = '' THEN 'NOT SPECIFIED'
        ELSE REPLACE(REPLACE(type, '(', ''), ')', '')
        END,
        suburb = LTRIM(REPLACE(REPLACE(suburb, '(', ''), ')', ''))
        WHERE price in (
        SELECT
        price
        FROM
        autotrader-toyota-dashboard.autotrader_staging.listings_raw
        )
        AND odometer in (
        SELECT
        odometer
        FROM
        autotrader-toyota-dashboard.autotrader_staging.listings_raw
        )
        AND car_model in (
        SELECT
        car_model
        FROM
        autotrader-toyota-dashboard.autotrader_staging.listings_raw
        )
        AND type in (
        SELECT
        type
        FROM
        autotrader-toyota-dashboard.autotrader_staging.listings_raw
        )
        AND suburb in (
        SELECT
        suburb
        FROM
        autotrader-toyota-dashboard.autotrader_staging.listings_raw
        );''',
        gcp_conn_id = 'google_cloud',
        use_legacy_sql = False)
    string_to_int = BigQueryExecuteQueryOperator(
        task_id = 'string_to_int',
        sql = '''CREATE OR REPLACE TABLE autotrader-toyota-dashboard.autotrader_staging.listings_raw AS
        SELECT
        CAST(price as INT64) as price,
        CAST(odometer as INT64) as odometer,
        CAST(year as INT64) as year,
        car_model,
        type,
        suburb,
        state
        FROM autotrader-toyota-dashboard.autotrader_staging.listings_raw;''',
        gcp_conn_id = 'google_cloud',
        use_legacy_sql = False
    )
    create_car_dim = BigQueryExecuteQueryOperator(
        task_id = 'create_car_dim',
        sql = '''CREATE OR REPLACE TABLE
        autotrader-toyota-dashboard.autotrader_transformed.car_dim 
        (
        car_key INT64 NOT NULL,
        year INT64,
        car_model STRING,
        type STRING,
        PRIMARY KEY (car_key) NOT ENFORCED
        );''',
        gcp_conn_id = 'google_cloud',
        use_legacy_sql = False
    )
    create_location_dim = BigQueryExecuteQueryOperator(
        task_id = 'create_location_dim',
        sql = '''CREATE OR REPLACE TABLE
        autotrader-toyota-dashboard.autotrader_transformed.location_dim 
        (
        location_key INT64 NOT NULL,
        suburb STRING,
        state STRING,
        PRIMARY KEY (location_key) NOT ENFORCED
        );''',
        gcp_conn_id = 'google_cloud',
        use_legacy_sql = False
    )
    empty2 = EmptyOperator(task_id = 'empty2')
    insert_into_car_dim = BigQueryExecuteQueryOperator(
        task_id = 'insert_into_car_dim',
        sql = '''INSERT INTO autotrader-toyota-dashboard.autotrader_transformed.car_dim (car_key, year, car_model, type)
        SELECT
        ROW_NUMBER() OVER () as car_key,
        year,
        car_model,
        type
        FROM (
        SELECT 
        DISTINCT
        year,
        car_model,
        type
        FROM
        autotrader-toyota-dashboard.autotrader_staging.listings_raw
        );''',
        write_disposition = 'WRITE_TRUNCATE',
        gcp_conn_id = 'google_cloud',
        use_legacy_sql = False,
    )
    insert_into_location_dim = BigQueryExecuteQueryOperator(
        task_id = 'insert_into_location_dim',
        sql = '''INSERT INTO autotrader-toyota-dashboard.autotrader_transformed.location_dim (location_key, suburb, state)
        SELECT
        ROW_NUMBER() OVER () as location_key,
        suburb,
        state
        FROM (
        SELECT 
        DISTINCT
        suburb,
        state
        FROM
        autotrader-toyota-dashboard.autotrader_staging.listings_raw
        );''',
        write_disposition = 'WRITE_TRUNCATE',
        gcp_conn_id = 'google_cloud',
        use_legacy_sql = False
    )
    create_listing_fact = BigQueryExecuteQueryOperator(
        task_id = 'create_listing_fact',
        sql = '''CREATE OR REPLACE TABLE
        autotrader-toyota-dashboard.autotrader_transformed.listing_fact 
        (
        car_key INT64 NOT NULL,
        location_key INT64 NOT NULL,
        odometer INT64 NOT NULL,
        price INT64 NOT NULL,
        PRIMARY KEY (car_key,location_key) NOT ENFORCED,
        FOREIGN KEY (car_key) REFERENCES autotrader_transformed.car_dim(car_key) NOT ENFORCED,
        FOREIGN KEY (location_key) REFERENCES autotrader_transformed.location_dim(location_key) NOT ENFORCED
        );''',
        gcp_conn_id = 'google_cloud',
        use_legacy_sql = False
    )
    insert_into_listing_fact = BigQueryExecuteQueryOperator(
        task_id = 'insert_into_listing_fact',
        sql = '''INSERT INTO autotrader-toyota-dashboard.autotrader_transformed.listing_fact (car_key, location_key, odometer, price)
        SELECT
        car_dim.car_key,
        location_dim.location_key,
        odometer,
        price
        FROM autotrader-toyota-dashboard.autotrader_staging.listings_raw listings_raw
        JOIN autotrader-toyota-dashboard.autotrader_transformed.car_dim car_dim
        ON listings_raw.year = car_dim.year
        AND listings_raw.car_model = car_dim.car_model
        AND listings_raw.type = car_dim.type
        JOIN autotrader-toyota-dashboard.autotrader_transformed.location_dim location_dim
        ON listings_raw.suburb = location_dim.suburb
        AND listings_raw.state = location_dim.state''',
        write_disposition = 'WRITE_TRUNCATE',
        gcp_conn_id = 'google_cloud',
        use_legacy_sql = False
    )
    
    extract >> [create_listings_raw_table, create_australian_suburbs_table] >> empty1 >> [insert_raw_data_into_listings_raw, insert_csv_data_into_australian_suburbs_table] \
    >> remove_nulls >> uppercase_columns >> format_columns >> string_to_int >> [create_car_dim, create_location_dim] \
    >> empty2 >> [insert_into_car_dim, insert_into_location_dim] >> create_listing_fact >> insert_into_listing_fact
    


