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
    'retries': 0,
    'retry_delay': timedelta(seconds = 30)
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
    """remove_nulls = BigQueryExecuteQueryOperator(
        task_id = 'remove_nulls',
        sql = '''DELETE autotrader-toyota-dashboard.autotrader_staging.listings_raw
        WHERE
        price IS NULL
        OR odometer IS NULL
        OR year IS NULL
        OR car_model IS NULL
        OR type IS NULL
        OR suburb IS NULL
        OR state IS NULL 
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
        car_model = CASE 
        WHEN car_model LIKE '%LAND CRUISER%' THEN REGEXP_REPLACE(REPLACE(car_model, type, ''), r'(\S+)\s(.+)', r'\\1\\2')
        ELSE REPLACE(car_model, type, '')
        END,
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
        geolocation GEOGRAPHY,
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
        sql = '''INSERT INTO autotrader-toyota-dashboard.autotrader_transformed.location_dim (location_key, suburb, state, geolocation)
        SELECT
        ROW_NUMBER() OVER () as location_key,
        suburb,
        state,
        ST_GEOGPOINT(longitude, latitude) as geolocation
        FROM (
        SELECT 
        DISTINCT
        listings_raw.suburb,
        listings_raw.state,
        latitude,
        longitude
        FROM
        autotrader-toyota-dashboard.autotrader_staging.listings_raw AS listings_raw
        JOIN autotrader-toyota-dashboard.autotrader_staging.australian_suburbs AS australian_suburbs
        ON listings_raw.suburb = australian_suburbs.suburb
        AND listings_raw.state = australian_suburbs.state
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
    create_looker_table = BigQueryExecuteQueryOperator(
        task_id = 'create_looker_table',
        sql = '''CREATE OR REPLACE TABLE autotrader-toyota-dashboard.autotrader_looker.listings_all AS
        SELECT 
        listing_fact.location_key,
        suburb,
        state,
        geolocation,
        listing_fact.car_key,
        year,
        car_model,
        type,
        odometer,
        price
        FROM autotrader-toyota-dashboard.autotrader_transformed.listing_fact AS listing_fact
        JOIN autotrader-toyota-dashboard.autotrader_transformed.location_dim AS location_dim
        ON listing_fact.location_key = location_dim.location_key
        JOIN autotrader-toyota-dashboard.autotrader_transformed.car_dim AS car_dim
        ON listing_fact.car_key = car_dim.car_key;''',
        write_disposition = 'WRITE_TRUNCATE',
        gcp_conn_id = 'google_cloud',
        use_legacy_sql = False
    )
    calculate_price_averages = BigQueryExecuteQueryOperator(
        task_id = 'calculate_price_averages',
        sql = '''CREATE OR REPLACE TABLE autotrader-toyota-dashboard.autotrader_looker.listings_all AS
        WITH calc_avg_price_state AS (
        SELECT
        state,
        year,
        car_model,
        AVG(price) AS avg_price_state
        FROM autotrader-toyota-dashboard.autotrader_looker.listings_all
        GROUP BY state, year, car_model),

        calc_avg_price_national AS (
        SELECT
        year,
        car_model,
        AVG(price) AS avg_price_national
        FROM autotrader-toyota-dashboard.autotrader_looker.listings_all
        GROUP BY year, car_model
        )

        SELECT 
        location_key,
        suburb,
        listings_all.state,
        geolocation,
        car_key,
        listings_all.year,
        listings_all.car_model,
        type,
        odometer,
        price,
        avg_price_state,
        avg_price_national
        FROM autotrader-toyota-dashboard.autotrader_looker.listings_all AS listings_all
        JOIN calc_avg_price_state
        ON listings_all.state = calc_avg_price_state.state
        AND listings_all.year = calc_avg_price_state.year
        AND listings_all.car_model = calc_avg_price_state.car_model
        JOIN calc_avg_price_national
        ON listings_all.year = calc_avg_price_national.year
        AND listings_all.car_model = calc_avg_price_national.car_model;''',
        write_disposition = 'WRITE_TRUNCATE',
        gcp_conn_id = 'google_cloud',
        use_legacy_sql = False
    )"""
    
    extract >> [create_listings_raw_table, create_australian_suburbs_table] >> empty1 >> [insert_raw_data_into_listings_raw, insert_csv_data_into_australian_suburbs_table]
    """>> remove_nulls >> uppercase_columns >> format_columns >> string_to_int >> [create_car_dim, create_location_dim] \
    >> empty2 >> [insert_into_car_dim, insert_into_location_dim] >> create_listing_fact >> insert_into_listing_fact >> create_looker_table >> calculate_price_averages"""
    


