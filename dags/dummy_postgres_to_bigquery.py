from airflow import DAG
from airflow.decorators import task, task_group
from datetime import timedelta
import pendulum

import sys
sys.path.insert(0, '/opt/airflow/scripts/')
from capstone3.project1_helpers.postgres_connector import PostgresConnector
from capstone3.project1_helpers.bq_connector_schema import BQConnector

import os

pg_config = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": 5432,
    "database":os.getenv("POSTGRES_PROJECT1_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

bq_config = {
    "credential_path": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), 
    "project_id": os.getenv("PROJECT_ID"), 
    "dataset_id": "jcdeah007_capstone3_shieranjuvi_retails",
    "location":"asia-southeast2"
}

default_args={
    "owner":"Shieran",
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id='FinPro_Postgres_to_BigQuery',
    description='Daily ingestion from Postgres to BigQuery',
    start_date = pendulum.datetime(2026, 2, 19, 00, 00, 00),
    schedule_interval='@daily',
    tags = ["postgres", "dummy", "capstone3", "project1", "ingestion", "BigQuery", "Final"],
    catchup=True,
    max_active_runs=1, #prevent concurrent runs
    concurrency=1 #1 task at a time
 
) as dag:
    # taskgroup based, customers (ingestion to stg table, merging to main table, deleting stg table)
    
    @task_group(group_id = 'customers')
    def daily_customer_ingestion():

        @task
        def customer_ingestion_to_stg(**context):
            logical_date = context["logical_date"]
            start_date = context["data_interval_start"]
            end_date = context["data_interval_end"]

            print(f"Ingesting customers from {start_date} to {end_date}...")
            print("Credential path from bq_config:", bq_config["credential_path"])

            pg = PostgresConnector(**pg_config)
            bq = BQConnector(**bq_config)

            pg.get_psycopg2_conn()

            staging_table_name = f"stg_customers_{logical_date.strftime('%Y%m%d')}"

            try:

                bq.load_df_to_stg(
                    pg_conn = pg.conn,
                    source_table='customers',
                    stg_table_name=staging_table_name,
                    schema_file='schema/bq_customers.yaml',
                    start_date=start_date,
                    end_date=end_date
                )

                return staging_table_name
            except Exception as e:
                print(f"Error ingesting customer table from Postgres! {e}")
                raise 
        
        @task
        def merge_customers_to_main_table(staging_table_name:str):
            
            bq = BQConnector(**bq_config)

            try:
                bq.merge_stg_to_main(
                    stg_table_name=staging_table_name,
                    main_table_name='customers',
                    schema_file='schema/bq_customers.yaml'
                    )
                return staging_table_name
            
            except Exception as e:
                print(f"Error merging from {staging_table_name} to main table! {e}")
                raise

        @task 
        def delete_stg_table(staging_table_name:str):
            bq = BQConnector(**bq_config)

            try:
                bq.delete_stg_table(
                    stg_table_name=staging_table_name
                )

            except Exception as e:
                print(f"Error deleting staging table {staging_table_name}, {e}")

        stg_name = customer_ingestion_to_stg()
        merged_stg_name = merge_customers_to_main_table(stg_name)
        delete_stg_table(merged_stg_name)

    @task_group(group_id = 'transactions')
    def daily_transactions_ingestion():

        @task
        def transactions_ingestion_to_stg(**context):
            logical_date = context["logical_date"]
            start_date = context["data_interval_start"]
            end_date = context["data_interval_end"]

            print(f"Ingesting transactions from {start_date} to {end_date}...")
            pg = PostgresConnector(**pg_config)
            bq = BQConnector(**bq_config)

            pg.get_psycopg2_conn()

            staging_table_name = f"stg_transactions_{logical_date.strftime('%Y%m%d')}"

            try:

                bq.load_df_to_stg(
                    pg_conn = pg.conn,
                    source_table='transactions',
                    stg_table_name=staging_table_name,
                    schema_file='schema/bq_transactions.yaml',
                    start_date=start_date,
                    end_date=end_date
                )

                return staging_table_name
            except Exception as e:
                print(f"Error ingesting transactions table from Postgres! {e}")
                raise 
        
        @task
        def merge_transactions_to_main_table(staging_table_name:str):
            
            bq = BQConnector(**bq_config)

            try:
                bq.merge_stg_to_main(
                    stg_table_name=staging_table_name,
                    main_table_name='transactions',
                    schema_file='schema/bq_transactions.yaml'
                    )
                return staging_table_name
            
            except Exception as e:
                print(f"Error merging from {staging_table_name} to main table! {e}")
                raise

        @task 
        def delete_stg_table(staging_table_name:str):
            bq = BQConnector(**bq_config)

            try:
                bq.delete_stg_table(
                    stg_table_name=staging_table_name
                )

            except Exception as e:
                print(f"Error deleting staging table {staging_table_name}, {e}")

        stg_name = transactions_ingestion_to_stg()
        merged_stg_name = merge_transactions_to_main_table(stg_name)
        delete_stg_table(merged_stg_name)

    @task_group(group_id = 'payments')
    def daily_payments_ingestion():

        @task
        def payments_ingestion_to_stg(**context):
            logical_date = context["logical_date"]
            start_date = context["data_interval_start"]
            end_date = context["data_interval_end"]

            print(f"Ingesting payments from {start_date} to {end_date}...")
            pg = PostgresConnector(**pg_config)
            bq = BQConnector(**bq_config)

            pg.get_psycopg2_conn()

            staging_table_name = f"stg_payments_{logical_date.strftime('%Y%m%d')}"

            try:

                bq.load_df_to_stg(
                    pg_conn = pg.conn,
                    source_table='payments',
                    stg_table_name=staging_table_name,
                    schema_file='schema/bq_payments.yaml',
                    start_date=start_date,
                    end_date=end_date
                )

                return staging_table_name
            except Exception as e:
                print(f"Error ingesting payments table from Postgres! {e}")
                raise 
        
        @task
        def merge_payments_to_main_table(staging_table_name:str):
            
            bq = BQConnector(**bq_config)

            try:
                bq.merge_stg_to_main(
                    stg_table_name=staging_table_name,
                    main_table_name='payments',
                    schema_file='schema/bq_payments.yaml'
                    )
                return staging_table_name
            
            except Exception as e:
                print(f"Error merging from {staging_table_name} to main table! {e}")
                raise

        @task 
        def delete_stg_table(staging_table_name:str):
            bq = BQConnector(**bq_config)

            try:
                bq.delete_stg_table(
                    stg_table_name=staging_table_name
                )

            except Exception as e:
                print(f"Error deleting staging table {staging_table_name}, {e}")

        stg_name = payments_ingestion_to_stg()
        merged_stg_name = merge_payments_to_main_table(stg_name)
        delete_stg_table(merged_stg_name)

    daily_customer_ingestion()
    daily_transactions_ingestion()
    daily_payments_ingestion()


            




