from airflow import DAG
from airflow.decorators import task, task_group
import pandas as pd
from datetime import timedelta
import pendulum

import sys
sys.path.insert(0, '/opt/airflow/scripts/')
from capstone3.project1_helpers.dummy_generation import generate_new_customers, generate_new_transactions, generate_new_payments
from capstone3.project1_helpers.postgres_connector import PostgresConnector

import os

pg_config = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": 5432,
    "database":os.getenv("POSTGRES_PROJECT1_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

default_args={
    "owner":"Shieran",
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id='FinPro_Dummy_Postgres',
    description='Data dummy creation and ingestion to postgres',
    start_date = pendulum.datetime(2026, 2, 19, 00, 00, 00),
    schedule_interval='@hourly',
    tags = ["postgres", "dummy", "taskflow", "capstone3","taskgroup", "project1"],
    catchup=True,
    max_active_runs=1, #prevent concurrent runs
    concurrency=1 #1 task at a time
 
) as dag:

##TaskGroup Based, customers (customer generation, load to postgres), transactions(trx generation, load to postgres), payments(payment generation, load to postgres)
    @task_group(group_id='customers')
    def process_customers():

        @task
        def generate_customers(**context):
            execution_date = context['logical_date']
            gcs_path = generate_new_customers(num_of_cust = 15, execution_date=execution_date)
            return gcs_path
        
        @task
        def load_customers_to_postgres(gcs_path:str):
            pg = PostgresConnector(**pg_config)
            try:
                pg.get_psycopg2_conn()
                customers_df = pd.read_csv(gcs_path)
                pg.load_dataframe(
                    table_name = 'customers',
                    df = customers_df,
                    primary_key='customer_id',
                    if_exists='append'
                )
            finally: 
                pg.close()
            return gcs_path #for transaction
        
        customer_path = generate_customers()
        return load_customers_to_postgres(customer_path)
        
    @task_group(group_id = 'transactions')
    def process_transactions(customers_gcs_path:str):

        @task
        def generate_transactions(customers_gcs_path:str, **context):
            execution_date = context['logical_date']
            gcs_path = generate_new_transactions(customers_gcs_path, execution_date=execution_date)
            return gcs_path
        
        @task
        def load_transactions_to_postgres(gcs_path:str):
            pg = PostgresConnector(**pg_config)
            try:
                pg.get_psycopg2_conn()
                transactions_df = pd.read_csv(gcs_path)
                pg.load_dataframe(
                    table_name='transactions',
                    df = transactions_df, 
                    primary_key= 'trx_id',
                    if_exists='append',
                    foreign_key='customer_id',
                    fk_source_key='customer_id',
                    fk_source_table='customers'
                )
            finally:
                pg.close()

            return gcs_path
        
        transactions_path = generate_transactions(customers_gcs_path)
        return load_transactions_to_postgres(transactions_path)
    
    @task_group(group_id = 'payments')
    def process_payments(transactions_gcs_path: str):

        @task
        def generate_payments(transactions_gcs_path:str,**context):
            execution_date = context['logical_date']
            gcs_path = generate_new_payments(transactions_gcs_path, execution_date=execution_date)

            return gcs_path
        
        @task
        def load_payments_to_postgres(gcs_path:str):
            pg = PostgresConnector(**pg_config)
            try:
                pg.get_psycopg2_conn()
                payments_df = pd.read_csv(gcs_path)
                pg.load_dataframe(
                    table_name='payments',
                    df = payments_df, 
                    primary_key='payment_id', 
                    if_exists='append', 
                    foreign_key='trx_id',
                    fk_source_key='trx_id',
                    fk_source_table='transactions'
                )
            finally:
                pg.close()

            return gcs_path
        
        payment_path = generate_payments(transactions_gcs_path)
        return load_payments_to_postgres(payment_path)
    
    customers_path = process_customers()
    transactions_path = process_transactions(customers_path)
    payment_path = process_payments(transactions_path)





