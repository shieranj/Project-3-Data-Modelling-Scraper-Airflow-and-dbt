from airflow import DAG
from airflow.decorators import task, task_group
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import pendulum

import sys
sys.path.insert(0, '/opt/airflow/scripts/')

from capstone3.project2_helpers.extraction import Extraction
from capstone3.project2_helpers.transformation import Transformation
from capstone3.project1_helpers.bq_connector_schema import BQConnector
from capstone3.project2_helpers.discord_webhook import discord_webhook

import os

def notify_discord_fail(context):
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    dag_id = context["dag"].dag_id
    execution_date = context["logical_date"]
    data_period = execution_date - relativedelta(months=1)
    data_period = data_period.strftime("%Y-%m")
    log_url = context["task_instance"].log_url


    content = (
        f"⚠️ **Airflow DAG Failed**\n"
        f"**DAG:** {dag_id}\n"
        f"**Data Period:** {data_period}\n"
        f"Logs: {log_url}"
    )

    discord_webhook(webhook_url, content)

default_args={
    "owner":"Shieran",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    }

with DAG(
    default_args=default_args,
    dag_id='Capstone3_NYGreenTaxiData_to_BigQuery_and_Discord',
    description='Monthly ingestion of NYC Green Taxi data, starting from 2023',
    start_date = pendulum.datetime(2023, 2, 1, 00, 00, 00),
    schedule_interval='@monthly',
    tags = ["NY Taxi Data", "capstone3", "project2", "ingestion", "BigQuery", "web scraping", "notification"],
    catchup=True,
    max_active_runs=1,
    concurrency=1,
    on_failure_callback=notify_discord_fail
) as dag:

    @task
    def get_year_month(**context):
        logical_date = context["logical_date"]
        run_date = logical_date - relativedelta(months=1)
        return run_date.strftime("%Y-%m")

    @task_group(group_id="Extraction")
    def green_taxi_data_extraction(url, year_month):
        
        e = Extraction(url)

        @task
        def extract_parquet_url(year_month):
            parquet_link = e.get_links(year_month)
            return parquet_link
        
        @task
        def save_raw_parquet_in_gcs(raw_parquet_link:str, year_month:str):

            raw_gcs_path = e.store_parquet_in_gcs(year_month,raw_parquet_link)
            return raw_gcs_path
        
        parquet_link=extract_parquet_url(year_month)
        raw_gcs_path = save_raw_parquet_in_gcs(raw_parquet_link=parquet_link, year_month=year_month)

        return raw_gcs_path
    
    @task_group(group_id = "Transformation")
    def taxi_data_transformation(raw_gcs_path:str, year_month):

        t= Transformation()
        @task
        def normalize_columns(raw_gcs_path, year_month):
            cleaned_columns_path = t.clean_columns(raw_gcs_path, year_month)
            return cleaned_columns_path
        
        @task
        def standardize_values(cleaned_columns_path, year_month):
            final_transformation_path = t.clean_values_types(cleaned_columns_path, year_month)
            return final_transformation_path
        
        normalized_columns_path = normalize_columns(raw_gcs_path, year_month)
        final_transformation_path = standardize_values(normalized_columns_path, year_month)
        
        return final_transformation_path
        

    @task_group(group_id = "Load_to_BQ")
    def loadjob_to_BQ(final_transformation_path:str, year_month:str):
        
        @task
        def load_from_gcs_path(final_transformation_path:str, year_month:str, **context):
            bq_config = {
                "credential_path": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), 
                "project_id": os.getenv("PROJECT_ID"), 
                "dataset_id": "jcdeah007_capstone3_shieranjuvi_NYGreenTaxi",
                "location":"asia-southeast2"
                }

            bq = BQConnector(**bq_config)

            logical_date = context["logical_date"]
            try:
                bq.load_from_gcs(gcs_path=final_transformation_path,
                                table_name = "NY_green_taxi",
                                schema_file = "schema/bq_green_taxi.yaml",
                                run_date=logical_date)
                print(f"Loading NY Green Taxi to BQ successful!, period: {year_month}")

            except Exception as e:
                print(f"Error loading from GCS to BigQuery for period: {year_month}! {e}")
                raise
        
        load_from_gcs_path(final_transformation_path, year_month)
    

    year_month = get_year_month()
    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    raw_gcs_path = green_taxi_data_extraction(
        url = url,
        year_month=year_month)
    
    transformed_gcs_path = taxi_data_transformation(
        raw_gcs_path=raw_gcs_path, 
        year_month=year_month)
    
    loadjob_to_BQ(
        final_transformation_path=transformed_gcs_path, 
        year_month=year_month)

            



