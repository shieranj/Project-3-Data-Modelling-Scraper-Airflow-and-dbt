from airflow import DAG
from airflow.decorators import task, task_group
from datetime import timedelta, datetime
import pendulum
import pandas as pd
import sys
import subprocess

sys.path.insert(0, '/opt/airflow/scripts/')

from putusan_ma.metadata_ingestion_helper import MetadataIngestion
from capstone3.project1_helpers.bq_connector_schema import BQConnector
from capstone3.project2_helpers.discord_webhook import discord_webhook

import os

def notify_discord_fail(context):
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    dag_id = context["dag"].dag_id
    execution_date = context["logical_date"]
    data_period = execution_date.strftime("%Y-%m")
    log_url = context["task_instance"].log_url

    content = (
        f"⚠️ **Airflow DAG Failed**\n"
        f"**DAG:** {dag_id}\n"
        f"**Data Period:** {data_period}\n"
        f"Logs: {log_url}"
    )

    discord_webhook(webhook_url, content)

default_args = {
    "owner": "Shieran",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    default_args = default_args,
    dag_id = "FinPro_putusan_ma_metadata_ingestion",
    description = "ingestion from GCS directory of putusan MA data to bigquery",
    start_date = pendulum.datetime(2025,1,1,00,00,00),
    schedule_interval = "@monthly",
    tags = ["putusan_ma", "pdf_extract", "case", "metadata"],
    catchup = True,
    max_active_runs = 1,
    concurrency = 1,
    on_failure_callback = notify_discord_fail
) as dag:
    
    @task
    def get_year_month(**context):
        ym = context["logical_date"]
        return ym.strftime("%Y-%m")

    @task_group(group_id="Case_Details_Ingestion")
    def case_detail_ingestion(year_month:str):

        @task
        def extract_case_details_from_GCS(year_month:str):
            credential_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            mi = MetadataIngestion(
                credential_path = credential_path
            )
            
            gcs_path = mi.case_details(
                year_month = year_month
            )
            print(f"GCS path used: {gcs_path}")
            return gcs_path
        
        @task
        def ingest_case_detail_to_BQ(gcs_path:str):

            if not gcs_path:
                print(f"No GCS Path found, SKIPPING ingestion step....")
                return False
            
            df = pd.read_parquet(gcs_path)
            df["ingestion_timestamp"] = datetime.now()

            bq_config = {
                "credential_path": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), 
                "project_id": os.getenv("PROJECT_ID"), 
                "dataset_id": "finalproject_shieranjuvi_putusanma_raw",
                "location":"asia-southeast2"
                }
            
            bq = BQConnector(**bq_config)
            try:
                bq.load_df_to_bigquery(
                    df = df,
                    table_name = "case_details",
                    schema_file = "schema/putusan_ma_case_details.yaml"
                )
                print(f"Successfully load case details to BQ! referencing {gcs_path}")
                return True
            
            except Exception as e:
                print(f"Error loading case detail data to BigQuery, {e}")
                raise

        gcs_path = extract_case_details_from_GCS(year_month)
        return ingest_case_detail_to_BQ(gcs_path)

    @task_group(group_id = "PDF_Metadata_Ingestion")
    def pdf_metadata_ingestion(year_month:str):

        @task
        def extract_pdf_metadata(year_month:str):
            credential_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            mi = MetadataIngestion(
                credential_path = credential_path
            )

            gcs_path = mi.pdf_details(year_month)
            print(f"GCS path used : {gcs_path}")

            return gcs_path
        
        @task
        def ingest_pdf_metadata_to_BQ(gcs_path, **context):
            if not gcs_path:
                print(f"GCS Path not found! SKIPPING ingestion step....")
                return False
            
            df = pd.read_parquet(gcs_path)
            df["putusan_ym"] = context["logical_date"]
            df["ingestion_timestamp"] = datetime.now()

            bq_config = {
                "credential_path": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), 
                "project_id": os.getenv("PROJECT_ID"), 
                "dataset_id": "finalproject_shieranjuvi_putusanma_raw",
                "location":"asia-southeast2"
                }
            
            bq = BQConnector(**bq_config)
            try:
                bq.load_df_to_bigquery(
                    df = df,
                    table_name = "pdf_details",
                    schema_file = "schema/putusan_ma_pdf_details.yaml"
                )
                print(f"Successfully load case details to BQ! referencing {gcs_path}")
                return True
            
            except Exception as e:
                print(f"Error loading pdf metadata to BigQuery, {e}")
                raise
        
        gcs_path = extract_pdf_metadata(year_month)
        return ingest_pdf_metadata_to_BQ(gcs_path)
    
    
    @task_group(group_id="dbt_Transformation")
    def dbt_layers(case_result, pdf_result):
        @task
        def build_dbt_command(case_result, pdf_result):
            if not case_result:
                print("Case ingestion returned nothing, skipping dbt process....")
                return None
            if case_result and not pdf_result:
                return "dbt build --select putusan_ma.preparation.prep_case_details "
            return "dbt build --select putusan_ma.preparation "
        
        @task
        def dbt_preparation(dbt_command: str):
            if not dbt_command:
                print(f"No dbt command received, skipping...")
                return
            
            result = subprocess.run(
                dbt_command,
                shell = True, 
                cwd = "/opt/airflow/dbt/dbt_projects",
                capture_output = True,
                text= True
            )
            print(result.stdout)
            if result.returncode != 0:
                raise Exception(f"dbt failed: {result.stderr}")

        
        dbt_command = build_dbt_command(case_result, pdf_result)
        dbt_preparation(dbt_command)
    
    year_month = get_year_month()
    case_ingest = case_detail_ingestion(year_month) 
    pdf_ingest = pdf_metadata_ingestion(year_month)
    dbt_layers(case_ingest, pdf_ingest)
    









            

