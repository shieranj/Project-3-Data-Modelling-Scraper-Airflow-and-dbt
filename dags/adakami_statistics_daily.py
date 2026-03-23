from airflow import DAG
from airflow.decorators import task, task_group
import pendulum
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
import os
import sys
import pandas as pd
import pytz
sys.path.insert(0, '/opt/airflow/scripts/')

from adakami_webscrape.statistics_scrape import get_statistics_to_gcs
from capstone3.project2_helpers.discord_webhook import discord_webhook
from capstone3.project1_helpers.bq_connector_schema import BQConnector

def notify_discord_fail(context):
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    dag_id = context["dag"].dag_id
    execution_date = context["logical_date"].strftime("%Y-%m-%d")
    log_url = context["task_instance"].log_url

    content = (
        f"⚠️ **Airflow DAG Failed**\n"
        f"**DAG:** {dag_id}\n"
        f"**Data Period:** {execution_date}\n"
        f"**Logs**: {log_url}"
    )

    discord_webhook(webhook_url, content)

default_args = {
    "owner": "Shieran",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}
with DAG(
    default_args = default_args, 
    dag_id = "FinPro_Adakami_Daily_Statistics",
    description = "Daily ingestion of Adakami's daily statistics",
    start_date = pendulum.datetime(2026, 3, 4, 00, 00, 00, tz="Asia/Jakarta"),
    schedule_interval = "0 0 * * *",
    tags = ["final", "adakami", "statistics"],
    catchup = False,
    max_active_runs = 1,
    concurrency = 1,
    on_failure_callback = notify_discord_fail
) as dag:

    @task_group(group_id="Extract_Load")
    def extract_load():
        @task
        def statistic_extraction(api_url):
            try:
                gcs_path = get_statistics_to_gcs(api_url)
                return gcs_path
            except Exception as e:
                print(f"Error extracting & saving to GCS, {e}")
                raise
        
        @task
        def load_to_bigquery(gcs_path, **context):

            tz_jakarta = pytz.timezone("Asia/Jakarta")
            data_period = context["data_interval_end"]

            data_period_jakarta = data_period.astimezone(tz_jakarta)

            df = pd.read_parquet(gcs_path)
            df["data_period"] = data_period_jakarta.date()

            bq_config = {
                "credential_path": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), 
                "project_id": os.getenv("PROJECT_ID"), 
                "dataset_id": "finalproject_adakami_statistics_shieranjuvi",
                "location":"asia-southeast2"
                }
            bq = BQConnector(**bq_config)
            try:
                bq.load_df_to_bigquery(
                    df = df,
                    table_name = "adakami_daily_statistics",
                    schema_file = "schema/bq_adakami_statistics.yaml"
                )
                print(f"Succesfully load from GCS, data period: {data_period_jakarta.date()}")
            except Exception as e:
                print(f"Error loading from GCS to Bigquery, {e} ")
                raise

        api_url = "https://www.adakami.id/api/configuration/statistics"
        gcs_path = statistic_extraction(api_url = api_url)
        load_to_bigquery(gcs_path)
    
    @task_group(group_id="dbt_Transformation")
    def dbt_layers():
        dbt_prep = BashOperator(
            task_id = "dbt_preparation",
            bash_command = (
                "dbt build "
                "--select adakami_statistics.preparation "
            ),
            cwd = "/opt/airflow/dbt/dbt_projects"
        )

        dbt_prep
    
    extract_load() >> dbt_layers()
