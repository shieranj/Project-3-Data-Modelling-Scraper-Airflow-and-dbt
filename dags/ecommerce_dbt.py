from airflow import DAG
from airflow.decorators import task, task_group
import pandas as pd
from datetime import timedelta
import pendulum
from airflow.operators.bash import BashOperator
import os
import sys
sys.path.insert(0, '/opt/airflow/scripts/')

from capstone3.project2_helpers.discord_webhook import discord_webhook

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

default_args={
    "owner":"Shieran",
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    default_args=default_args,
    dag_id='FinPro_ecommerce_pipeline',
    description='ecommerce dbt pipeline',
    start_date = pendulum.datetime(2026, 2, 21, 00, 00, 00),
    schedule_interval='@daily',
    tags = ["dummy", "final","dbt","ecommerce"],
    catchup=False,
    max_active_runs=1, #prevent concurrent runs
    concurrency=1, #1 task at a time
    on_failure_callback = notify_discord_fail
 
) as dag:
    
    @task_group(group_id="dbt_ecommerce")
    def dbt_ecommerce_group():

        dbt_prep = BashOperator(
            task_id = "dbt_preparation",
            bash_command = "dbt build --select ecommerce.preparation",
            cwd = "/opt/airflow/dbt/dbt_projects"
        )

        dbt_core = BashOperator(
            task_id = "dbt_core",
            bash_command = "dbt build --select ecommerce.core",
            cwd = "/opt/airflow/dbt/dbt_projects"
        )

        dbt_mart = BashOperator(
            task_id = "dbt_mart",
            bash_command = "dbt build --select ecommerce.mart",
            cwd = "/opt/airflow/dbt/dbt_projects"
        )
        
        dbt_prep >> dbt_core >> dbt_mart
    
    dbt_ecommerce_group()
