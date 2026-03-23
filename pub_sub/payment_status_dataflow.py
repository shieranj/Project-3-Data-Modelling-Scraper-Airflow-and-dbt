#apache beam library
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery

import os
import json
import yaml
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
#GCP Environment Set Up
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PUBSUB")

#PubSub config
INPUT_SUBSCRIPTION = "projects/jcdeah-007/subscriptions/capstone3_shieran_project3-sub"

#BQ config
schema_file = "schema/payment_status.yaml"
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = "jcdeah007_capstone3_shieranjuvi_retails"
TABLE_ID = "payment_status"
OUTPUT_TABLE = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}"

beam_options_dict={
    "project": PROJECT_ID, 
    "runner": "DataflowRunner",
    "region": "asia-southeast2",
    "temp_location": "gs://shieran-gcs-bucket/capstone3/project3/beam_temp",
    "job_name": "capstone3-project3-shieranjuvi",
    "streaming": True
}
beam_options = PipelineOptions.from_dictionary(beam_options_dict)

# BQ schema collection
with open(schema_file, "r") as f:
    table_config = yaml.safe_load(f)

bq_schema = []
for field in table_config["schema"]:
    bq_schema.append(f"{field['name']}:{field['type']}")

bq_schema = ", ".join(bq_schema)

def main():
    with beam.Pipeline(options=beam_options) as p:
        (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription = INPUT_SUBSCRIPTION)
            | "Decode" >> beam.Map(lambda x: x.decode("utf-8"))
            | "Parse JSON" >> beam.Map(json.loads)
            | "Add ingestion_time" >> beam.Map(lambda x: {**x, "ingestion_time": datetime.now()})
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE, 
                schema=bq_schema, 
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters = {
                    "timePartitioning":{
                        "type" : "DAY",
                        "field": "ingestion_time"
                    },
                    "clustering":{
                        "fields": ["status","payment_id"]
                    }
                }
            )
        )

if __name__ == "__main__":
    main()