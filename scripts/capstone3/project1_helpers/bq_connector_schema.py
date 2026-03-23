from google.cloud import bigquery
import os
import pandas as pd
import yaml
from datetime import datetime
from dateutil.relativedelta import relativedelta
class BQConnector:
    """
    BigQuery connector class for managing dataset, table creation, and data loading using load job.
    """
    def __init__(self, credential_path, project_id, dataset_id, location="asia-southeast2"):
        self.credential_path = credential_path
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.location = location

        # Set environment variable for Google Cloud authentication
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path

        self.client = bigquery.Client()
        dataset_ref = self.client.dataset(self.dataset_id)

        try:
            self.client.get_dataset(dataset_ref)

        except Exception:
            try:
                #create dataset if not exist
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = self.location
                self.client.create_dataset(dataset, exists_ok=True)
                print(f"Created new dataset: {dataset_id}")

            except Exception as e:
                print(f"Failed to create dataset {dataset_id}, {e}")
                raise

    def table_exists(self, table_name):
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        try:
            self.client.get_table(table_id)
            return True
        except:
            return False

    def create_table_schema_config(self, table_name, schema_file):
        """
        Docstring for create_table_schema_config - create Table with schema.yaml
        
        :param table_name: table_name in BQ
        :param schema_file: schema.yml file path
        """
        schema = []
        try:
            with open(schema_file, 'r') as f:
                table_config = yaml.safe_load(f)

            for field in table_config['schema']:
                schema.append(bigquery.SchemaField(
                    field['name'],
                    field['type'],
                    mode=field.get('mode', 'NULLABLE'),
                    description=field.get('description', '')
                ))
            
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            #if table exist, skip this function
            if self.table_exists(table_name):
                print(f"Table `{table_name}` already exists, skipping table creation...")
                return
            #create table if table doesnt exist
            print(f"Creating table {table_name}...")
            table = bigquery.Table(table_id, schema=schema)

            #Table Description
            description = table_config.get('description', '')
            if description:
                table.description = description

            #Table Partition
            partitioning = table_config.get('partitioning', {})
            if partitioning.get('enabled', False):

                partition_type_map = {
                'DAY': bigquery.TimePartitioningType.DAY,
                'HOUR': bigquery.TimePartitioningType.HOUR,
                'MONTH': bigquery.TimePartitioningType.MONTH,
                'YEAR': bigquery.TimePartitioningType.YEAR,
            }
                
                partition_type = partition_type_map.get(
                partitioning.get('type', 'DAY'),
                bigquery.TimePartitioningType.DAY
            )
            
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=partition_type,
                    field=partitioning.get('field', 'run_date')
                )
                print(f"Table partitioned by {partitioning.get('field')} ({partitioning.get('type')})")
            else:
                print(f"No partitioning configured for table {table_id}")
            
            #Table Clustering
            clustering = table_config.get('clustering', {})
            if clustering.get('enabled', False):
                fields = clustering.get('fields', [])
                if fields:
                    table.clustering_fields = fields
                    print(f"Table clustered by {', '.join(fields)}")
            else:
                print(f"No clustering fields for table {table_id}")

            created_table = self.client.create_table(table)
            print(f"table {table_id} successfully created!")
            return created_table
        
        except Exception as e:
            print(f"Error creating table with schema {schema_file}, {e}")
            raise
        
    def load_df_to_bigquery(self, df, table_name, schema_file):
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        try:
            with open(schema_file, 'r') as f:
                table_config = yaml.safe_load(f)

            self.create_table_schema_config(table_name = table_name, schema_file = schema_file)

            bq_schema = []
            for field in table_config['schema']:
                bq_schema.append(bigquery.SchemaField(
                    field["name"],
                    field["type"],
                    mode = field.get("mode", "NULLABLE")
                ))
            print(f"Appending {len(df)} row(s) to BQ {table_name}...")
            job_config = bigquery.LoadJobConfig(write_disposition = bigquery.WriteDisposition.WRITE_APPEND, schema = bq_schema)
            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            
            print(f"Successfully load to {table_id}, {job.output_rows} rows")
             
        except Exception as e:
            print(f"Error loading data from dataframe to {table_name}! {e}")
            raise

    def load_from_gcs(self, gcs_path, table_name, schema_file, run_date):
        """
        Docstring for load_from_gcs [for NY TAXI]

        load from gcs file path
        :param gcs_path: gcs path that contains transformed data(parquet)      
        :param table_name: bigquery table name
        :param schema_file: schema file path
        :param run_date: ingestion date, taken from airflow dag context
        """
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        try:
            with open(schema_file, 'r') as f:
                table_config = yaml.safe_load(f)

            df = pd.read_parquet(gcs_path)
            print(f"✓ Extracted {len(df)} rows")

            df_copy = df.copy()
            df_copy["data_period"] = run_date.date() - relativedelta(months=1)
            df_copy["run_date"] = run_date.date()
            print("Added BQ metadata run_date!")

            self.create_table_schema_config(table_name=table_name, schema_file=schema_file)

            bq_schema = []
            for field in table_config['schema']:
                bq_schema.append(bigquery.SchemaField(
                    field["name"],
                    field["type"],
                    mode = field.get("mode", "NULLABLE")
                ))
            print(f"Appending {len(df)} rows to BQ {table_name}...")
            job_config = bigquery.LoadJobConfig(write_disposition = bigquery.WriteDisposition.WRITE_APPEND, schema = bq_schema)
            job = self.client.load_table_from_dataframe(df_copy, table_id, job_config=job_config)
            job.result()
            
            print(f"Successfully load to {table_id}, {job.output_rows} rows")
             
        except Exception as e:
            print(f"Error loading data from dataframe {gcs_path} to {table_name}! {e}")
            raise


    def load_df_to_stg(self, pg_conn, source_table, stg_table_name, schema_file, start_date, end_date):
        """
        Docstring for load_df_to_stg

        Load postgres table to bigquery staging stable
        
        :param pg_conn: postgres connector
        :param source_table: postgres_table
        :param stg_table_name: staging table name in bigquery
        :param schema_file: schema file path
        :param start_date: data interval start date from airflow dag properties, access though context (pendulum time)
        :param end_date: data interval end date from airflow dag properties, access though context (pendulum time)
        """
        table_id = f"{self.project_id}.{self.dataset_id}.{stg_table_name}"
        try:
            with open(schema_file, 'r') as f:
                table_config = yaml.safe_load(f)

            source_columns_config = table_config.get('source_columns', {})

            if 'include' in source_columns_config and source_columns_config['include']:
                ingested_cols = source_columns_config['include']
                joined_cols = ", ".join(ingested_cols)
            
            query = f"""SELECT {joined_cols} FROM {source_table}
                    WHERE updated_at >= '{start_date}'
                    AND updated_at < '{end_date}'"""
            df = pd.read_sql(query, pg_conn)
            print(f"✓ Extracted {len(df)} rows")

            #add BQ Metadata
            df_copy = df.copy()
            df_copy["run_date"] = end_date.date()

            self.create_table_schema_config(table_name = stg_table_name, schema_file=schema_file)
            
            bq_schema = []
            for field in table_config["schema"]:
                bq_schema.append(bigquery.SchemaField(
                    field["name"],
                    field["type"],
                    mode = field.get("mode", "NULLABLE")
                ))

            print(f"Loading {len(df_copy)} rows to staging table {stg_table_name}...")
            job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, schema=bq_schema)
            job = self.client.load_table_from_dataframe(df_copy, table_id, job_config=job_config)
            job.result()
            
            print(f"Successfully load to STAGING: {table_id}, {job.output_rows} rows")

        except Exception as e:
            print(f"Error loading to {table_id}, {e}")
            raise

    def merge_stg_to_main(self, stg_table_name, main_table_name, schema_file):
        try:
            stg_table_id = f"{self.project_id}.{self.dataset_id}.{stg_table_name}"
            main_table_id = f"{self.project_id}.{self.dataset_id}.{main_table_name}"

            self.create_table_schema_config(main_table_name, schema_file=schema_file)
            
            with open(schema_file, 'r') as f:
                table_config = yaml.safe_load(f)

            dedup_config = table_config.get('deduplication')
            if not dedup_config.get('enabled', False):
                raise #if false raise error
            
            key_columns = dedup_config.get('key_columns', [])
            order_by = dedup_config.get('order_by', ['updated_at DESC'])

            on_clause = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])

            all_columns = [field['name'] for field in table_config['schema']]
            update_columns = [col for col in all_columns if col not in key_columns]

            update_set = ", ".join([f"{col} = source.{col}" for col in update_columns])

            insert_columns = ", ".join(all_columns)
            insert_values = ", ".join([f"source.{col}" for col in all_columns])

            partition_clause = ", ".join(key_columns)
            order_clause = ", ".join(order_by)

            merge_query = f"""
                MERGE `{main_table_id}` AS target
                USING (
                    SELECT * EXCEPT(row_num)
                    FROM(
                        SELECT *, 
                            ROW_NUMBER() OVER (
                                PARTITION BY {partition_clause}
                                ORDER BY {order_clause}
                            ) as row_num
                        FROM `{stg_table_id}`
                        )
                    WHERE row_num = 1
                ) AS source
                ON {on_clause}
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
            """

            job = self.client.query(merge_query)
            job.result()
            print(f"Succesfully merge from staging table {stg_table_name} to main table {main_table_name}! Affected rows: {job.num_dml_affected_rows}")

        except Exception as e:
            print(f"Error merging from {stg_table_name} to {main_table_name}, {e}")
            raise

    def delete_stg_table(self, stg_table_name:str):
        """
        Docstring for delete_stg_table
        
        :param self: BQ Connector
        :param stg_table_name: staging table name
        :type stg_table_name: str
        """
        stg_table_id = f"{self.project_id}.{self.dataset_id}.{stg_table_name}"
        try:
            self.client.delete_table(stg_table_id)
            print(f"Successfully delete stg table {stg_table_name}")

        except Exception as e:
            print(f"Error deleting staging table {stg_table_name}! {e}")




            


                