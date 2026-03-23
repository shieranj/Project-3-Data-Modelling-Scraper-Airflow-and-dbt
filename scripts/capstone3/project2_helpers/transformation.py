import pandas as pd
import os 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

class Transformation:

    def clean_columns(self, raw_gcs_path:str, year_month:str) -> str:
        try:
            df = pd.read_parquet(raw_gcs_path)
            df = df.copy()

            df.columns = df.columns.str.lower().str.strip()

            name_mapping = {
                "vendorid": "vendor_id",
                "ratecodeid": "ratecode_id",
                "pulocationid": "pu_location_id",
                "dolocationid": "do_location_id"
            }

            df = df.rename(columns=name_mapping)
            print(f"Successfully renamed columns to lower case & snakecase")

            dropped_columns = ["ehail_fee", "store_and_fwd_flag"]
            df = df.drop(columns=dropped_columns)
            print(f"Dropping empty columns and columns with Y/N, columns dropped: {', '.join(dropped_columns)}")
            
            # start of 2025, cbd_congestion_fee part of data
            if "cbd_congestion_fee" not in df.columns:
                df["cbd_congestion_fee"] = pd.NA
                print(f"Added column cbd_congestion_fee, filled with NULLs (pre-Jan 2025 data)")

            file_name = f"green_{year_month}.parquet"
            gcs_path = f"gs://jcdeah007-bucket/capstone3_shieranjuvi/project2/transformation_stage/clean_columns/green_taxi/{file_name}"
            
            print(f"Saving to GCS....")
            df.to_parquet(gcs_path, index=False)
            print(f"Successfully saved in GCS, path {gcs_path}")
            return gcs_path

        except Exception as e:
            print(f"Error cleaning columns! {e}")
            raise
    
    def clean_values_types(self, cleaned_gcs_path:str, year_month:str):
        """
        Docstring for clean_columns - standardize datatype, fill nulls, keep valid records
        
        :param gcs_path: cleaned columns parquet GCS path
        :type gcs_path: str
        """
        try:
            df = pd.read_parquet(cleaned_gcs_path)

            # ratecode_id null fill with 99
            if "ratecode_id" in df.columns:
                df["ratecode_id"] = df["ratecode_id"].fillna(99)

            # payment type null fill with 5(unknown)
            if "payment_type" in df.columns:
                df["payment_type"] = df["payment_type"].fillna(5)

            # payment type null fill with 1(streethail)
            if "trip_type" in df.columns:
                df["trip_type"] = df["trip_type"].fillna(1)

            print("Successfully filled null values!")

            #change columns type to use Int instead of int, Int accepts null values
            for col in df.columns:
                if col.endswith("_count") or col.endswith("_type"):
                    df[col] = pd.to_numeric(df[col], errors = "coerce").astype("Int32")
                else:
                    pass
            print(f"Deleting invalid fares data wuth negative values...")
            df = df[(df["fare_amount"] > 0) & (df["total_amount"] > 0) & (df["trip_distance"] > 0)]
            
            file_name = f"green_{year_month}.parquet"
            gcs_path = f"gs://jcdeah007-bucket/capstone3_shieranjuvi/project2/transformation_stage/final_transformed/green_taxi/{file_name}"
            
            print(f"Saving to GCS....")
            df.to_parquet(gcs_path, index=False)
            print(f"Successfully saved in GCS, path {gcs_path}")
            return gcs_path
        
        except Exception as e:
            print(f"Error cleaning values & columns! {e}")
            raise

    

    
    
            

