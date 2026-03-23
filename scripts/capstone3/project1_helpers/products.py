
from datetime import datetime
import pandas as pd
import os
from google.cloud import bigquery
from scripts.capstone3.project1_helpers.bq_connector_schema import BQConnector

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"keys/new_jcdeah-007.json"
# static products data
products = {
    "PROD_001": {
        "product_id": "PROD_001",
        "product_name": "Wireless Mouse",
        "category_id": "CAT_01",
        "category_name": "Electronics",
        "price": 125000
    },
    "PROD_002": {
        "product_id": "PROD_002",
        "product_name": "Mechanical Keyboard",
        "category_id": "CAT_01",
        "category_name": "Electronics",
        "price": 450000
    },
    "PROD_003": {
        "product_id": "PROD_003",
        "product_name": "Bluetooth Speaker",
        "category_id": "CAT_01",
        "category_name": "Electronics",
        "price": 300000
    },
    "PROD_004": {
        "product_id": "PROD_004",
        "product_name": "Basic T-Shirt",
        "category_id": "CAT_02",
        "category_name": "Fashion",
        "price": 90000
    },
    "PROD_005": {
        "product_id": "PROD_005",
        "product_name": "Denim Jeans",
        "category_id": "CAT_02",
        "category_name": "Fashion",
        "price": 250000
    },
    "PROD_006": {
        "product_id": "PROD_006",
        "product_name": "Running Shoes",
        "category_id": "CAT_02",
        "category_name": "Fashion",
        "price": 550000
    },
    "PROD_007": {
        "product_id": "PROD_007",
        "product_name": "Rice Cooker",
        "category_id": "CAT_03",
        "category_name": "Home",
        "price": 400000
    },
    "PROD_008": {
        "product_id": "PROD_008",
        "product_name": "Blender",
        "category_id": "CAT_03",
        "category_name": "Home",
        "price": 350000
    },
    "PROD_009": {
        "product_id": "PROD_009",
        "product_name": "Desk Lamp",
        "category_id": "CAT_03",
        "category_name": "Home",
        "price": 150000
    },
    "PROD_010": {
        "product_id": "PROD_010",
        "product_name": "Face Wash",
        "category_id": "CAT_04",
        "category_name": "Beauty",
        "price": 75000
    },
    "PROD_011": {
        "product_id": "PROD_011",
        "product_name": "Body Lotion",
        "category_id": "CAT_04",
        "category_name": "Beauty",
        "price": 120000
    },
    "PROD_012": {
        "product_id": "PROD_012",
        "product_name": "Perfume",
        "category_id": "CAT_04",
        "category_name": "Beauty",
        "price": 600000
    },
    "PROD_013": {
        "product_id": "PROD_013",
        "product_name": "Football",
        "category_id": "CAT_05",
        "category_name": "Sports",
        "price": 180000
    },
    "PROD_014": {
        "product_id": "PROD_014",
        "product_name": "Yoga Mat",
        "category_id": "CAT_05",
        "category_name": "Sports",
        "price": 220000
    },
    "PROD_015": {
        "product_id": "PROD_015",
        "product_name": "Dumbbell Set",
        "category_id": "CAT_05",
        "category_name": "Sports",
        "price": 480000
    }
}
# try:
#     df = pd.DataFrame(products.values())
#     file_name = "retail_products.csv"
#     gcs_path = f"gs://shieran-gcs-bucket/capstone3/project1/postgres_temp/products/{file_name}"
#     df.to_csv(gcs_path, index = False)
#     print(f"Successfully saved in GCS as CSV. location: {gcs_path}")
# except Exception as e:
#     print(f"Error saving to GCS {e}")

bq_config = {
    "credential_path":  r"keys/new_jcdeah-007.json", 
    "project_id": "jcdeah-007", 
    "dataset_id": "jcdeah007_capstone3_shieranjuvi_retails",
    "location":"asia-southeast2"
    }
try:
    df = pd.DataFrame(products.values())
    timestamp = datetime.now()
    df["created_at"] = timestamp
    df["updated_at"] = timestamp

    bq = BQConnector(**bq_config)
    bq.load_df_to_bigquery(
        df = df,
        table_name="products",
        schema_file="schema/products.yaml"
    )
    print(f"Successfully load products to BQ")
except Exception as e:
            print(f"Error loading df to Bigquery, {e} ")
            raise
    
                
