import requests
import pandas as pd
import re
from datetime import datetime
import os
import pytz

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

def camel_to_snake(s):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()

def get_statistics_to_gcs(url:str)->pd.DataFrame:
    """
    Docstring for get_statistics
    
    :param url: adakami statistics api url
    :type url: str
    :return: return pandas dataframe of statistics
    :rtype: DataFrame
    """

    try:
        resp = requests.get(url)
        resp.raise_for_status()
        print(f"fetching from api url: {url} with status code {resp.status_code}")

        stats = resp.json()

        renamed_stats = {
            camel_to_snake(key):value for key,value in stats["content"].items()
        }

        tz_jakarta = pytz.timezone("Asia/Jakarta")
        renamed_stats["fetch_time"] = datetime.now(tz_jakarta)

        df = pd.DataFrame([renamed_stats])

        cols = df.columns.tolist()
        columns = [col for col in cols if col != "fetch_time"]
        for col in columns:
            df[col] = df[col].astype(int)

        date_str = datetime.now(tz_jakarta).strftime("%Y%m%d")
        file_name = f"adakami_statistics_{date_str}.parquet"
        gcs_path = f"gs://shieran-gcs-bucket/final_project/adakami_statistics/{file_name}"

        df.to_parquet(gcs_path,index=False)
        print(f"Successfully saved data to {gcs_path}")

        return gcs_path

    except Exception as e:
        print(f"Error scraping from {url}, Error: {e}")

