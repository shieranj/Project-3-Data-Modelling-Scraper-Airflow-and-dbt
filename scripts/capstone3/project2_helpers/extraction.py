import requests
from bs4 import BeautifulSoup
import os
import pandas as pd

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

class Extraction:
    """
    class to extract & preprocess NYC taxi

    1. Scrapes the NYC TLC site to find .parquet files
    2. Read parquet files into pandas dataframe & save them in GCS

    """

    def __init__(self, url: str):
        self.url = url

    def get_links(self, year_month:str) -> str:
        """
         Scrape the TLC webpage for links to relevant Parquet files
        """
        try:
            resp = requests.get(self.url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            print(f"Successfully parse html from {self.url} -  code {resp.status_code}")

            for a_tag in soup.find_all("a", href = True):
                href = a_tag["href"].strip()
                if "green" in href and year_month in href and href.endswith(".parquet"):
                    link = href
                    print(f"Link match: {link} ")
        except Exception as e:
            print(f"Get links error: {e}")
            raise
        
        return link
    
    def store_parquet_in_gcs(self, year_month: str, link:str)-> str:
        """
        read parquet files as dataframes and save in GCS
        """
        if link is None:
            print(f"Error: No links found! Exiting function...")
            raise
        
        try:

            df = pd.read_parquet(link)
            file_name = f"green_{year_month}.parquet"
            gcs_path = f"gs://jcdeah007-bucket/capstone3_shieranjuvi/project2/raw/green_taxi/{file_name}"

            print(f"saving raw parquet in GCS...")
            df.to_parquet(gcs_path, index=False)
            print(f"Successfully saved in {gcs_path}")

            return gcs_path

        except Exception as e:
            print(f"Error saving raw data in GCS! {e}")
            raise

