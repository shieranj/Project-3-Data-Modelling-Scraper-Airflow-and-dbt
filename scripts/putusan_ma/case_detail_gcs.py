import os
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
import random
import time
from datetime import datetime
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PUBSUB")

month_map = {
    "januari": "01",
    "februari": "02",
    "maret": "03",
    "april": "04",
    "mei":"05",
    "juni":"06",
    "juli":"07",
    "agustus":"08",
    "september":"09",
    "oktober":"10",
    "nopember":"11",
    "desember":"12"
}

class GroupCase:
    def __init__(self, source_path:str, destination_bucket:str, month_map:dict):
        self.source_path = source_path
        self.destination_bucket = destination_bucket
        self.month_map = month_map
        self.driver = self._init_driver()

    def _init_driver(self):
        """
        Selenium Initialization
        """
        self.download_dir = Path("temp_pdf")
        self.download_dir.mkdir(exist_ok=True)
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.page_load_strategy = "none" #don't wait for page load to complete
        options.add_experimental_option("prefs", {
        "download.default_directory": str(self.download_dir.resolve()),
        "download.prompt_for_download": False, #no popup
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True #don't open in browser, just download
    })
        driver =  webdriver.Chrome(options=options)
        driver.set_page_load_timeout(300)

        return driver

    def convert_date(self, date_str):
        """Map through months and convert dates to yyyy-mm-dd format

        Args:
            date_str (_type_): date string, taken from dataframe

        Returns:
            _type_: string
        """
        if pd.isna(date_str) or date_str == "—":
            return None
        
        parts = date_str.split()
        if len(parts) != 3:
            return None
        
        day, month, year = parts
        month_int = self.month_map.get(month.lower())

        return f"{year}-{month_int}-{day}"

    def transform_case_metadata(self):
        try:
            df = pd.read_parquet(self.source_path)
            date_col = [col for col in df.columns if "tanggal" in col]
            for col in date_col:
                df[col] = df[col].apply(self.convert_date)
                df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")
            
            df["tahun"] = df["tahun"].astype("Int64")
            df["putusan_ym"] = df["tanggal_dibacakan"].dt.to_period("M").astype(str)
            print(f"Metadata transformation successful!")

            return df
        
        except Exception as e:
            print(f"Error transforming case metadata, {e}")
            raise

    def save_to_gcs(self, df):
        grouped_df = df.groupby("putusan_ym")
        try:
            for ym, group in grouped_df:
                gcs_path = f"gs://{self.destination_bucket}/final_project/putusan_ma_2025_20pages/grouped_cases/{ym}/putusan_details.parquet"
                group.to_parquet(gcs_path, index=False)
                print(f"successfully saved parquet file to GCS for period {ym}")
        
        except Exception as e:
            print(f"Error saving parquet file to GCS! {e}")
            raise

    def save_pdf_gcs(self,df):
        client = storage.Client()
        bucket = client.bucket(self.destination_bucket)
        skipped_log_path = Path("html_2025_scraped26Feb_20/skipped_pdf.txt")
        skipped_list = []
        grouped_df = df[["nomor_putusan", "putusan_ym", "pdf_link"]].groupby("putusan_ym")
        for ym, group in grouped_df:
            print(f"Downloading PDFs for period {ym} - {len(group)} files")

            for _, row in group.iterrows():
                pdf_url = row["pdf_link"]
                case_number = row["nomor_putusan"].replace("/","_")

                if pdf_url == "Unavailable":
                    print(f"SKIPPING, no pdf link for {case_number}")
                    skipped_list.append(case_number)
                    continue
                
                try:
                    for f in self.download_dir.glob("*.pdf"):
                        f.unlink()

                    self.driver.get(pdf_url)

                    timeout = 120
                    start = time.time()
                    download_success = False
                    while True:
                        files = list(self.download_dir.glob("*.pdf"))
                        temp_files = list(self.download_dir.glob("*.crdownload"))

                        if files and not temp_files:
                            download_success = True
                            break

                        if time.time() - start > timeout:
                            print("download timeout, skipping...")
                            skipped_list.append(case_number)
                            break
                        time.sleep(random.uniform(3,7))
                    
                    if download_success:
                        downloaded_file = files[0]
                        blob = bucket.blob(f"final_project/putusan_ma_2025_20pages/downloaded_pdf/{ym}/{case_number}.pdf")
                        blob.upload_from_filename(str(downloaded_file))
                        print(f"Successfully downloaded {case_number} pdf to GCS!")
                except WebDriverException as e:
                    print(f"Failed to navigate to {pdf_url}: {e}")
                    skipped_list.append(case_number)
                    continue
                
                except Exception as e:
                    print(f"Error uploading to GCS! {e}")
                    raise

        with open(skipped_log_path,"w") as f:
            f.write("\n".join(skipped_list))
        print(f"Skipped list successfully created, contains {len(skipped_list)}")

    def run(self):
        df = self.transform_case_metadata()
        self.save_to_gcs(df)
        self.save_pdf_gcs(df)
        self.driver.quit()
        print(f"Scraping completed!")

group_case = GroupCase(
    source_path = "gs://shieran-gcs-bucket/final_project/putusan_ma/raw_case_scrape/raw_scrape_2025.parquet",
    destination_bucket= "shieran-gcs-bucket",
    month_map = month_map
)
group_case.run()





