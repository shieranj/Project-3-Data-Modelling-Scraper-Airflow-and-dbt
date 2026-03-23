import os
from pathlib import Path
from bs4 import BeautifulSoup
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
import time
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PUBSUB")

class Scraper:
    def __init__(self):
        """
        Selenium initialization 
        """
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")

        self.driver = webdriver.Chrome(options=options)

    def scrape_case_links(self,file_path)->list[str]:
        """Accepts a local html file path
        aims to scrape case links
        returns a list of strings(links)

        Args:
            file_path (_type_): html file path

        Returns:
            list[str]: list of links
        """
        case_links = []
        with open(file_path, "r", encoding="utf-8") as f:
            html = f.read()

        soup = BeautifulSoup(html, "html.parser")
        print(f"html from {file_path.name} parsed!")

        try:
            entries = soup.select("#tabs-1 div.entry-c strong a")
            for a in entries:
                link = a.get("href")
                case_links.append(link)
            
            print(f"{len(case_links)} collected!")
            return case_links
        
        except Exception as e:
            print(f"Error scraping case links from {file_path.name} due to {e}!")
            return []

    def scrape_case_details(self, url, max_attempt=3)->dict:
        """scrape case details for metadata

        Args:
            url (str): case detail link 
            max_attempt (int, optional): attempt to handle 500 Error. Defaults to 3.

        Returns:
            dict: returns a dictionary of metadata
        """
        metadata = {}
        for attempt in range(1, max_attempt+1):
            try:
                self.driver.set_page_load_timeout(120)
                self.driver.get(url)
                sleep_time = random.uniform(5,8)
                time.sleep(sleep_time)
                break
            
            except Exception as e:
                wait = attempt * random.uniform(5,8)
                print(f"[RETRY {attempt}/{max_attempt}] {url} failed: {e}. Waiting {wait:.1f}s")
                time.sleep(wait)

                if attempt == max_attempt:
                    print(f"[SKIPPED] {url} after {max_attempt} attempts")
                    return

        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        field_mapping = {
            "Nomor": "nomor_putusan",
            "Tingkat Proses": "tingkat_proses",
            "Klasifikasi": "klasifikasi",
            "Kata Kunci": "kata_kunci",
            "Tahun": "tahun",
            "Tanggal Register": "tanggal_register",
            "Lembaga Peradilan": "lembaga_peradilan",
            "Jenis Lembaga Peradilan": "jenis_lembaga_peradilan",
            "Hakim Ketua": "hakim_ketua",
            "Hakim Anggota": "hakim_anggota",
            "Panitera": "panitera",
            "Amar": "amar",
            "Catatan Amar": "catatan_amar",
            "Tanggal Musyawarah": "tanggal_musyawarah",
            "Tanggal Dibacakan": "tanggal_dibacakan"
        }

        title = soup.find("span", id = "title_pihak")
        if not title: #if web has no title, it is unpublished/not ready and will provide 0 data. skip
            print(f"Skipping {url} due to unavailable content")
            return

        #table_data
        rows = soup.select("table tr")
        for row in rows:
            label_td = row.select_one("td.text-right")
            if not label_td:
                continue
            label = label_td.get_text(strip=True)

            value_td = label_td.find_next_sibling("td")
            value = value_td.get_text(strip=True) if value_td else None

            col_name = field_mapping.get(label)
            if col_name:
                metadata[col_name] = value

        pdf_link = "Unavailable"
        all_links = soup.find_all("a", href=True)
        for a in all_links:
            href = a["href"]
            if "pdf" in href.lower() and "download_file" in href.lower():
                pdf_link = href
                break
        
        metadata["pdf_link"] = pdf_link
                
                    
        metadata["scrape_timestamp"] = datetime.now()
        return metadata

    def close(self):
        self.driver.quit()
        print("Driver closed")

results = [] #extend with case_links
scraper = Scraper()
BASE_FOLDER = Path("html_2025_scraped26Feb_20")

for item in BASE_FOLDER.glob("*.html"):
    path = item.resolve()
    print(f"Processing file: {path.name}....")
    try:
        case_links = scraper.scrape_case_links(path)
        if case_links:
            results.extend(case_links)
    
    except Exception as e:
        print(f"Error collecting case links for {path.name}! {e}")

print(f"Collected total {len(results)} links")
all_metadata = [] #list of dictionary
skipped_links = []
count = 1
for link in results:
    try:
        data = scraper.scrape_case_details(link)
        if data:
            all_metadata.append(data)
        else:
            skipped_links.append(link)

        print(f"Scraped {count} links!")
        if count%20 == 0:
            rest = random.uniform(10,20)
            print(f"[RESTING] sleeping {rest:.1f}s after {count} links")
            time.sleep(rest)
        count+=1
    except Exception as e:
        print(f"Error scraping case detail {link}! {e}")

scraper.close()
print("Scraping completed!")
with open(BASE_FOLDER/"skipped.txt", "w") as f:
    f.write("\n".join(skipped_links))

gcs_path = "gs://shieran-gcs-bucket/final_project/putusan_ma/raw_case_scrape/raw_scrape_2025.parquet"
df = pd.DataFrame(all_metadata)

try:
    df.to_parquet(gcs_path, index=False)
    print(f"Successfully save to GCS")
except Exception as e:
    print(f"Error saving to GCS {e}")



