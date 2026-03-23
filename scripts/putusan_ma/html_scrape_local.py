import os
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

START_PAGE = 1
END_PAGE = 20
MAX_RETRIES = 3
BASE_FOLDER = "html_2025_scraped26Feb_20"

BASE_URL = "https://putusan3.mahkamahagung.go.id/direktori/index/tahunjenis/putus/tahun/2025"
os.makedirs(BASE_FOLDER, exist_ok=True)


options = Options()
options.add_argument("--incognito")
options.add_argument("--start-maximized")
options.add_argument("--disable-blink-features=AutomationControlled")

driver = webdriver.Chrome(options=options)


def build_url(page):
    if page == 1:
        return f"{BASE_URL}.html"
    else:
        return f"{BASE_URL}/page/{page}.html"


def is_valid_page(html):
    html_lower = html.lower()

    if "error-page-wrapper" in html_lower:
        return False

    if "maaf, terjadi kesalahan pada aplikasi" in html_lower:
        return False

    if "captcha" in html_lower:
        return False

    return True

for page in range(START_PAGE, END_PAGE + 1):

    file_path = os.path.join(BASE_FOLDER, f"page_{page}.html")

    if os.path.exists(file_path) and os.path.getsize(file_path) > 200_000:
        print(f"[SKIP] Page {page} already saved.")
        continue

    url = build_url(page)
    retries = 0

    while retries < MAX_RETRIES:
        try:
            print(f"[SCRAPING] Page {page} -> {url}")

            driver.get(url)

            #human delay
            time.sleep(random.uniform(5, 8))

            #fake scroll
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(1, 3))
            driver.execute_script("window.scrollTo(0, 0);")

            html = driver.page_source

            if not is_valid_page(html):
                raise Exception("Detected invalid / blocked page")

            #save HTML
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(html)

            print(f"[SAVED] Page {page}")
            break

        except Exception as e:
            retries += 1
            wait_time = random.uniform(15, 30)
            print(f"[RETRY {retries}] Page {page} failed. Waiting {wait_time:.2f}s")
            time.sleep(wait_time)

    if retries == MAX_RETRIES:
        print(f"[SKIPPED] Page {page} after 3 attempts.")
        long_wait = random.uniform(60, 120)
        print(f"[COOLDOWN] Sleeping {long_wait:.2f}s")
        time.sleep(long_wait)

    if page % 10 == 0:
        rest_time = random.uniform(30, 60)
        print(f"[BATCH REST] After 10 pages. Sleeping {rest_time:.2f}s")
        time.sleep(rest_time)

driver.quit()
print("Done scraping.")