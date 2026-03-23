import os
from pathlib import Path
from datetime import datetime
import pandas as pd
from google.cloud import storage
import pdfplumber
import io
import re


def clean_text(text):
    text = re.sub(
    r"M\s*a\s*h\s*k\s*a\s*m\s*a\s*h\s*A\s*g\s*u\s*n\s*g\s*R\s*e\s*p\s*u\s*b\s*l\s*i\s*k\s*I\s*n\s*d\s*o\s*n\s*e\s*s\s*i\s*a",
    "",text,flags=re.IGNORECASE)
    text = re.sub(
      r"putusan\.mahkamahagung\.go\.id",
      "",
      text,
      flags=re.IGNORECASE
    )
    text = re.sub(
      r"Disclaimer\s*.*kepaniteraan@mahkamahagung\.go\.id.*\n",
      "",
      text,
      flags=re.IGNORECASE | re.DOTALL
    )
    text = re.sub(r"\n\s*[a-zA-Z]\s*\n", "\n", text)

    return text

def extract_field(pattern, text, multiline=False):
    flags = re.IGNORECASE | (re.DOTALL if multiline else 0)
    match = re.search(pattern, flags=flags, string=text)
    if match:
        result = match.group(1).strip()
        result = re.sub(r"\s+[a-zA-Z]$","", result)
        return result
    return None

def extract_keputusan(text):
    mengadili_pattern = r"M\s*E\s*N\s*G\s*A\s*D\s*I\s*L\s*I\s*:\s*(.*?;.*?;)"
    menetapkan_pattern = r"M\s*E\s*N\s*E\s*T\s*A\s*P\s*K\s*A\s*N\s*(.*?;.*?;)"

    mengadili_match = re.search(mengadili_pattern, text, flags=re.IGNORECASE | re.DOTALL)
    menetapkan_match = re.search(menetapkan_pattern, text, flags=re.IGNORECASE | re.DOTALL)

    if mengadili_match:
        result = re.sub(r"\s+", " ", mengadili_match.group(1)).strip()
        return result
    
    if menetapkan_match:
        result = re.sub(r"\s+", " ", menetapkan_match.group(1)).strip()
        return result
    
    return None
        
def collect_field(text):
    data = {
        "nama_lengkap":       extract_field(r"(?:Nama|Nama lengkap)\s*\w*:\s*(.*)", text),
        "tempat_lahir":       extract_field(r"Tempat lahir\s*\w*:\s*(.*)", text),
        "umur_tanggal_lahir": extract_field(r"Umur/\s*Tanggal lahir\s*\w*:\s*(.*)", text),
        "jenis_kelamin":      extract_field(r"Jenis kelamin\s*\w*:\s*(.*)", text),
        "kebangsaan":         extract_field(r"(?:Kebangsaan|Kewarganegaraan)\s*\w*:\s*(.*)", text),
        "agama":              extract_field(r"Agama\s*\w*:\s*(.*)", text),
        "pekerjaan":          extract_field(r"Pekerjaan\s*\w*:\s*(.*)", text),

        # Multi-line field — needs re.DOTALL + explicit stop anchor
        "tempat_tinggal": extract_field(
            r"Tempat tinggal[\s*|\w*]:\s*(.*?)\s*(?=\d+\.\s*Agama|Agama)",
            text,
            multiline=True
        ),
        "keputusan":extract_keputusan(text)
    }

    return data

class MetadataIngestion:
    def __init__(self, credential_path):
        self.credential_path = credential_path

        # for GCS creds
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path
        self.client = storage.Client()
        self.bucket = self.client.bucket("shieran-gcs-bucket")

    def case_details(self, year_month)-> str:
        """extract case detail from parquet file

        Args:
            year_month (_type_): year_month taken from airflow's execution date, in a form of YYYY-mm string

        Returns:
            str: returns gcs_path
        """
        gcs_path = f"gs://shieran-gcs-bucket/final_project/putusan_ma_2025_20pages/grouped_cases/{year_month}/putusan_details.parquet"
        try:
            df = pd.read_parquet(gcs_path)

        except FileNotFoundError as e:
            print(f"No such directory in GCS! period {year_month}, {e}")
            return None
        except Exception as e:
            print(f"Error with dataframe creation of case details for {year_month}, {e}")
            raise

        if df.empty:
            print(f"No DataFrame for period {year_month}, SKIPPING... ")
            return None
        
        print(f"Successfully create dataframe case details for period {year_month}")
        return gcs_path

    def pdf_details(self, year_month)-> str:
        """Loops though objects in GCS bucket to extract pdf metadata

        Args:
            year_month (_type_): year_month taken from airflow's execution date, in a form of YYYY-mm string

        Returns:
            str: returns gcs_path
        """
        blobs = self.bucket.list_blobs(prefix=f"final_project/putusan_ma_2025_20pages/downloaded_pdf/{year_month}")
        pdf_metadata = [] #list of dictionary to turn into dataframe
        for blob in blobs:
            try:
                if blob and blob.name.endswith(".pdf"):
                    pdf_bytes = blob.download_as_bytes()
                    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                        text = ""
                        for page in pdf.pages:
                            text+= page.extract_text() + "\n"

                        text = clean_text(text)
                        metadata_dict= collect_field(text)

                        filename = Path(blob.name).stem
                        metadata_dict["nomor_putusan"] = filename.replace("_","/")
                    pdf_metadata.append(metadata_dict)
                    print(f"Collected_metadata for {blob.name}")
            except Exception as e:
                print(f"Failed to process {blob.name}, {e}")
        
        if not pdf_metadata:
            print(f"No PDFs found for period {year_month}, SKIPPING....")
            return None
        
        df = pd.DataFrame(pdf_metadata)
        print(f"successfully created DataFrame for period {year_month}!")
        transformed_gcs_path = f"gs://shieran-gcs-bucket/final_project/putusan_ma_2025_20pages/downloaded_pdf/{year_month}/transformed_pdf_details.parquet"
        df.to_parquet(transformed_gcs_path, index = False)
        print(f"Successfully saved pdf metadata dataframe to GCS {transformed_gcs_path} ")
        return transformed_gcs_path



