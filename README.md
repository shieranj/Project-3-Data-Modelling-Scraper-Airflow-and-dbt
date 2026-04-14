# Project-3-Data-Modelling-Scraper-Airflow-and-dbt
End-to-end data pipelines covering batch ingestion, real-time streaming, web scraping (structured & unstructured), dimensional modelling, and datamart delivery. The project is orchestrated with Apache Airflow, transformed with dbt, and deployed on Google Cloud Platform.
- [Core Project](https://docs.google.com/document/d/14GmpAnVlfQPLYaFtlThM_GprW5cFJ8NYdKoEOAwwkfg/edit?tab=t.hmdn9dutq2xh)
- [Project Documentation](https://docs.google.com/document/d/1XijkOXWrAomxmqPIQjHnc-ouDwxDJuq9G-bZHt5DYMc/edit?tab=t.8ynluzw7hb8y)

## 📌 Table of Contents
- Project Summary
- Tech Stack
- Project 1 — Ecommerce Pipeline (Batch & Streaming)
- Project 2 — NY Green Taxi Pipeline
- Project 3 — Adakami Statistics (API Scrape)
- Project 4 — Mahkamah Agung Court Decisions (Unstructured Scrape)
- Monitoring — Discord Alerts
- Data Modelling — dbt Dimensional Models
- Data Mart Visualization

## Project Summary
Four independent pipelines that demonstrate a broad range of real-world data engineering patterns.
| **#** |        **Project**        |       **Source Type**       |    **Ingestion Pattern**    |          **Output**         |
|-------|:-------------------------:|:---------------------------:|:---------------------------:|:---------------------------:|
| 1     | Ecommerce Pipeline        | Synthetic OLTP [PostgreSQL] | Batch + Real-time Streaming | BigQuery Datamart           |
| 2     | NY TLC Green Taxi         | Public TLC parquet file     | Monthly Batch               | BigQuery Datamart           |
| 3     | Adakami Statistics        | Public REST API             | Daily Batch                 | BigQuery  Preparation Layer |
| 4     | Mahkamah Agung  Decisions | HTML & PDF  [web scrape]    | Monthly Batch               | BigQuery  Preparation Layer |

## Tech Stack
| **Category**          |                   **Tools**                   |
|-----------------------|:---------------------------------------------:|
| **Orchestration**     | Apache Airflow                                |
| **Cloud Platform**    | GCP - BigQuery, GCS, Pub/SUb, Dataflow        |
| **Stream Processing** | Apache Beam (Dataflow)                        |
| **Transformation**    | pandas, dbt                                   |
| **Web Scraping**      | Selenium Webdriver, BeautifulSoup, pdfplumber |
| **Database**          | PostgreSQL                                    |
| **Languages**         | Python, SQL                                   |
| **Alerting**          | Discord Webhook                               |
| **Visualization**     | Looker Studio                                 |


## 1️⃣Ecommerce Data Pipeline - Batch & Streaming
Simulates a production-grade ecommerce system with both batch & real-time data flows feeding into BigQuery

#### Highlights
- Designed a **relational PostgreSQL OLTP** schema (customers, transactions, payments) with enforced foreign key relationships and realistic sysnthetic data generation
- Batch pipeline orchestrated by **Airflow**: daily incremental ingestion from PostgreSQL -> GCS -> BigQuery
- Real-time streaming pipeline using **Pub/Sub** as message broker and **Dataflow (Apache Beam)** for continuous ingestion to BigQuery
- **dbt transoformation** with dimensional modeling for analytical consumption
- Data quality checks with **dbt tests** and **Discord Webhook** alerting on pipeline failure

#### Batching Pipeline Flow:
 <p align="center">
     <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a84885cd3b40130e985778e5b7fb78ee900354de/images/dummy_batch.png" width="700"/>
 </p>
 
#### Streaming Pipeline Flow:
 <p align="center">
     <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a84885cd3b40130e985778e5b7fb78ee900354de/images/dummy_streaming.png" width="700"/>
 </p>

## 2️⃣New York Green Taxi Data Pipeline
A monthly batch ELT pipeline ingesting NYC Green Taxi Trip data from NYC TLC public dataset (2023 onwards) into BigQuery

#### Highlights
- Automated monthly ingestion of Green Taxi Parquet files from TLC website, with raw files preserved in GCS and metadata enrichment
- Handles **schema evolution** accross monthly datasets with dynamically **backfill missing columns**, preventing pipeline failres from upstream format changes
- BigQuery storage optimized with **paritioning and clustering** for cost efficient querying
- dbt transformation producing preparation, core, and mart layers to produce tables for reporting and analysis
- Data quality checks with **dbt tests** and **Discord Webhook** alerting on pipeline failure

#### Pipeline Flow:
 <p align="center">
     <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a84885cd3b40130e985778e5b7fb78ee900354de/images/ny_taxi_batch.png" width="700"/>
 </p>

## 3️⃣Adakami Daily Statistics
A daily automated pipeline ingesting lending statistics from Adakami's public data API, staged in GCS and loaded into BigQuery with dbt transformations up to preparation layer. 

#### Highlights
- Daily API ingestion with raw JSON staged in GCS before loading into BigQuery
- **dbt incremental** preparation model using **insert_overwrite** strategy for efficient daily partitioning
- Data cleansing and standardization in dbt preparation layer for analytics ready output
- Discord webhook notifications for pipeline monitoring and failure alerting
  
#### Pipeline Flow:
 <p align="center">
     <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a84885cd3b40130e985778e5b7fb78ee900354de/images/adakami_drawio.png" width="700"/>
 </p>

## 4️⃣Putusan Mahkamah Agung 2025 (Case & PDF Metadata)
The most technically challenging pipeline in this repository. Scraping unstructured legal data (case metadata and PDF verdicts) from Indonesia's Supreme Court website and transforming it into structured data in BigQuery. 

#### Highlights
- Multi-stage scraper extracting case metadata and PDF verdict documents, with **retry logic** and **error handling** for unstable servers and bot detection. 
- Structured data extracted from **PDF documents** using **pdfplumber** for downstream analytics
- **dbt incremental merge strategy** for deduplication, handles both within-run and cross-run duplicates
- Airflow pipeline using **Taskflow API** with dynamic dbt command selectors based on upstream task results
  
#### Pipeline Flow:
 <p align="center">
     <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a84885cd3b40130e985778e5b7fb78ee900354de/images/putusan_ma.png" width="700"/>
 </p>

## ⚠️Pipeline Monitoring - Discord Alerts
All pipelines are integrated with Discord webhook notifications to surface pipeline failures and task level errors. 

<p align="center">
    <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/da2bbcf13c6ab0503625a6c54ef130ad8f62caf1/images/discord_example.png" width="700"/>
</p>

## 𝄜Data Modelling
#### Ecommerce - Dim & Fact Data Modelling

<p align="center">
        <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/da2bbcf13c6ab0503625a6c54ef130ad8f62caf1/images/dummy_dimfact.drawio.png" width="700"/>
</p>

#### NY Green Taxi - Dim & Fact Data Modelling

<p align="center">
    <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/da2bbcf13c6ab0503625a6c54ef130ad8f62caf1/images/ny_taxi_dimfact.drawio.png" width="700"/>
</p>

## 📊Data Mart Visualization
Using one of NY TLC Green Taxi Data's Datamart, Zone Performance. Zone Performance datamart provides statistical information of the top 3 busiest zone in NY, Google's Looker Studio is used for data visualization. 
<p align="center">
    <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/main/images/looker_studio_sample.gif" width="700"/>
</p>
