# Project-3-Data-Modelling-Scraper-Airflow-and-dbt
This repository showcases an end-to-end data engineering project that integrates data extraction, pipeline orchestration, and data transformation into a unified workflow.

The project is designed to simulate real-world data engineering challenges, including:
- Handling semi-structured and unreliable data sources
- Building scalable batch and streaming pipelines
- Automating workflows using orchestration tools
- Transforming raw data into analytics-ready datasets
- Alerting via Webhook

[Core Project](https://docs.google.com/document/d/14GmpAnVlfQPLYaFtlThM_GprW5cFJ8NYdKoEOAwwkfg/edit?tab=t.hmdn9dutq2xh)
[Project Documentation](https://docs.google.com/document/d/1XijkOXWrAomxmqPIQjHnc-ouDwxDJuq9G-bZHt5DYMc/edit?tab=t.8ynluzw7hb8y)

## 1️⃣ Data Pipeline & Modelling (Core Project)
### Dummy Ecommerce Data (Batch & Streaming)

**BATCHING** 
    Design an end to end pipeline that generates ecommerce dummy data, that handles ingestion to PostgreSQL, daily ingestion to BigQuery, apply data warehouse layering concepts with dbt. 
    **Process Flow:**
    ![alt text](https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/fd5942c18b8e4a49753ba20ee88416e330cb59d2/images/dummy_batch.png)

**Streaming** 
Utilize Pub/Sub as message broker and Dataflow to perform data streaming to BigQuery
    **Process Flow:**
    ![alt text](https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/fd5942c18b8e4a49753ba20ee88416e330cb59d2/images/dummy_streaming.png)

### New York Taxi & Limousine Commission dataset
Create a pipeline that is orchestrated with Apache Airflow for alert and run monthly tasks, that handles web scraping from TLC site for Green Taxi Data [starting from 2023],  preserve raw data in GCS, load data to BigQuery with metadata addition, apply transformation with dbt.

**Process Flow:**
![alt text](https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/fd5942c18b8e4a49753ba20ee88416e330cb59d2/images/ny_taxi_batch.png)

## 2️⃣ Web Scraping Pipelines
### Adakami Daily Statistics**
This project simulates data engineering workflow for financial platform data.The pipeline is orchestrated using Apache Airflow, leverages Google Cloud Storage (GCS) as a staging layer, and uses BigQuery as the analytical data warehouse, and dbt for data transformation up till Preparation Layer

**Process Flow**
![alt text](https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/fd5942c18b8e4a49753ba20ee88416e330cb59d2/images/adakami.png)

### Putusan Mahkamah Agung 2025 (Case & PDF Metadata)
The pipeline extracts case metadata and associated PDF files, stages them in Google Cloud Storage (GCS), and processes the data for analytics in BigQuery. The pipeline is orchestrated using Apache Airflow, uses Google Cloud Storage (GCS) as a staging layer, BigQuery as the analytical data warehouse, and dbt for data transformation.
**Process Flow**
![alt text](https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/fd5942c18b8e4a49753ba20ee88416e330cb59d2/images/putusan_ma.png)

## 3️⃣ Orchestration & Transformation
### Airflow DAGs (pipeline automation)
    
### dbt models (data transformation)
#### Dummy Ecommerce Data
**Process Flow**:
![alt text](https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/fd5942c18b8e4a49753ba20ee88416e330cb59d2/images/dbt_dummy.png)

**Dim & Fact Modelling**
![alt text](https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a45ec30a53804fecc4c83c8c69ebb09dc3785ca2/images/snowflake.drawio.png)

#### New York Taxi & Limousine Commission dataset
**Process Flow**:
![alt text](https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a45ec30a53804fecc4c83c8c69ebb09dc3785ca2/images/ny_taxi_dbt.png)

**Dim & Fact Modelling**
![alt text](https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a45ec30a53804fecc4c83c8c69ebb09dc3785ca2/images/taxi.drawio.png)