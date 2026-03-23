# Project-3-Data-Modelling-Scraper-Airflow-and-dbt
This repository showcases an end-to-end data engineering project that integrates data extraction, pipeline orchestration, and data transformation into a unified workflow.

The project is designed to simulate real-world data engineering challenges, including:
- Handling semi-structured and unreliable data sources
- Building scalable batch and streaming pipelines
- Automating workflows using orchestration tools
- Transforming raw data into analytics-ready datasets
- Alerting via Webhook

## 📄 Documentation
[Core Project](https://docs.google.com/document/d/14GmpAnVlfQPLYaFtlThM_GprW5cFJ8NYdKoEOAwwkfg/edit?tab=t.hmdn9dutq2xh)
[Project Documentation](https://docs.google.com/document/d/1XijkOXWrAomxmqPIQjHnc-ouDwxDJuq9G-bZHt5DYMc/edit?tab=t.8ynluzw7hb8y)

## 1️⃣ Data Pipeline & Modelling (Core Project)
### Dummy Ecommerce Data (Batch & Streaming)
**BATCHING** 
    Design an end to end pipeline that generates ecommerce dummy data, that handles ingestion to PostgreSQL, daily ingestion to BigQuery, apply data warehouse layering concepts with dbt. 
    **Process Flow:**
    <p align="center">
        <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a84885cd3b40130e985778e5b7fb78ee900354de/images/dummy_batch.png" width="700"/>
    </p>

**Streaming** 
Utilize Pub/Sub as message broker and Dataflow to perform data streaming to BigQuery
    **Process Flow:**
    <p align="center">
        <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a84885cd3b40130e985778e5b7fb78ee900354de/images/dummy_streaming.png" width="700"/>
    </p>

### New York Taxi & Limousine Commission dataset
Create a pipeline that is orchestrated with Apache Airflow for alert and run monthly tasks, that handles web scraping from TLC site for Green Taxi Data [starting from 2023],  preserve raw data in GCS, load data to BigQuery with metadata addition, apply transformation with dbt.
**Process Flow:**
    <p align="center">
        <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a84885cd3b40130e985778e5b7fb78ee900354de/images/ny_taxi_batch.png" width="700"/>
    </p>

## 2️⃣ Web Scraping Pipelines
### Adakami Daily Statistics
This project simulates data engineering workflow for financial platform data.The pipeline is orchestrated using Apache Airflow, leverages Google Cloud Storage (GCS) as a staging layer, and uses BigQuery as the analytical data warehouse, and dbt for data transformation up till Preparation Layer
**Process Flow**
    <p align="center">
        <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a84885cd3b40130e985778e5b7fb78ee900354de/images/adakami_drawio.png" width="700"/>
    </p>

### Putusan Mahkamah Agung 2025 (Case & PDF Metadata)
The pipeline extracts case metadata and associated PDF files, stages them in Google Cloud Storage (GCS), and processes the data for analytics in BigQuery. The pipeline is orchestrated using Apache Airflow, uses Google Cloud Storage (GCS) as a staging layer, BigQuery as the analytical data warehouse, and dbt for data transformation.
**Process Flow**
    <p align="center">
        <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/a84885cd3b40130e985778e5b7fb78ee900354de/images/putusan_ma.png" width="700"/>
    </p>

## 3️⃣ Alerts & Transformation
### Discord Alert via Airflow
    <p align="center">
        <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/da2bbcf13c6ab0503625a6c54ef130ad8f62caf1/images/discord_example.png" width="700"/>
    </p>

### dbt models (data transformation)
#### Dummy Ecommerce Data

**Dim & Fact Modelling**
    <p align="center">
        <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/da2bbcf13c6ab0503625a6c54ef130ad8f62caf1/images/dummy_dimfact.drawio.png" width="700"/>
    </p>

#### New York Taxi & Limousine Commission dataset
**Dim & Fact Modelling**
    <p align="center">
        <img src="https://github.com/shieranj/Project-3-Data-Modelling-Scraper-Airflow-and-dbt/blob/da2bbcf13c6ab0503625a6c54ef130ad8f62caf1/images/ny_taxi_dimfact.drawio.png" width="700"/>
    </p>