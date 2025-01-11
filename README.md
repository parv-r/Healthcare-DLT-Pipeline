## Healthcare Data Processing with PySpark and Delta Live Tables (DLT)

This repository contains PySpark notebooks and SQL scripts designed to process and analyze healthcare data using Databricks. The workflow ingests raw data, transforms it through structured pipelines, and outputs enriched analytics tables categorized into Bronze, Silver, and Gold quality layers. This project demonstrates expertise in data engineering, real-time processing, and Delta Lake capabilities.

### Overview

#### Key Objectives:
1. Ingest raw healthcare data and store it in Delta tables.
2. Transform raw data into enriched, actionable datasets using Delta Live Tables (DLT).
3. Implement data quality layers to ensure clean and reliable insights.
4. Provide aggregated statistics for business intelligence purposes.

#### Pipeline Layers:
- Bronze: Raw data ingestion.
- Silver: Data enrichment with joins and quality checks.
- Gold: Aggregated statistics for diagnostics and patient demographics.

#### Pipeline Details

1. ##### feed_raw_tables_notebook.py

This PySpark notebook ingests raw data from CSV files and writes it to Delta tables.
- Dimension Table: healthcare_dlt.default.raw_diagnosis_map (diagnosis mapping file).
- Fact Table: healthcare_dlt.default.raw_patients_daily (daily patient records for 2024).

##### Steps:
- Load diagnosis mapping data from a CSV file.
- Write the data into a Delta table for further processing.
- Load and process multiple daily patient files, casting the admission_date column to the appropriate date type.
- Append the processed data to a Delta table with schema merging.

2. ##### healthcare_dlt_processing.py

This Delta Live Tables (DLT) notebook defines a continuously running data pipeline to transform raw data into meaningful insights.

##### Bronze Tables:
- diagnostic_mapping: Raw diagnosis mapping data.
- daily_patients: Streaming ingestion of patient records.

##### Silver Table:
- processed_patient_data: Combines patient data with diagnostic mapping using a left join. Enriches data with diagnosis descriptions.

##### Gold Tables:
- patient_statistics_by_diagnosis: Aggregated patient statistics grouped by diagnosis descriptions.
- Total patient count, average age, unique gender count, minimum age, and maximum age.
- patient_statistics_by_gender: Aggregated patient statistics grouped by gender.
- Total patient count, average age, unique diagnosis count, minimum age, and maximum age.

#### Technologies Used
- PySpark: For data ingestion and processing.
- Databricks Delta Lake: For scalable and reliable data storage.
- Delta Live Tables (DLT): To manage and orchestrate the data pipeline.
- Databricks SQL: For creating streaming and batch live tables.

#### How to Use
1. Clone the repository: git clone <repository_url>
2. Upload the notebooks (feed_raw_tables_notebook.py and healthcare_dlt_processing.py) to your Databricks workspace.
3. Configure the input file paths in feed_raw_tables_notebook.py to match your storage location.
4. Run the feed_raw_tables_notebook.py to ingest raw data.
5. Deploy and start the DLT pipeline using healthcare_dlt_processing.py.

Future Enhancements
- Data Validation: Implement validation logic to handle missing or corrupt records.
- Partitioning: Optimize Delta tables by introducing partitioning for large datasets.
- Monitoring: Add logging and monitoring for pipeline health and performance.