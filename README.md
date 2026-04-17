# Healthcare Data Lakehouse Pipeline

## Objective

The objective of this project is to build an end-to-end healthcare data pipeline using **Databricks Delta Live Tables (DLT)** following the **Medallion Architecture (Bronze, Silver, Gold)**.

The pipeline processes raw healthcare data and generates business insights such as revenue trends, patient behavior, and operational KPIs.

---

## Dataset

The project uses multiple healthcare-related datasets:

* Appointments
* Doctors
* Departments
* Billings
* Diagnostics

---

## Architecture

The pipeline is implemented using Medallion Architecture:

* **Bronze Layer** → Raw data ingestion from CSV files
* **Silver Layer** → Data cleaning, transformation, and joins
* **Gold Layer** → Business-level aggregations and analytics

---

## Technologies Used

* PySpark
* Delta Live Tables (DLT)
* Databricks Lakehouse
* SQL (for analytics layer)

---

## Team Roles

* **Member A** → Bronze (Data Ingestion)
* **Member B** → Silver (Data Transformation)
* **Member C** → Gold (Analytics + Documentation)

---

## Pipeline Flow

1. Load raw CSV data into Bronze tables
2. Clean, standardize, and validate data in Silver
3. Join multiple datasets into a unified table (`silver_joined`)
4. Generate analytics and KPIs in Gold layer

---

## Key Insights Generated

* Monthly revenue trends
* Revenue by department and doctor
* No-show rate analysis
* Patient revisit behavior
* Daily and monthly appointment trends
* Booking behavior (same-day vs advance)
* Lead time analysis
* Age-group patient distribution
* Payment status and revenue loss
* Diagnostic test popularity and results

---

## Steps to Run

1. Create a Delta Live Tables pipeline in Databricks
2. Add the following files:

   * `bronze.py`
   * `silver.py`
   * `gold.py`
3. Configure source data paths
4. Run the pipeline
5. Query Gold tables for analytics

---

## Highlights

* Implemented **DLT expectations** for data quality
* Used **broadcast joins** for performance optimization
* Designed multiple **analytical tables for business insights**
* Built a scalable **Lakehouse architecture**

---

## Output

Refer to the `screenshots/` folder for:

* Pipeline execution
* Table outputs
* Analytical results

---
