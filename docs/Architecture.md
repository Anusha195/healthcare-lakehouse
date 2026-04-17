# Architecture Overview

## Medallion Architecture

This project follows the Medallion Architecture pattern:

```
Bronze → Silver → Gold
```

---

## Bronze Layer (Raw Ingestion)

* Ingests raw CSV data into Delta tables
* Minimal transformations
* Ensures schema consistency

### Tables:

* bronze_appointments
* bronze_doctors
* bronze_departments
* bronze_billings
* bronze_diagnostics

---

## Silver Layer (Transformation & Cleaning)

* Data standardization and cleansing
* Null handling and data validation
* Type casting and normalization
* Removal of duplicates
* Joining multiple datasets

### Key Features:

* DLT expectations for data quality
* Broadcast joins for performance
* Unified dataset: `silver_joined`

---

## Gold Layer (Analytics)

* Business-level aggregations
* KPI generation
* Time-based analysis
* Advanced insights using transformations

### Key Analytical Tables:

* Monthly revenue and trends
* Revenue by department and doctor
* Patient revisit analysis
* No-show rate analysis
* Booking behavior analysis
* Lead time analysis
* Age-group segmentation
* Payment and revenue loss analysis
* Diagnostic insights

---

## Data Flow

1. Raw data → Bronze tables
2. Cleaned data → Silver tables
3. Joined dataset → `silver_joined`
4. Aggregations → Gold tables

---

## Key Design Decisions

* Used Delta Live Tables for pipeline automation
* Separated layers for maintainability and scalability
* Optimized joins using broadcast strategy
* Designed multiple Gold tables for flexible analytics

---

## Outcome

The architecture enables:

* Scalable data processing
* Reliable data quality
* Rich business insights
* Clear separation of concerns

---
