# Azure Healthcare Data Engineering Project

An end-to-end Azure Data Engineering project that demonstrates how healthcare data can be ingested, transformed, and analyzed using the Medallion Architecture on Microsoft Azure.

---

## Overview

This project implements a complete Azure Data Engineering pipeline for processing healthcare datasets using Azure Data Lake Storage Gen2, Azure Databricks, Delta Lake, and Azure Data Factory.

The pipeline follows the Medallion Architecture, where raw healthcare data is ingested into the Bronze layer, cleaned and standardized in the Silver layer, and transformed into business-ready analytical datasets in the Gold layer.

The project demonstrates practical data engineering concepts including data ingestion, data transformation, Delta Lake, Slowly Changing Dimensions (SCD), dimensional modeling, and analytical data preparation using PySpark.

---

## Project Objectives

- Build an end-to-end Azure Data Engineering pipeline.
- Implement the Medallion Architecture using Bronze, Silver, and Gold layers.
- Process healthcare datasets using Azure Databricks and PySpark.
- Store data using Delta Lake.
- Perform data cleansing and standardization.
- Implement Slowly Changing Dimensions (SCD).
- Generate business-ready analytical datasets including:
  - Patient Analytics
  - Provider Analytics
  - Hospital Summary
  - Insurance Summary

---

## Technology Stack

- Azure Data Lake Storage Gen2
- Azure Data Factory
- Azure Databricks
- Apache Spark (PySpark)
- Delta Lake
- SQL
- Git
- GitHub

---

## Project Architecture

The project follows an end-to-end Azure Data Engineering architecture built on the Medallion Architecture. Healthcare datasets are ingested into Azure Data Lake Storage Gen2, orchestrated using Azure Data Factory, processed in Azure Databricks with PySpark, and stored as Delta Lake tables across the Bronze, Silver, and Gold layers.


---

## Repository Structure

```text
azure-healthcare-data-engineering/
│
├── architecture/
│   └── architecture.png
│
├── bronze/
│   ├── README.md
│   └── Bronze notebooks
│
├── silver/
│   ├── README.md
│   └── Silver notebooks
│
├── gold/
│   ├── README.md
│   └── Gold notebooks
│
├── README.md
├── LICENSE
└── .gitignore
```

---

## Project Layers

### Bronze Layer

Stores raw healthcare datasets as Delta tables while preserving the original source data.

### Silver Layer

Performs data cleansing, standardization, business rule implementation, and Slowly Changing Dimension (SCD) processing to create curated datasets.

### Gold Layer

Creates business-ready analytical datasets for reporting and decision-making, including:

- Patient Analytics
- Provider Analytics
- Hospital Summary
- Insurance Summary

---

## Future Enhancements

- Azure Databricks Workflows
- Auto Loader
- Change Data Capture (CDC)
- CI/CD using GitHub Actions
- Power BI Dashboard Integration

---

## Author

**Nandan Jha**
