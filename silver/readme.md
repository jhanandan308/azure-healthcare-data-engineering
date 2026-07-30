# Silver Layer

## Overview

The Silver layer is responsible for transforming, cleansing, and standardizing the raw data ingested into the Bronze layer.

This layer improves data quality by applying business rules, handling missing values, standardizing formats, and implementing Slowly Changing Dimensions (SCD). The transformed data is stored as Delta tables and serves as the foundation for analytical processing in the Gold layer.

---

## Objectives

- Clean and validate raw data
- Standardize column formats and data types
- Remove duplicate records
- Handle null and invalid values
- Apply business transformations
- Implement Slowly Changing Dimension (SCD) logic
- Create optimized Delta tables for downstream analytics

---

## Datasets Processed

- Patients
- Providers
- Hospitals
- Claims
- Insurance

---

## Transformations Performed

### Patients

- Trim leading and trailing spaces
- Standardize text columns
- Handle missing values
- Add audit columns
- Implement SCD Type 2 for historical tracking

### Providers

- Standardize names using proper case
- Convert email addresses to lowercase
- Normalize gender values
- Generate Full Name column
- Add Load Timestamp
- Implement SCD Type 1

### Hospitals

- Standardize hospital names
- Clean address information
- Remove duplicate records
- Add audit columns

### Claims

- Validate claim amounts
- Handle invalid records
- Standardize claim status
- Generate calculated columns

### Insurance

- Standardize insurance provider information
- Clean policy details
- Remove duplicates

---

## Technologies Used

- Azure Databricks
- Apache Spark
- PySpark
- Delta Lake
- SQL

---

## Output

The processed data is stored as Delta tables in the Silver layer and is used by the Gold layer to generate business analytics and reporting.
