# Gold Layer

## Overview

The Gold layer is the business-ready layer of the Medallion Architecture. It contains curated datasets and aggregated metrics that support reporting, dashboards, and business decision-making.

The Gold layer consumes cleansed and standardized data from the Silver layer to generate meaningful analytical insights for healthcare stakeholders.

---

## Objectives

- Generate business-ready datasets
- Create aggregated reporting tables
- Build analytical views for stakeholders
- Optimize data for reporting and dashboarding
- Provide a single source of truth for business metrics

---

## Gold Tables

### Hospital Summary

Provides key operational insights for hospitals.

**Sample Metrics**

- Total Patients
- Total Providers
- Total Claims
- Average Claim Amount
- Claim Status Distribution

---

### Insurance Summary

Provides analytics related to insurance providers and claims.

**Sample Metrics**

- Total Policies
- Total Claims
- Approved Claims
- Rejected Claims
- Average Claim Amount by Insurance Provider

---

### Provider Analytics

Provides performance insights for healthcare providers.

**Sample Metrics**

- Number of Patients Handled
- Total Claims Processed
- Average Claim Amount
- Provider-wise Performance Statistics

---

## Technologies Used

- Azure Databricks
- Apache Spark
- PySpark
- Delta Lake
- SQL

---

## Data Flow

```text
Bronze
   │
   ▼
Silver
   │
   ▼
Gold
   │
   ▼
Business Reports & Dashboards
```

---

## Business Benefits

- Faster analytical queries
- Optimized reporting datasets
- Simplified dashboard creation
- Reliable business metrics
- Improved decision-making

---

## Output

The Gold layer serves as the final analytical layer and provides trusted datasets for reporting tools such as Power BI, Tableau, or any downstream analytics platform.
