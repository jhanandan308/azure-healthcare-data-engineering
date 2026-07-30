# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer - Hospital Summary
# MAGIC
# MAGIC **Objective**
# MAGIC
# MAGIC Build the Hospital Summary gold table by aggregating provider,
# MAGIC hospital, and specialty information from the Silver layer.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Silver Tables

# COMMAND ----------

provider_df = spark.table("healthcare.silver.providers")
hospital_df = spark.table("healthcare.silver.master_hospital")
specialty_df = spark.table("healthcare.silver.master_specialty")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Provider, Hospital and Specialty Data

# COMMAND ----------

gold_df = (
    provider_df.alias("p")
    .join(
        hospital_df.alias("h"),
        col("p.hospital_id") == col("h.hospital_id"),
        "left"
    )
    .join(
        specialty_df.alias("s"),
        col("p.specialty_id") == col("s.specialty_id"),
        "left"
    )
)

display(gold_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Hospital Summary

# COMMAND ----------

hospital_summary = (
    gold_df.groupBy(
        col("h.hospital_id"),
        col("h.hospital_name"),
        col("h.city"),
        col("h.state"),
        col("h.hospital_type"),
        col("h.bed_capacity"),
        col("h.established_year")
    )
    .agg(
        count(col("p.provider_id")).alias("total_providers"),
        round(avg("p.years_experience"), 2).alias("avg_years_experience"),
        countDistinct("p.specialty_id").alias("total_departments")
    )
    .orderBy(col("total_providers").desc())
)

display(hospital_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data into Gold Layer

# COMMAND ----------

hospital_summary.write.format("delta")     .mode("overwrite")     .save("abfss://gold@nanadls.dfs.core.windows.net/hospital_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Gold Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare.gold.hospital_summary
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://gold@nanadls.dfs.core.windows.net/hospital_summary"
