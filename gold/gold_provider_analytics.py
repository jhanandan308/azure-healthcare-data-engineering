# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer - Provider Analytics
# MAGIC
# MAGIC **Objective**
# MAGIC
# MAGIC Build the Provider Analytics gold table by combining curated provider,
# MAGIC hospital, and specialty data from the Silver layer.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Analytics Columns

# COMMAND ----------

gold_df_final = gold_df.select(
    col("p.provider_id"),
    col("p.full_name").alias("provider_name"),
    col("p.gender"),
    col("p.qualification"),
    col("p.years_experience"),
    col("h.hospital_name"),
    col("h.city"),
    col("h.state"),
    col("s.specialty_name").alias("department"),
    col("s.consultation_fee")
)

display(gold_df_final.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data into Gold Layer

# COMMAND ----------

gold_df_final.write.format("delta")     .mode("overwrite")     .save("abfss://gold@nanadls.dfs.core.windows.net/provider_analytics")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Gold Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare.gold.provider_analytics
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://gold@nanadls.dfs.core.windows.net/provider_analytics"
