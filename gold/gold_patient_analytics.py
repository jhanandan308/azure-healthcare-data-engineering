# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer - Patient Analytics
# MAGIC
# MAGIC **Objective**
# MAGIC
# MAGIC Build the Patient Analytics gold table by combining curated patient
# MAGIC data with insurance plan information from the Silver layer.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Silver Tables

# COMMAND ----------

patient_df = spark.table("healthcare.silver.patients")
display(patient_df.limit(10))

insurance_df = spark.table("healthcare.silver.master_insurance_plan")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Patient and Insurance Data

# COMMAND ----------

gold_df = (
    patient_df.alias("p")
    .join(
        insurance_df.alias("i"),
        col("p.insurance_plan_id") == col("i.insurance_plan_id"),
        "left"
    )
)

display(gold_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Analytics Columns

# COMMAND ----------

gold_df_final = gold_df.select(
    col("p.patient_id"),
    col("p.full_name").alias("patient_name"),
    col("p.gender"),
    col("p.dob").alias("date_of_birth"),
    col("p.age"),
    col("i.insurance_provider"),
    col("i.plan_name").alias("insurance_plan"),
    col("p.registration_date"),
    col("p.email"),
    col("p.address")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Sensitive Columns

# COMMAND ----------

gold_df_final = gold_df_final.drop(
    col("email"),
    col("address")
)

display(gold_df_final.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data into Gold Layer

# COMMAND ----------

gold_df_final.write.format("delta")     .mode("overwrite")     .save("abfss://gold@nanadls.dfs.core.windows.net/patient_analytics")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Gold Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare.gold.patient_analytics
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://gold@nanadls.dfs.core.windows.net/patient_analytics"
