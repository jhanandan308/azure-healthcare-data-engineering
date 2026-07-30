# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer - Insurance Summary
# MAGIC
# MAGIC **Objective**
# MAGIC
# MAGIC Build the Insurance Summary gold table by aggregating patient
# MAGIC and insurance plan information from the Silver layer.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Silver Tables

# COMMAND ----------

insurance_df = spark.table("healthcare.silver.master_insurance_plan")
patient_df = spark.table("healthcare.silver.patients")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Insurance Summary

# COMMAND ----------

insurance_summary = (
    gold_df.groupBy(
        col("i.insurance_provider"),
        col("i.plan_name")
    )
    .agg(
        count(col("p.patient_id")).alias("total_patients"),
        round(avg(col("p.age")), 2).alias("avg_age"),
        sum(when(col("p.gender")=="Male",1).otherwise(0)).alias("male_patients"),
        sum(when(col("p.gender")=="Female",1).otherwise(0)).alias("female_patients")
    )
)

display(insurance_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data into Gold Layer

# COMMAND ----------

insurance_summary.write.format("delta").mode("overwrite").save(
    "abfss://gold@nanadls.dfs.core.windows.net/insurance_summary"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Gold Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare.gold.insurance_summary
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://gold@nanadls.dfs.core.windows.net/insurance_summary"
