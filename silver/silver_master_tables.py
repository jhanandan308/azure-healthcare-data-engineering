# Databricks notebook source
from pyspark.sql.functions import *

# MAGIC %md
# MAGIC ## Silver Layer - Master Tables
# MAGIC
# MAGIC **Objective**
# MAGIC
# MAGIC Apply standard data cleansing and formatting to the master reference
# MAGIC tables and load them into the Silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Master Hospital

# COMMAND ----------

hospital_df = spark.table("healthcare.bronze.master_hospital")

display(hospital_df)

print(f"Row Count: {hospital_df.count()}")

hospital_df.printSchema()

hospital_silver_df = (
    hospital_df
    .withColumn("hospital_name", initcap(trim(col("hospital_name"))))
    .withColumn("city", initcap(trim(col("city"))))
    .withColumn("state", initcap(trim(col("state"))))
    .withColumn("hospital_type", initcap(trim(col("hospital_type"))))
    .withColumn("load_time", current_timestamp())
)

hospital_silver_df.write.format("delta").mode("overwrite").save(
    "abfss://silver@nanadls.dfs.core.windows.net/master_hospital"
)

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare.silver.master_hospital
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@nanadls.dfs.core.windows.net/master_hospital"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Master Specialty

# COMMAND ----------

specialty_df = spark.table("healthcare.bronze.master_specialty")

display(specialty_df.limit(10))

specialty_silver_df = (
    specialty_df
    .withColumn("specialty_name", initcap(trim(col("specialty_name"))))
    .withColumn("load_time", current_timestamp())
)

specialty_silver_df.write.format("delta").mode("overwrite").save(
    "abfss://silver@nanadls.dfs.core.windows.net/master_specialty"
)

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare.silver.master_specialty
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@nanadls.dfs.core.windows.net/master_specialty"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Master Insurance Plan

# COMMAND ----------

insurance_plan_df = spark.table("healthcare.bronze.master_insurance_plan")

display(insurance_plan_df.limit(10))

silver_insurance_plan_df = (
    insurance_plan_df
    .withColumn("insurance_provider", initcap(trim(col("insurance_provider"))))
    .withColumn("load_time", current_timestamp())
)

silver_insurance_plan_df.write.format("delta").mode("overwrite").save(
    "abfss://silver@nanadls.dfs.core.windows.net/master_insurance_plan"
)

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare.silver.master_insurance_plan
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@nanadls.dfs.core.windows.net/master_insurance_plan"
