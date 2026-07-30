# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer - Master Specialty
# MAGIC
# MAGIC **Objective**
# MAGIC
# MAGIC Ingest `master_specialty.csv` from the Source layer into the Bronze layer,
# MAGIC perform basic data quality validation, and store the data as a Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Parameters

# COMMAND ----------

dbutils.widgets.text("source_path", "")
source_path = dbutils.widgets.get("source_path")

dbutils.widgets.text("target", "")
target = dbutils.widgets.get("target")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Source Data

# COMMAND ----------

df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{source_path}/master_specialty.csv")
)

print("Schema:")
df.printSchema()

# Display sample records
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Checks

# COMMAND ----------

from pyspark.sql.functions import col, sum

total_rows = df.count()
print(f"Total Rows: {total_rows}")

null_df = df.select(
    [sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
)
display(null_df)

duplicate_count = (
    df.groupBy("specialty_id")
      .count()
      .filter(col("count") > 1)
      .count()
)

print(f"Duplicate specialty_id records: {duplicate_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data into Bronze Layer

# COMMAND ----------

(
    df.write
      .format("delta")
      .mode("overwrite")
      .save(target)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Bronze Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare.bronze.master_specialty
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@nanadls.dfs.core.windows.net/master_specialty"
