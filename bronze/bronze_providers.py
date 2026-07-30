# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer - Providers
# MAGIC
# MAGIC **Objective**
# MAGIC
# MAGIC Ingest incremental provider data using Auto Loader,
# MAGIC add ETL metadata, and load the data into the Bronze Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Parameters

# COMMAND ----------

dbutils.widgets.text("source_path", "")
source_path = dbutils.widgets.get("source_path")

dbutils.widgets.text("target", "")
target = dbutils.widgets.get("target")

dbutils.widgets.text("checkpoint", "")
checkpoint = dbutils.widgets.get("checkpoint")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Source Data using Auto Loader

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .option("cloudFiles.schemaLocation", checkpoint)
         .load(source_path)
)

# Add ETL metadata
df = df.withColumn("ingestion_time", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Streaming Data into Bronze Layer

# COMMAND ----------

query = (
    df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpoint)
      .trigger(availableNow=True)
      .option("path", target)
      .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Bronze Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare.bronze.providers
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@nanadls.dfs.core.windows.net/providers"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_records
# MAGIC FROM healthcare.bronze.providers;
