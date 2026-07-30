# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer - Providers
# MAGIC
# MAGIC **Objective**
# MAGIC
# MAGIC Process the latest incremental provider records from the Bronze layer,
# MAGIC apply business transformations and SCD Type 1 logic, and load the
# MAGIC curated data into the Silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Latest Incremental Data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col, max

latest_ingestion_time = (
    spark.table("healthcare.bronze.providers")
         .agg(max("ingestion_time").alias("ingestion_time"))
         .collect()[0]["ingestion_time"]
)

provider_df = (
    spark.table("healthcare.bronze.providers")
         .filter(col("ingestion_time") == latest_ingestion_time)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Transformations

# COMMAND ----------

provider_sil_df = (
    provider_df
    .withColumn("first_name", initcap(trim(col("first_name"))))
    .withColumn("last_name", initcap(trim(col("last_name"))))
    .withColumn("load_time", current_timestamp())
    .withColumn("email", lower(trim(col("email"))))
)

provider_silver_df = (
    provider_sil_df
    .withColumn(
        "gender",
        when(lower(trim(col("gender"))) == "male", "Male")
        .when(lower(trim(col("gender"))) == "female", "Female")
        .otherwise("Unknown")
    )
    .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data into Silver Layer

# COMMAND ----------

provider_silver_df.createOrReplaceTempView("dimprovider")

from delta.tables import DeltaTable

if spark.catalog.tableExists("healthcare.silver.providers"):

    spark.sql("""
    MERGE INTO healthcare.silver.providers tgt
    USING dimprovider src
    ON tgt.provider_id = src.provider_id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
    """)

else:

    provider_silver_df.write.format("delta").mode("overwrite").save(
        "abfss://silver@nanadls.dfs.core.windows.net/providers"
    )

    spark.sql("""
    CREATE TABLE IF NOT EXISTS healthcare.silver.providers
    USING DELTA
    LOCATION "abfss://silver@nanadls.dfs.core.windows.net/providers"
    """)
