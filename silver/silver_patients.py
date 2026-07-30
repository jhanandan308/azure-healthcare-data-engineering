# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer - Patients
# MAGIC
# MAGIC **Objective**
# MAGIC
# MAGIC Process the latest incremental patient records from the Bronze layer,
# MAGIC apply business transformations and SCD Type 2 logic, and load the
# MAGIC curated data into the Silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Latest Incremental Data

# COMMAND ----------

from pyspark.sql.functions import *

latest_ingestion_time = (
    spark.table("healthcare.bronze.patients")
         .agg(max("ingestion_time").alias("ingestion_time"))
         .collect()[0]["ingestion_time"]
)

df = (
    spark.table("healthcare.bronze.patients")
         .filter(col("ingestion_time") == latest_ingestion_time)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Transformations

# COMMAND ----------

s_df = (
    df.withColumn("age", floor(datediff(current_date(), col("dob")) / 365.25))
      .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
      .withColumn("load_at", current_timestamp())
      .withColumn("email", lower(trim(col("email"))))
      .withColumn("first_name", initcap(trim(col("first_name"))))
      .withColumn("last_name", initcap(trim(col("last_name"))))
)

silver_df = (
    s_df.withColumn(
        "gender",
        when(lower(trim(col("gender"))) == "male", "Male")
        .when(lower(trim(col("gender"))) == "female", "Female")
        .otherwise("Unknown")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply SCD Type 2 Columns

# COMMAND ----------

silver_df = (
    silver_df.withColumn("start_date", current_date())
             .withColumn("end_date", lit(None).cast("date"))
             .withColumn("is_active", lit("Y"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data into Silver Layer

# COMMAND ----------

if spark.catalog.tableExists("healthcare.silver.patients"):

    silver_df.createOrReplaceTempView("dimpatients")

    spark.sql("""
    MERGE INTO healthcare.silver.patients t
    USING dimpatients s
    ON t.patient_id = s.patient_id
       AND t.is_active = 'Y'
    WHEN MATCHED
         AND (
             t.phone <> s.phone
             OR t.insurance_plan_id <> s.insurance_plan_id
         )
    THEN UPDATE SET
         t.end_date = current_date(),
         t.is_active = 'N'
    """)

    spark.sql("""
    INSERT INTO healthcare.silver.patients
    SELECT src.*
    FROM dimpatients src
    LEFT JOIN healthcare.silver.patients tgt
           ON src.patient_id = tgt.patient_id
          AND tgt.is_active = 'Y'
    WHERE tgt.patient_id IS NULL
    """)

else:

    silver_df.write.format("delta")         .mode("overwrite")         .save("abfss://silver@nanadls.dfs.core.windows.net/patients")

    spark.sql("""
    CREATE TABLE IF NOT EXISTS healthcare.silver.patients
    USING DELTA
    LOCATION "abfss://silver@nanadls.dfs.core.windows.net/patients"
    """)
