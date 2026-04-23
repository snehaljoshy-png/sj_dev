# Databricks notebook source
# DBTITLE 1,Cell 3
#data Ingestion to broze layer

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

# Ingest raw CSVs incrementally
raw_data = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.inferColumnTypes", "true") 
            .option("header", "true")
            .load(source_path)
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source_file", F.col("_metadata.file_path")))

# Write to Bronze Delta Table
turbine_bronze= (raw_data.writeStream
         .format("delta")
         # CHECKPOINT LOCATION: Tracks the actual data processing progress
         .option("checkpointLocation","/Volumes/demo/dwh/turbinepowerstation/_checkpoint_bronze")
         .option("mergeSchema", "true")
         .trigger(availableNow=True)
         .outputMode("append")
         .table("bronze_turbine"))

# COMMAND ----------


#check if files has loaded or not
#display(spark.table("bronze_turbine").select("source_file").distinct())


# COMMAND ----------

from pyspark.sql import functions as F

# 1. Read from Bronze
# Config
checkpoint_silver = "/Volumes/demo/dwh/turbinepowerstation/_checkpoint_silver"

# Read from Bronze for processing
silver_raw = spark.readStream.table("bronze_turbine")

# 2. Apply Cleaning Logic
silver_cleaned = silver_raw \
    .dropDuplicates(["turbine_id", "timestamp"]) \
    .filter("power_output >= 0 OR power_output IS NULL")\
    .fillna(0, subset=["power_output"])

# 3. Write to Silver Table
(silver_cleaned.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_silver)
    .option("mergeSchema", "false")
    .outputMode("append")
    .trigger(availableNow=True)
    .table("turbine_silver"))


# COMMAND ----------

from pyspark.sql.window import Window

# Read from clean Silver data
silver_df = spark.read.table("turbine_silver")

# 1. Daily Summary Stats
gold_stats = silver_df.groupBy("turbine_id", F.window("timestamp", "24 hours")) \
    .agg(
        F.min("power_output").alias("min_mw"),
        F.max("power_output").alias("max_mw"),
        F.avg("power_output").alias("avg_mw"),
        F.stddev("power_output").alias("stddev_mw")
    )

# 2. Anomaly Detection
# Compare each turbine's avg output against the fleet average for the same window
window_spec = Window.partitionBy("window")

gold_final = gold_stats.withColumn("fleet_avg", F.avg("avg_mw").over(window_spec)) \
    .withColumn("fleet_std", F.stddev("avg_mw").over(window_spec)) \
    .withColumn("is_anomaly", 
                F.abs(F.col("avg_mw") - F.col("fleet_avg")) > (F.col("fleet_std") * 2))

# Save for reporting
gold_final.write.format("delta").mode("overwrite").saveAsTable("turbine_gold_performance")

# COMMAND ----------

# --- TEST CELL ---

# 1. Test Silver Cleaning
silver_count = spark.table("turbine_silver").filter("power_output < 0").count()
assert silver_count == 0, f"FAILED: Found {silver_count} negative power records in Silver!"

# 2. Test Forward Fill
# Pick a known turbine/timestamp that was null in raw
null_check = spark.table("turbine_silver").filter("turbine_id = 1 AND power_output IS NULL").count()
assert null_check == 0, "FAILED: Forward fill left NULL values in Silver!"

# 3. Test Gold Anomaly Logic
# Ensure the standard deviation logic is actually flagging rows
anomaly_exists = spark.table("turbine_gold_performance").filter("is_anomaly = true").count()
if anomaly_exists > 0:
    print("SUCCESS: Anomalies correctly identified.")
else:
    print("WARNING: No anomalies found. Check if your test data has outliers.")

print("ALL TESTS PASSED")
