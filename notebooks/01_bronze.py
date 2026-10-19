# Databricks notebook source
STORAGE = "stdarkstore55"
LANDING = f"abfss://landing@{STORAGE}.dfs.core.windows.net"
BRONZE = f"abfss://bronze@{STORAGE}.dfs.core.windows.net"

print("Reading from:", LANDING)
print("Writing to:  ", BRONZE)

# COMMAND ----------

from pyspark.sql import functions as F


def ingest_parquet(name):
    raw_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{BRONZE}/_schemas/{name}")
        .load(f"{LANDING}/{name}/")
    )

    with_audit = (
        raw_stream
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    query = (
        with_audit.writeStream.format("delta")
        .option("checkpointLocation", f"{BRONZE}/_checkpoints/{name}")
        .option("mergeSchema", "true")
        .partitionBy("date")
        .trigger(availableNow=True)
        .start(f"{BRONZE}/{name}")
    )
    query.awaitTermination()
    print(f"{name}: done")

# COMMAND ----------

for table_name in ["orders", "inventory", "weather"]:
    ingest_parquet(table_name)

# COMMAND ----------

for table_name in ["stores", "products"]:
    (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{LANDING}/{table_name}.csv")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .write.format("delta").mode("overwrite")
        .save(f"{BRONZE}/{table_name}")
    )
    print(f"{table_name}: done")

# COMMAND ----------

for table_name in ["orders", "inventory", "weather", "stores", "products"]:
    df = spark.read.format("delta").load(f"{BRONZE}/{table_name}")
    print(f"{table_name:10} rows = {df.count()}")

orders = spark.read.format("delta").load(f"{BRONZE}/orders")
inventory = spark.read.format("delta").load(f"{BRONZE}/inventory")
print("Distinct order_id in orders:", orders.select("order_id").distinct().count())
print("Inventory rows with qty < 0:", inventory.filter("qty_on_shelf < 0").count())
orders.select("order_id", "store_id", "order_ts", "date", "_source_file", "_ingested_at").show(3, truncate=False)

# COMMAND ----------

print("Orders rows after re-run:", spark.read.format("delta").load(f"{BRONZE}/orders").count())