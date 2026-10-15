# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

STORAGE = "stdarkstore55"
BRONZE = f"abfss://bronze@{STORAGE}.dfs.core.windows.net"
SILVER = f"abfss://silver@{STORAGE}.dfs.core.windows.net"


def read_bronze(name):
    return spark.read.format("delta").load(f"{BRONZE}/{name}")


def silver_count(name):
    return spark.read.format("delta").load(f"{SILVER}/{name}").count()

# COMMAND ----------

def drop_duplicates_keep_first(df, keys):
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").asc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def apply_rules(df, rules):
    reason = F.lit(None).cast("string")
    for text, bad_condition in reversed(rules):
        reason = F.when(F.expr(bad_condition), F.lit(text)).otherwise(reason)
    return df.withColumn("_reject_reason", reason)


def save_quarantine(bad_df, source):
    data_cols = [c for c in bad_df.columns if c != "_reject_reason"]
    out = bad_df.select(
        F.lit(source).alias("source_table"),
        F.col("_reject_reason").alias("reject_reason"),
        F.to_json(F.struct(*data_cols)).alias("record"),
        F.current_timestamp().alias("quarantined_at"),
    )
    out.write.format("delta").mode("overwrite").save(f"{SILVER}/_quarantine/{source}")


def upsert(df, name, keys, partition_by=None):
    path = f"{SILVER}/{name}"
    if DeltaTable.isDeltaTable(spark, path):
        condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])
        (
            DeltaTable.forPath(spark, path).alias("t")
            .merge(df.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.save(path)


def log_run(table, rows_in, duplicates, rejected, rows_out):
    schema = ("table_name string, rows_in long, duplicates_removed long, "
              "rows_rejected long, rows_out long")
    (
        spark.createDataFrame([(table, rows_in, duplicates, rejected, rows_out)], schema)
        .withColumn("run_at", F.current_timestamp())
        .write.format("delta").mode("append").save(f"{SILVER}/_run_log")
    )

# COMMAND ----------

for name, key in [("stores", "store_id"), ("products", "product_id")]:
    df = read_bronze(name)
    n_in = df.count()
    clean = drop_duplicates_keep_first(df.filter(f"{key} IS NOT NULL"), [key])
    n_clean = clean.count()
    upsert(clean, name, [key])
    log_run(name, n_in, n_in - n_clean, 0, silver_count(name))
    print(f"{name}: {silver_count(name)} rows in Silver")

store_list = ",".join(f"'{r.store_id}'" for r in spark.read.format("delta").load(f"{SILVER}/stores").select("store_id").collect())
product_list = ",".join(f"'{r.product_id}'" for r in spark.read.format("delta").load(f"{SILVER}/products").select("product_id").collect())
print("Valid stores:", store_list)

# COMMAND ----------

bronze_orders = read_bronze("orders")
n_in = bronze_orders.count()

orders_dedup = drop_duplicates_keep_first(bronze_orders, ["order_id"])
n_dups = n_in - orders_dedup.count()

rules = [
    ("missing_order_id", "order_id IS NULL"),
    ("missing_timestamp", "order_ts IS NULL"),
    ("invalid_qty", "qty IS NULL OR qty <= 0"),
    ("invalid_price", "unit_price_inr IS NULL OR unit_price_inr <= 0"),
    ("unknown_store", f"store_id IS NULL OR store_id NOT IN ({store_list})"),
    ("unknown_product", f"product_id IS NULL OR product_id NOT IN ({product_list})"),
]
flagged = apply_rules(orders_dedup, rules)
good = flagged.filter("_reject_reason IS NULL")
bad = flagged.filter("_reject_reason IS NOT NULL")
save_quarantine(bad, "orders")

silver_orders = good.select(
    "order_id", "store_id", "product_id", "qty", "unit_price_inr", "order_ts",
    F.to_date("order_ts").alias("order_date"),
    F.date_trunc("hour", "order_ts").alias("order_hour"),
    (F.col("qty") * F.col("unit_price_inr")).alias("revenue_inr"),
    "_source_file", "_ingested_at",
)
upsert(silver_orders, "orders", ["order_id"], partition_by="order_date")

log_run("orders", n_in, n_dups, bad.count(), silver_count("orders"))
print("orders  in:", n_in, "| duplicates removed:", n_dups, "| rejected:", bad.count(), "| silver rows:", silver_count("orders"))

# COMMAND ----------

bronze_inv = read_bronze("inventory")
n_in = bronze_inv.count()

inv_dedup = drop_duplicates_keep_first(bronze_inv, ["store_id", "product_id", "snapshot_ts"])
n_dups = n_in - inv_dedup.count()

rules = [
    ("missing_timestamp", "snapshot_ts IS NULL"),
    ("missing_qty", "qty_on_shelf IS NULL"),
    ("negative_qty", "qty_on_shelf < 0"),
    ("unknown_store", f"store_id IS NULL OR store_id NOT IN ({store_list})"),
    ("unknown_product", f"product_id IS NULL OR product_id NOT IN ({product_list})"),
]
flagged = apply_rules(inv_dedup, rules)
good = flagged.filter("_reject_reason IS NULL")
bad = flagged.filter("_reject_reason IS NOT NULL")
save_quarantine(bad, "inventory")

silver_inv = (
    good
    .withColumn("hour_ts", F.expr("snapshot_ts - INTERVAL 1 HOUR"))
    .withColumn("snapshot_date", F.to_date("hour_ts"))
    .withColumn("is_stockout", F.col("qty_on_shelf") == 0)
    .select("store_id", "product_id", "snapshot_ts", "hour_ts", "snapshot_date",
            "qty_on_shelf", "is_stockout", "_source_file", "_ingested_at")
)
upsert(silver_inv, "inventory", ["store_id", "product_id", "snapshot_ts"], partition_by="snapshot_date")

log_run("inventory", n_in, n_dups, bad.count(), silver_count("inventory"))
print("inventory  in:", n_in, "| duplicates removed:", n_dups, "| rejected:", bad.count(), "| silver rows:", silver_count("inventory"))

# COMMAND ----------

bronze_weather = read_bronze("weather")
n_in = bronze_weather.count()

w_dedup = drop_duplicates_keep_first(bronze_weather, ["city", "weather_ts"])
n_dups = n_in - w_dedup.count()

RAIN_THRESHOLD_MM = 0.5
HOT_THRESHOLD_C = 30

rules = [
    ("missing_timestamp", "weather_ts IS NULL"),
    ("missing_city", "city IS NULL"),
    ("temperature_out_of_range", "temperature_c IS NULL OR temperature_c < -10 OR temperature_c > 55"),
    ("rain_out_of_range", "precipitation_mm IS NULL OR precipitation_mm < 0 OR precipitation_mm > 200"),
]
flagged = apply_rules(w_dedup, rules)
good = flagged.filter("_reject_reason IS NULL")
bad = flagged.filter("_reject_reason IS NOT NULL")
save_quarantine(bad, "weather")

silver_weather = good.select(
    "city", "weather_ts", "temperature_c", "precipitation_mm",
    (F.col("precipitation_mm") > RAIN_THRESHOLD_MM).alias("is_rainy"),
    (F.col("temperature_c") >= HOT_THRESHOLD_C).alias("is_hot"),
    F.to_date("weather_ts").alias("weather_date"),
    "_source_file", "_ingested_at",
)
upsert(silver_weather, "weather", ["city", "weather_ts"], partition_by="weather_date")

log_run("weather", n_in, n_dups, bad.count(), silver_count("weather"))
print("weather  in:", n_in, "| duplicates removed:", n_dups, "| rejected:", bad.count(), "| silver rows:", silver_count("weather"))

# COMMAND ----------

print("--- Silver table sizes ---")
for name in ["orders", "inventory", "weather", "stores", "products"]:
    print(f"{name:10} rows = {silver_count(name)}")

print()
print("--- Quarantine sizes ---")
for src in ["orders", "inventory", "weather"]:
    q = spark.read.format("delta").load(f"{SILVER}/_quarantine/{src}")
    print(f"{src:10} rejected = {q.count()}")

print()
o = spark.read.format("delta").load(f"{SILVER}/orders")
i = spark.read.format("delta").load(f"{SILVER}/inventory")
print("Duplicate order_ids left in Silver orders:", o.count() - o.select("order_id").distinct().count())
print("Lowest shelf count left in Silver inventory:", i.agg(F.min("qty_on_shelf")).first()[0])

q_inv = spark.read.format("delta").load(f"{SILVER}/_quarantine/inventory")
q_inv.groupBy("reject_reason").count().show()
q_inv.show(3, truncate=False)

spark.read.format("delta").load(f"{SILVER}/_run_log").orderBy("run_at").show(truncate=False)