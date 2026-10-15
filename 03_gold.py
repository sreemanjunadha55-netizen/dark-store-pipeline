# Databricks notebook source
from pyspark.sql import functions as F

STORAGE = "stdarkstore55"
SILVER = f"abfss://silver@{STORAGE}.dfs.core.windows.net"
GOLD = f"abfss://gold@{STORAGE}.dfs.core.windows.net"


def read_silver(name):
    return spark.read.format("delta").load(f"{SILVER}/{name}")


def write_gold(df, name):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{GOLD}/{name}")
    )
    n = spark.read.format("delta").load(f"{GOLD}/{name}").count()
    print(f"{name}: {n} rows written")


orders_check = read_silver("orders")
inventory_check = read_silver("inventory")
print("Duplicate order_ids in Silver orders:", orders_check.count() - orders_check.select("order_id").distinct().count())
print("Lowest shelf count in Silver inventory:", inventory_check.agg(F.min("qty_on_shelf")).first()[0])

# COMMAND ----------

dim_date = (
    spark.sql("SELECT explode(sequence(to_date('2026-09-01'), to_date('2026-10-01'), interval 1 day)) AS date")
    .select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        F.weekofyear("date").alias("week_of_year"),
    )
)
write_gold(dim_date, "dim_date")

dim_store = read_silver("stores").select("store_id", "city", "area", "latitude", "longitude")
write_gold(dim_store, "dim_store")

dim_product = read_silver("products").select("product_id", "product_name", "category", "price_inr")
write_gold(dim_product, "dim_product")

# COMMAND ----------

stores = read_silver("stores")
products = read_silver("products")
weather = read_silver("weather")

hours = weather.select(F.col("weather_ts").alias("hour_ts")).distinct()

grid = (
    stores.select("store_id", "city")
    .crossJoin(products.select("product_id"))
    .crossJoin(hours)
)

sales = (
    read_silver("orders")
    .groupBy("store_id", "product_id", F.col("order_hour").alias("hour_ts"))
    .agg(
        F.sum("qty").alias("units_sold"),
        F.count("*").alias("order_lines"),
        F.sum("revenue_inr").alias("revenue_inr"),
    )
)

shelf = read_silver("inventory").select(
    "store_id", "product_id", "hour_ts",
    F.col("qty_on_shelf").alias("end_of_hour_qty"),
)

wx = weather.select(
    "city", F.col("weather_ts").alias("hour_ts"),
    "temperature_c", "precipitation_mm", "is_rainy", "is_hot",
)

fact = (
    grid
    .join(sales, ["store_id", "product_id", "hour_ts"], "left")
    .join(shelf, ["store_id", "product_id", "hour_ts"], "left")
    .join(wx, ["city", "hour_ts"], "left")
    .withColumn("units_sold", F.coalesce("units_sold", F.lit(0)))
    .withColumn("order_lines", F.coalesce("order_lines", F.lit(0)))
    .withColumn("revenue_inr", F.coalesce("revenue_inr", F.lit(0)))
    .withColumn("date_key", F.date_format("hour_ts", "yyyyMMdd").cast("int"))
    .withColumn("hour_of_day", F.hour("hour_ts"))
    .withColumn("is_weekend", F.dayofweek("hour_ts").isin(1, 7))
    .withColumn("inventory_reading_missing", F.col("end_of_hour_qty").isNull())
    .withColumn("is_stockout", F.col("end_of_hour_qty") == 0)
    .select(
        "store_id", "product_id", "hour_ts", "date_key", "hour_of_day", "is_weekend",
        "units_sold", "order_lines", "revenue_inr",
        "end_of_hour_qty", "is_stockout", "inventory_reading_missing",
        "temperature_c", "precipitation_mm", "is_rainy", "is_hot",
    )
)
write_gold(fact, "fact_store_product_hour")

# COMMAND ----------

fact = spark.read.format("delta").load(f"{GOLD}/fact_store_product_hour")

print("Total rows:", fact.count())
print("Distinct store-product-hour combinations:", fact.select("store_id", "product_id", "hour_ts").distinct().count())
print("Units sold in fact table:", fact.agg(F.sum("units_sold")).first()[0])
print("Units sold in Silver orders:", read_silver("orders").agg(F.sum("qty")).first()[0])
print("Hours with a missing shelf reading:", fact.filter("inventory_reading_missing").count())
print("Rows with no weather:", fact.filter("temperature_c IS NULL").count())
share = fact.filter("NOT inventory_reading_missing").agg(F.avg(F.col("is_stockout").cast("int"))).first()[0]
print(f"Stockout share (rows that have a reading): {share:.1%}")

# COMMAND ----------

worst = (
    fact.filter("is_stockout")
    .groupBy("store_id", "product_id", "date_key")
    .agg(F.count("*").alias("stockout_hours"))
    .filter("stockout_hours BETWEEN 6 AND 10")
    .orderBy("date_key", "store_id", "product_id")
    .first()
)
print("Showing:", worst.store_id, worst.product_id, worst.date_key)

(
    fact.filter(
        (F.col("store_id") == worst.store_id)
        & (F.col("product_id") == worst.product_id)
        & (F.col("date_key") == worst.date_key)
    )
    .orderBy("hour_ts")
    .select("hour_of_day", "units_sold", "end_of_hour_qty", "is_stockout", "is_rainy", "is_hot")
    .show(24, truncate=False)
)