# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

STORAGE = "stdarkstore55"
LANDING = f"abfss://landing@{STORAGE}.dfs.core.windows.net"
GOLD = f"abfss://gold@{STORAGE}.dfs.core.windows.net"


def read_gold(name):
    return spark.read.format("delta").load(f"{GOLD}/{name}")


def write_gold(df, name):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{GOLD}/{name}")
    )
    print(f"{name}: {read_gold(name).count()} rows written")


fact = read_gold("fact_store_product_hour")
print("Fact rows:", fact.count())

# COMMAND ----------

w_all = Window.partitionBy("store_id", "product_id").orderBy("hour_ts")
indexed = fact.withColumn("hour_idx", F.row_number().over(w_all))

stockout_rows = indexed.filter("is_stockout")

w_so = Window.partitionBy("store_id", "product_id").orderBy("hour_ts")
grouped = (
    stockout_rows
    .withColumn("rn", F.row_number().over(w_so))
    .withColumn("spell_key", F.col("hour_idx") - F.col("rn"))
)

spells = (
    grouped.groupBy("store_id", "product_id", "spell_key")
    .agg(
        F.min("hour_ts").alias("first_empty_hour"),
        F.max("hour_ts").alias("last_empty_hour"),
        F.count("*").alias("empty_hours"),
    )
    .withColumn("first_empty_hour_of_day", F.hour("first_empty_hour"))
    .drop("spell_key")
)
write_gold(spells, "mart_stockout_spells")

spells.agg(
    F.count("*").alias("spells"),
    F.round(F.avg("empty_hours"), 1).alias("avg_empty_hours"),
    F.max("empty_hours").alias("longest_spell"),
).show()
spells.groupBy("first_empty_hour_of_day").count().orderBy("first_empty_hour_of_day").show(24)

# COMMAND ----------

pair_hours = fact.filter("NOT inventory_reading_missing")

stock_profile = (
    pair_hours.filter("hour_of_day BETWEEN 6 AND 21")
    .groupBy("store_id", "product_id")
    .agg(F.avg(F.col("is_stockout").cast("int")).alias("early_stockout_share"))
)
well_stocked = stock_profile.filter("early_stockout_share < 0.03").select("store_id", "product_id")
print("Well-stocked store-products:", well_stocked.count(), "of", stock_profile.count())

level = (
    pair_hours.filter("hour_of_day BETWEEN 8 AND 16 AND end_of_hour_qty > 0")
    .groupBy("store_id", "product_id")
    .agg(F.avg("units_sold").alias("level"))
    .filter("level > 0")
)

shape = (
    pair_hours.filter("end_of_hour_qty > 0")
    .join(well_stocked, ["store_id", "product_id"], "inner")
    .join(level, ["store_id", "product_id"], "inner")
    .groupBy("hour_of_day", "is_weekend")
    .agg(
        F.try_divide(F.sum("units_sold"), F.sum("level")).alias("shape"),
        F.count("*").alias("rows_used"),
    )
)

write_gold(level, "mart_demand_level")
write_gold(shape, "mart_demand_shape")
shape.filter("NOT is_weekend").orderBy("hour_of_day").show(24)
shape.filter("is_weekend").orderBy("hour_of_day").show(24)

# COMMAND ----------

price = read_gold("dim_product").select("product_id", "price_inr")

lost = (
    fact.filter("is_stockout")
    .join(level, ["store_id", "product_id"], "left")
    .join(shape.select("hour_of_day", "is_weekend", "shape"), ["hour_of_day", "is_weekend"], "left")
    .join(price, "product_id", "left")
    .withColumn("expected_demand", F.col("level") * F.col("shape"))
    .withColumn("has_estimate", F.col("expected_demand").isNotNull())
    .withColumn(
        "est_lost_units",
        F.when(
            F.col("has_estimate"),
            F.greatest(F.col("expected_demand") - F.col("units_sold"), F.lit(0.0)),
        ).otherwise(F.lit(0.0)),
    )
    .withColumn("est_lost_revenue_inr", F.col("est_lost_units") * F.col("price_inr"))
    .select(
        "store_id", "product_id", "hour_ts", "date_key", "hour_of_day", "is_weekend",
        "is_rainy", "is_hot", "units_sold", "expected_demand", "has_estimate",
        "est_lost_units", "est_lost_revenue_inr",
    )
)
write_gold(lost, "mart_lost_sales_hourly")
print("Stockout rows without an estimate:", lost.filter("NOT has_estimate").count())

# COMMAND ----------

keys = ["store_id", "product_id", "hour_ts"]

truth = (
    spark.read.parquet(f"{LANDING}/truth/lost_sales_truth.parquet")
    .select(*keys, F.col("lost_units").alias("true_lost_units"))
)
est = read_gold("mart_lost_sales_hourly").select(*keys, "est_lost_units")

compare = (
    fact.filter("NOT inventory_reading_missing")
    .select(*keys, "hour_of_day", "is_rainy", "is_hot", "is_stockout")
    .join(truth, keys, "left")
    .join(est, keys, "left")
    .fillna({"true_lost_units": 0, "est_lost_units": 0.0})
)

tot = compare.agg(
    F.sum("true_lost_units").alias("true_units"),
    F.sum("est_lost_units").alias("est_units"),
).first()
print(f"True lost units (hours with a reading): {tot.true_units}")
print(f"Estimated lost units:                   {tot.est_units:.0f}")
print(f"Estimate / truth:                       {tot.est_units / tot.true_units:.1%}")

outside = compare.filter("true_lost_units > 0 AND NOT is_stockout").agg(
    F.coalesce(F.sum("true_lost_units"), F.lit(0))
).first()[0]
print("True lost units outside flagged stockout hours (should be 0):", outside)

hidden = (
    truth.join(fact.filter("inventory_reading_missing").select(*keys), keys, "inner")
    .agg(F.coalesce(F.sum("true_lost_units"), F.lit(0)))
    .first()[0]
)
print("True lost units in hours with a missing reading (unobservable):", hidden)

pairs = compare.groupBy("store_id", "product_id").agg(
    F.sum("true_lost_units").alias("t"), F.sum("est_lost_units").alias("e")
)
print("Correlation across store-products:", round(pairs.stat.corr("t", "e"), 3))

print("\nBy hour of day:")
(
    compare.groupBy("hour_of_day")
    .agg(
        F.sum("true_lost_units").alias("true_units"),
        F.round(F.sum("est_lost_units"), 0).alias("est_units"),
    )
    .withColumn("est_vs_true", F.round(F.try_divide(F.col("est_units"), F.col("true_units")), 2))
    .orderBy("hour_of_day")
    .show(24)
)

print("Stockout hours by weather:")
for flag in ["is_rainy", "is_hot"]:
    (
        compare.filter("is_stockout")
        .groupBy(flag)
        .agg(
            F.sum("true_lost_units").alias("true_units"),
            F.round(F.sum("est_lost_units"), 0).alias("est_units"),
        )
        .withColumn("est_vs_true", F.round(F.try_divide(F.col("est_units"), F.col("true_units")), 2))
        .show()
    )

# COMMAND ----------

dim_store = read_gold("dim_store").select("store_id", "city", "area")
dim_prod = read_gold("dim_product").select("product_id", "product_name")

by_pair = (
    read_gold("mart_lost_sales_hourly")
    .groupBy("store_id", "product_id")
    .agg(
        F.round(F.sum("est_lost_units"), 0).alias("est_lost_units"),
        F.round(F.sum("est_lost_revenue_inr"), 0).alias("est_lost_revenue_inr"),
    )
    .join(dim_store, "store_id")
    .join(dim_prod, "product_id")
    .select("store_id", "city", "area", "product_id", "product_name", "est_lost_units", "est_lost_revenue_inr")
)
write_gold(by_pair, "mart_lost_sales_by_store_product")

by_pair.orderBy(F.col("est_lost_revenue_inr").desc()).show(10, truncate=False)
total = by_pair.agg(F.sum("est_lost_revenue_inr")).first()[0]
print(f"Estimated revenue lost to stockouts in September (10 stores): Rs {total:,.0f}")