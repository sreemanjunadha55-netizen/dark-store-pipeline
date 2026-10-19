# Databricks notebook source
import uuid
from pyspark.sql import functions as F

STORAGE = "stdarkstore55"
LANDING = f"abfss://landing@{STORAGE}.dfs.core.windows.net"
BRONZE = f"abfss://bronze@{STORAGE}.dfs.core.windows.net"
SILVER = f"abfss://silver@{STORAGE}.dfs.core.windows.net"
GOLD = f"abfss://gold@{STORAGE}.dfs.core.windows.net"


def rd(base, name):
    return spark.read.format("delta").load(f"{base}/{name}")


RUN_ID = uuid.uuid4().hex[:8]
results = []


def record(layer, table, check, severity, ok, value, expected):
    status = "PASS" if ok else "FAIL"
    results.append((RUN_ID, layer, table, check, severity, status,
                    None if value is None else float(value), str(expected), ""))
    print(f"{status} [{severity}] {layer}.{table}: {check} -> {value} (expected {expected})")


def check_equals(layer, table, check, actual, expected, severity="error"):
    record(layer, table, check, severity, actual == expected, actual, f"= {expected}")


def check_zero(layer, table, check, bad_rows, severity="error"):
    record(layer, table, check, severity, bad_rows == 0, bad_rows, "= 0")


def check_between(layer, table, check, value, low, high, severity="warn"):
    ok = value is not None and low <= value <= high
    record(layer, table, check, severity, ok, value, f"{low} to {high}")


def check_close(layer, table, check, actual, expected, tol=0.5, severity="error"):
    diff = abs(float(actual) - float(expected))
    record(layer, table, check, severity, diff <= tol, diff, f"<= {tol}")


def dup_count(df, keys):
    return df.count() - df.select(*keys).distinct().count()


def null_count(df, cols):
    condition = " OR ".join([f"{c} IS NULL" for c in cols])
    return df.filter(condition).count()


def bad_count(df, bad_condition):
    return df.filter(bad_condition).count()


def orphan_count(df, key, ref_df, ref_key):
    refs = ref_df.select(F.col(ref_key).alias(key)).distinct()
    return df.filter(F.col(key).isNotNull()).select(key).distinct().join(refs, key, "left_anti").count()

# COMMAND ----------

b_orders, b_inv, b_weather = rd(BRONZE, "orders"), rd(BRONZE, "inventory"), rd(BRONZE, "weather")
s_orders, s_inv, s_weather = rd(SILVER, "orders"), rd(SILVER, "inventory"), rd(SILVER, "weather")
s_stores, s_products = rd(SILVER, "stores"), rd(SILVER, "products")
q_orders = rd(SILVER, "_quarantine/orders")
q_inv = rd(SILVER, "_quarantine/inventory")
q_weather = rd(SILVER, "_quarantine/weather")

check_equals(
    "reconciliation", "orders", "bronze rows = silver + quarantine + duplicates",
    b_orders.count(), s_orders.count() + q_orders.count() + dup_count(b_orders, ["order_id"]),
)
check_equals(
    "reconciliation", "inventory", "bronze rows = silver + quarantine + duplicates",
    b_inv.count(),
    s_inv.count() + q_inv.count() + dup_count(b_inv, ["store_id", "product_id", "snapshot_ts"]),
)
check_equals(
    "reconciliation", "weather", "bronze rows = silver + quarantine + duplicates",
    b_weather.count(),
    s_weather.count() + q_weather.count() + dup_count(b_weather, ["city", "weather_ts"]),
)

# COMMAND ----------

# --- orders (7) ---
check_zero("silver", "orders", "duplicate order_id", dup_count(s_orders, ["order_id"]))
check_zero("silver", "orders", "null key columns", null_count(s_orders, ["order_id", "store_id", "product_id", "order_ts"]))
check_zero("silver", "orders", "qty <= 0", bad_count(s_orders, "qty <= 0"))
check_zero("silver", "orders", "unit price <= 0", bad_count(s_orders, "unit_price_inr <= 0"))
check_zero("silver", "orders", "order_date matches order_ts", bad_count(s_orders, "order_date != to_date(order_ts)"))
check_zero("silver", "orders", "orphan store_id", orphan_count(s_orders, "store_id", s_stores, "store_id"))
check_zero("silver", "orders", "orphan product_id", orphan_count(s_orders, "product_id", s_products, "product_id"))

# --- inventory (5) ---
check_zero("silver", "inventory", "duplicate store-product-time", dup_count(s_inv, ["store_id", "product_id", "snapshot_ts"]))
check_zero("silver", "inventory", "null key columns", null_count(s_inv, ["store_id", "product_id", "snapshot_ts", "qty_on_shelf"]))
check_zero("silver", "inventory", "negative shelf count", bad_count(s_inv, "qty_on_shelf < 0"))
check_zero("silver", "inventory", "orphan store_id", orphan_count(s_inv, "store_id", s_stores, "store_id"))
check_zero("silver", "inventory", "orphan product_id", orphan_count(s_inv, "product_id", s_products, "product_id"))

# --- weather (4) ---
check_zero("silver", "weather", "duplicate city-hour", dup_count(s_weather, ["city", "weather_ts"]))
check_zero("silver", "weather", "null key columns", null_count(s_weather, ["city", "weather_ts", "temperature_c", "precipitation_mm"]))
check_zero("silver", "weather", "temperature outside -10..55 C", bad_count(s_weather, "temperature_c < -10 OR temperature_c > 55"))
check_zero("silver", "weather", "negative rainfall", bad_count(s_weather, "precipitation_mm < 0"))

# COMMAND ----------

fact = rd(GOLD, "fact_store_product_hour")
dim_store, dim_product, dim_date = rd(GOLD, "dim_store"), rd(GOLD, "dim_product"), rd(GOLD, "dim_date")
v2 = rd(GOLD, "mart_lost_sales_hourly_v2")
mult = rd(GOLD, "mart_weather_multipliers")

n_hours = s_weather.select("weather_ts").distinct().count()
expected_rows = dim_store.count() * dim_product.count() * n_hours

check_equals("gold", "fact_store_product_hour", "row count = stores x products x hours", fact.count(), expected_rows)
check_zero("gold", "fact_store_product_hour", "duplicate store-product-hour", dup_count(fact, ["store_id", "product_id", "hour_ts"]))
check_zero("gold", "fact_store_product_hour", "orphan store_id", orphan_count(fact, "store_id", dim_store, "store_id"))
check_zero("gold", "fact_store_product_hour", "orphan product_id", orphan_count(fact, "product_id", dim_product, "product_id"))
check_zero("gold", "fact_store_product_hour", "orphan date_key", orphan_count(fact, "date_key", dim_date, "date_key"))

check_equals(
    "gold", "fact_store_product_hour", "units sold = silver orders qty",
    fact.agg(F.sum("units_sold")).first()[0], s_orders.agg(F.sum("qty")).first()[0],
)
check_close(
    "gold", "fact_store_product_hour", "revenue = silver orders revenue",
    fact.agg(F.sum("revenue_inr")).first()[0], s_orders.agg(F.sum("revenue_inr")).first()[0],
)
check_equals(
    "gold", "fact_store_product_hour", "missing shelf readings = quarantined readings",
    fact.filter("inventory_reading_missing").count(), q_inv.count(),
)
stockout_share = (
    fact.filter("NOT inventory_reading_missing")
    .agg(F.avg(F.col("is_stockout").cast("int"))).first()[0]
)
check_between("gold", "fact_store_product_hour", "stockout share is plausible", stockout_share, 0.05, 0.40, "warn")

check_equals("gold", "mart_lost_sales_hourly_v2", "one row per stockout hour", v2.count(), fact.filter("is_stockout").count())
check_zero("gold", "mart_lost_sales_hourly_v2", "negative lost units", bad_count(v2, "est_lost_units < 0"))

dim_dups = (
    dup_count(dim_store, ["store_id"]) + dup_count(dim_product, ["product_id"]) + dup_count(dim_date, ["date_key"])
)
check_zero("gold", "dimensions", "duplicate dimension keys", dim_dups)
check_zero(
    "gold", "mart_weather_multipliers", "multipliers outside 0.8..3.0",
    bad_count(mult, "rain_mult < 0.8 OR rain_mult > 3.0 OR heat_mult < 0.8 OR heat_mult > 3.0"),
)

# COMMAND ----------

n_days = s_orders.select("order_date").distinct().count()
span = s_orders.agg(F.datediff(F.max("order_date"), F.min("order_date"))).first()[0]
per_city = s_weather.groupBy("city").count()

check_equals("completeness", "orders", "distinct days with orders", n_days, 30)
check_equals("completeness", "orders", "days between first and last order", span, 29)
check_zero("completeness", "weather", "cities without exactly 720 hourly rows", bad_count(per_city, "count != 720"))

daily = (
    s_orders.groupBy("order_date").agg(F.count("*").alias("n"))
    .withColumn("is_weekend", F.dayofweek("order_date").isin(1, 7))
)
medians = daily.groupBy("is_weekend").agg(F.percentile_approx("n", 0.5).alias("median_n"))
flagged = daily.join(medians, "is_weekend").withColumn("ratio", F.round(F.col("n") / F.col("median_n"), 2))

outliers = flagged.filter("ratio < 0.7 OR ratio > 1.3").count()
check_zero("completeness", "orders", "days with unusual order volume (vs same day-type median)", outliers, "warn")
flagged.orderBy("ratio").select("order_date", "is_weekend", "n", "median_n", "ratio").show(5)
flagged.orderBy(F.col("ratio").desc()).select("order_date", "is_weekend", "n", "median_n", "ratio").show(5)

# COMMAND ----------

keys = ["store_id", "product_id", "hour_ts"]
truth = (
    spark.read.parquet(f"{LANDING}/truth/lost_sales_truth.parquet")
    .select(*keys, F.col("lost_units").alias("true_lost_units"))
)
joined = v2.select(*keys, "est_lost_units").join(truth, keys, "left").fillna({"true_lost_units": 0})
totals = joined.agg(F.sum("est_lost_units").alias("est"), F.sum("true_lost_units").alias("true")).first()
check_between("model", "lost_sales_v2", "estimate / hidden truth (synthetic-only check)", totals.est / totals.true, 0.85, 1.05, "warn")

backtest = rd(GOLD, "mart_restock_priority_backtest")
m = backtest.agg(
    F.sum((F.col("predicted_run_out") & F.col("actual_run_out")).cast("int")).alias("tp"),
    F.sum((F.col("predicted_run_out") & ~F.col("actual_run_out")).cast("int")).alias("fp"),
).first()
check_between("model", "restock_backtest", "precision on 29 Sep", m.tp / (m.tp + m.fp), 0.70, 1.0, "warn")

# COMMAND ----------

schema = ("run_id string, layer string, table_name string, check_name string, severity string, "
          "status string, value double, expected string, detail string")
res = spark.createDataFrame(results, schema).withColumn("run_at", F.current_timestamp())
res.write.format("delta").mode("append").option("mergeSchema", "true").save(f"{GOLD}/ops_dq_results")

(
    res.groupBy("layer")
    .agg(
        F.count("*").alias("checks"),
        F.sum((F.col("status") == "PASS").cast("int")).alias("passed"),
        F.sum((F.col("status") == "FAIL").cast("int")).alias("failed"),
    )
    .orderBy("layer")
    .show()
)

errors = res.filter("status = 'FAIL' AND severity = 'error'").count()
warnings = res.filter("status = 'FAIL' AND severity = 'warn'").count()
print(f"Run {RUN_ID}: {res.count()} checks | error-level failures: {errors} | warnings: {warnings}")
if errors > 0:
    raise Exception(f"{errors} error-level data-quality checks failed in run {RUN_ID}. See gold/ops_dq_results.")
print("All error-level checks passed.")

# COMMAND ----------

good = spark.createDataFrame(s_orders.limit(1000).collect(), s_orders.schema)

dup_row = good.limit(1)
bad_qty = good.limit(1).withColumn("order_id", F.lit("O99999991")).withColumn("qty", F.lit(-5))
null_store = good.limit(1).withColumn("order_id", F.lit("O99999992")).withColumn("store_id", F.lit(None).cast("string"))
unknown_store = good.limit(1).withColumn("order_id", F.lit("O99999993")).withColumn("store_id", F.lit("S999"))
broken = good.unionByName(dup_row).unionByName(bad_qty).unionByName(null_store).unionByName(unknown_store)

detectors = [
    ("duplicate order_id", lambda d: dup_count(d, ["order_id"]), 1),
    ("non-positive qty", lambda d: bad_count(d, "qty IS NULL OR qty <= 0"), 1),
    ("null store_id", lambda d: null_count(d, ["store_id"]), 1),
    ("unknown store_id", lambda d: orphan_count(d, "store_id", s_stores, "store_id"), 1),
]
for name, fn, expected in detectors:
    clean_found = fn(good)
    broken_found = fn(broken)
    verdict = "CAUGHT" if (clean_found == 0 and broken_found == expected) else "CHECK THIS"
    print(f"{name:20} clean sample: {clean_found}   defective sample: {broken_found}   -> {verdict}")

# COMMAND ----------

hist = rd(GOLD, "ops_dq_results")
(
    hist.groupBy("run_id")
    .agg(
        F.min("run_at").alias("run_at"),
        F.count("*").alias("checks"),
        F.sum((F.col("status") == "FAIL").cast("int")).alias("failed"),
    )
    .orderBy("run_at")
    .show(truncate=False)
)
hist.filter("status = 'FAIL'").select("run_id", "layer", "table_name", "check_name", "severity", "value", "expected").show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
hist = spark.read.format("delta").load("abfss://gold@stdarkstore55.dfs.core.windows.net/ops_dq_results")
(
    hist.groupBy("run_id")
    .agg(F.min("run_at").alias("run_at"), F.count("*").alias("checks"),
         F.sum((F.col("status") == "FAIL").cast("int")).alias("failed"))
    .orderBy("run_at").show(truncate=False)
)