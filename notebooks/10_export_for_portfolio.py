# Databricks notebook source
import json
import math
from pyspark.sql import functions as F

STORAGE = "stdarkstore55"
LANDING = f"abfss://landing@{STORAGE}.dfs.core.windows.net"
BRONZE = f"abfss://bronze@{STORAGE}.dfs.core.windows.net"
SILVER = f"abfss://silver@{STORAGE}.dfs.core.windows.net"
GOLD = f"abfss://gold@{STORAGE}.dfs.core.windows.net"
EXPORT = f"{LANDING}/export"


def rd(base, name):
    return spark.read.format("delta").load(f"{base}/{name}")


def clean(x):
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return None
    if isinstance(x, dict):
        return {k: clean(v) for k, v in x.items()}
    if isinstance(x, list):
        return [clean(v) for v in x]
    return x


def save_json(name, payload):
    text = json.dumps(clean(payload), indent=1, default=str)
    dbutils.fs.put(f"{EXPORT}/{name}.json", text, True)
    print(f"saved {name}.json ({len(text):,} characters)")


def rows(df):
    return [r.asDict(recursive=True) for r in df.collect()]


fact = rd(GOLD, "fact_store_product_hour")
print("Fact rows:", fact.count())

# COMMAND ----------

tables5 = ["orders", "inventory", "weather", "stores", "products"]
bronze_rows = {n: rd(BRONZE, n).count() for n in tables5}
silver_rows = {n: rd(SILVER, n).count() for n in tables5}
quarantine_rows = {n: rd(SILVER, f"_quarantine/{n}").count() for n in ["orders", "inventory", "weather"]}
gold_rows = {
    "dim_date": rd(GOLD, "dim_date").count(),
    "dim_store": rd(GOLD, "dim_store").count(),
    "dim_product": rd(GOLD, "dim_product").count(),
    "fact_store_product_hour": fact.count(),
    "mart_stockout_spells": rd(GOLD, "mart_stockout_spells").count(),
    "mart_lost_sales_hourly_v2": rd(GOLD, "mart_lost_sales_hourly_v2").count(),
}

last_run = {r.table_name: r for r in rd(SILVER, "_run_log").orderBy("run_at").collect()}
duplicates_removed = {n: last_run[n].duplicates_removed for n in tables5}

reasons = []
for n in ["orders", "inventory", "weather"]:
    reasons += rows(rd(SILVER, f"_quarantine/{n}").groupBy("source_table", "reject_reason").count())

totals = fact.agg(
    F.sum("units_sold").alias("units_sold"),
    F.sum("revenue_inr").alias("revenue_inr"),
    F.avg(F.when(~F.col("inventory_reading_missing"), F.col("is_stockout").cast("int"))).alias("stockout_share"),
).first().asDict()

save_json("pipeline_overview", {
    "bronze_rows": bronze_rows, "silver_rows": silver_rows, "gold_rows": gold_rows,
    "quarantine_rows": quarantine_rows, "duplicates_removed": duplicates_removed,
    "quarantine_reasons": reasons, "totals": totals,
})
print(bronze_rows, silver_rows, quarantine_rows, duplicates_removed, totals)

# COMMAND ----------

daily = (
    rd(SILVER, "orders")
    .groupBy(F.col("order_date").cast("string").alias("date"))
    .agg(F.count("*").alias("orders"), F.sum("qty").alias("units"), F.sum("revenue_inr").alias("revenue_inr"))
    .orderBy("date")
)
save_json("daily_orders", rows(daily))
save_json("demand_shape", rows(rd(GOLD, "mart_demand_shape").orderBy("is_weekend", "hour_of_day")))
save_json("stores", rows(rd(GOLD, "dim_store").orderBy("store_id")))
save_json("products", rows(rd(GOLD, "dim_product").orderBy("product_id")))

# COMMAND ----------

effects = json.loads(dbutils.fs.head(f"{LANDING}/truth/weather_effects.json"))
used = {r.product_id: r for r in rd(GOLD, "mart_weather_multipliers").collect()}

out = []
for p in rd(GOLD, "dim_product").orderBy("product_id").collect():
    m = used[p.product_id]
    out.append({
        "product_id": p.product_id,
        "product_name": p.product_name,
        "model_rain": round(m.rain_mult, 3),
        "true_rain": float(effects["rain_boost_by_product"].get(p.product_id, effects["default_rain_boost"])),
        "model_heat": round(m.heat_mult, 3),
        "true_heat": float(effects["heat_boost_by_product"].get(p.product_id, 1.0)),
        "rain_units": m.rain_units,
        "heat_units": m.heat_units,
    })
save_json("weather_multipliers", out)

# COMMAND ----------

spells = rd(GOLD, "mart_stockout_spells")
summary = spells.agg(
    F.count("*").alias("spells"),
    F.avg("empty_hours").alias("avg_empty_hours"),
    F.max("empty_hours").alias("longest_spell"),
    F.sum("empty_hours").alias("total_empty_hours"),
).first().asDict()
by_hour = rows(spells.groupBy("first_empty_hour_of_day").count().orderBy("first_empty_hour_of_day"))
save_json("stockout_spells", {"summary": summary, "by_start_hour": by_hour})
print(summary)

# COMMAND ----------

keys = ["store_id", "product_id", "hour_ts"]
truth = (
    spark.read.parquet(f"{LANDING}/truth/lost_sales_truth.parquet")
    .select(*keys, F.col("lost_units").alias("true_units"))
)
v1 = rd(GOLD, "mart_lost_sales_hourly").select(*keys, F.col("est_lost_units").alias("est_v1"))
v2 = rd(GOLD, "mart_lost_sales_hourly_v2").select(*keys, F.col("est_lost_units").alias("est_v2"), "is_rainy", "is_hot", "hour_of_day")

cmp = (
    fact.filter("is_stockout").select(*keys)
    .join(truth, keys, "left").join(v1, keys, "left").join(v2, keys, "left")
    .fillna({"true_units": 0, "est_v1": 0.0, "est_v2": 0.0})
)

slices = {"all stockout hours": F.lit(True), "rainy": F.col("is_rainy"), "dry": ~F.col("is_rainy"),
          "hot": F.col("is_hot"), "not hot": ~F.col("is_hot")}
slice_rows = []
for label, cond in slices.items():
    r = cmp.filter(cond).agg(F.sum("true_units").alias("t"), F.sum("est_v1").alias("v1"), F.sum("est_v2").alias("v2")).first()
    slice_rows.append({"slice": label, "true_units": r.t, "est_before_weather": r.v1, "est_after_weather": r.v2})

by_hour = rows(
    cmp.groupBy("hour_of_day")
    .agg(F.sum("true_units").alias("true_units"), F.sum("est_v2").alias("est_units"))
    .orderBy("hour_of_day")
)
save_json("lost_sales_validation", {"slices": slice_rows, "by_hour": by_hour})
for s in slice_rows:
    print(s)

# COMMAND ----------

stores_d = rd(GOLD, "dim_store").select("store_id", "city", "area")
prods_d = rd(GOLD, "dim_product").select("product_id", "product_name")
agg = (
    rd(GOLD, "mart_lost_sales_hourly_v2")
    .groupBy("store_id", "product_id")
    .agg(F.round(F.sum("est_lost_units"), 0).alias("est_lost_units"),
         F.round(F.sum("est_lost_revenue_inr"), 0).alias("est_lost_revenue_inr"))
    .join(stores_d, "store_id").join(prods_d, "product_id")
)
total = agg.agg(F.sum("est_lost_revenue_inr")).first()[0]
save_json("lost_sales_by_store_product", {
    "total_est_lost_revenue_inr": total,
    "top_15": rows(agg.orderBy(F.col("est_lost_revenue_inr").desc()).limit(15)),
    "by_city": rows(agg.groupBy("city").agg(F.sum("est_lost_revenue_inr").alias("est_lost_revenue_inr")).orderBy(F.col("est_lost_revenue_inr").desc())),
    "by_product": rows(agg.groupBy("product_name").agg(F.sum("est_lost_revenue_inr").alias("est_lost_revenue_inr")).orderBy(F.col("est_lost_revenue_inr").desc())),
})
print("Total estimated lost revenue: Rs", f"{total:,.0f}")

# COMMAND ----------

pick = (
    fact.filter("is_stockout")
    .groupBy("store_id", "product_id", "date_key").agg(F.count("*").alias("h"))
    .filter("h BETWEEN 6 AND 10").orderBy("date_key", "store_id", "product_id").first()
)
day = rows(
    fact.filter((F.col("store_id") == pick.store_id) & (F.col("product_id") == pick.product_id) & (F.col("date_key") == pick.date_key))
    .orderBy("hour_ts")
    .select("hour_of_day", "units_sold", "end_of_hour_qty", "is_stockout", "is_rainy", "is_hot")
)
names = rd(GOLD, "dim_product").filter(F.col("product_id") == pick.product_id).first().product_name
city = rd(GOLD, "dim_store").filter(F.col("store_id") == pick.store_id).first()
save_json("shelf_example_day", {
    "store_id": pick.store_id, "city": city.city, "area": city.area,
    "product_id": pick.product_id, "product_name": names, "date_key": pick.date_key, "hours": day,
})
print(pick.store_id, pick.product_id, pick.date_key, len(day), "hours")

# COMMAND ----------

hist = rd(GOLD, "ops_dq_results")
runs = rows(
    hist.groupBy("run_id")
    .agg(F.min("run_at").alias("run_at_utc"), F.count("*").alias("checks"),
         F.sum((F.col("status") == "FAIL").cast("int")).alias("failed"))
    .withColumn("run_at_ist", F.date_format(F.from_utc_timestamp("run_at_utc", "Asia/Kolkata"), "yyyy-MM-dd HH:mm"))
    .drop("run_at_utc").orderBy("run_at_ist")
)
catalog = rows(hist.select("layer", "table_name", "check_name", "severity").distinct().orderBy("layer", "table_name", "check_name"))
failures = rows(
    hist.filter("status = 'FAIL'")
    .select("run_id", "layer", "table_name", "check_name", "severity", F.col("value").cast("string").alias("value"), "expected")
)
save_json("dq_history", {"runs": runs, "check_catalog": catalog, "failures": failures})
print(len(runs), "runs |", len(catalog), "distinct checks |", len(failures), "failed checks")

# COMMAND ----------

# Copied from the output of notebook 05, Cell 7 (backtest 22-29 Sep). Compare with your screenshot.
restock_days = [
    ("2026-09-22", 65, 14, 15, 62, 82.3, 81.2, 1.82),
    ("2026-09-23", 62, 15, 16, 64, 80.5, 79.5, 1.61),
    ("2026-09-24", 67, 18, 8, 57, 78.8, 89.3, 2.01),
    ("2026-09-25", 64, 16, 17, 53, 80.0, 79.0, 1.91),
    ("2026-09-26", 120, 6, 13, 13, 95.2, 90.2, 1.09),
    ("2026-09-27", 110, 8, 8, 19, 93.2, 93.2, 1.27),
    ("2026-09-28", 68, 14, 9, 60, 82.9, 88.3, 1.34),
    ("2026-09-29", 68, 12, 16, 60, 85.0, 81.0, 1.85),
]
days = [
    {"day": d, "hit": a, "false_alarm": b, "miss": c, "correct_ok": e,
     "precision_pct": p, "recall_pct": r, "avg_timing_error_hours": t}
    for (d, a, b, c, e, p, r, t) in restock_days
]
tp = sum(x["hit"] for x in days); fp = sum(x["false_alarm"] for x in days); fn = sum(x["miss"] for x in days)
save_json("restock_backtest", {
    "source": "notebook 05 Cell 7 output, in-sample backtest with actual weather as the forecast",
    "days": days,
    "all_days": {"precision_pct": round(100 * tp / (tp + fp), 1), "recall_pct": round(100 * tp / (tp + fn), 1)},
})
print(round(100 * tp / (tp + fp), 1), round(100 * tp / (tp + fn), 1))

# COMMAND ----------

hist_orders = spark.sql(f"DESCRIBE HISTORY delta.`{SILVER}/orders`")
hist_orders.select("version", "timestamp", "operation", "operationMetrics").orderBy(F.col("version").desc()).show(8, truncate=False)

for f in dbutils.fs.ls(EXPORT):
    print(f.name, f.size)

# COMMAND ----------

# 1) the restock priority list for 29 Sep (what a store team would act on)
bt = rd(GOLD, "mart_restock_priority_backtest")
top = (
    bt.filter("predicted_run_out")
    .orderBy(F.col("revenue_at_risk_inr").desc()).limit(15)
    .select("store_id", "city", "area", "product_name", "stock_now",
            F.round("pred_hours_until_empty", 1).alias("pred_hours_until_empty"),
            F.round("actual_hours_until_empty", 1).alias("actual_hours_until_empty"),
            "actual_run_out",
            F.round("revenue_at_risk_inr", 0).alias("revenue_at_risk_inr"))
)
counts = bt.agg(
    F.count("*").alias("store_products_scored"),
    F.sum(F.col("predicted_run_out").cast("int")).alias("predicted_run_out"),
    F.sum(F.col("actual_run_out").cast("int")).alias("actually_ran_out"),
).first().asDict()
save_json("restock_priority_example", {
    "as_of": "2026-09-29 16:00, forecast until the 06:00 restock",
    "counts": counts, "top_15": rows(top)})

# 2) examples of rejected rows
q = {}
for n in ["orders", "inventory", "weather"]:
    qd = rd(SILVER, f"_quarantine/{n}")
    q[n] = {"rejected_rows": qd.count(), "sample": rows(qd.limit(3))}
save_json("quarantine_samples", q)

# 3) inventory of every table
inv = []
layers = [
    (BRONZE, "bronze", ["orders", "inventory", "weather", "stores", "products"]),
    (SILVER, "silver", ["orders", "inventory", "weather", "stores", "products", "_run_log"]),
    (GOLD, "gold", ["dim_date", "dim_store", "dim_product", "fact_store_product_hour",
                    "mart_stockout_spells", "mart_demand_level", "mart_demand_shape",
                    "mart_weather_multipliers", "mart_lost_sales_hourly",
                    "mart_lost_sales_hourly_v2", "mart_lost_sales_by_store_product",
                    "mart_restock_priority_backtest", "ops_dq_results"]),
]
for base, layer, names in layers:
    for n in names:
        try:
            d = spark.sql(f"DESCRIBE DETAIL delta.`{base}/{n}`").first()
            inv.append({"layer": layer, "table": n, "rows": rd(base, n).count(),
                        "files": d.numFiles, "size_kb": round(d.sizeInBytes / 1024)})
        except Exception as e:
            print("skipped", layer, n, str(e)[:80])
save_json("table_inventory", inv)
print(len(inv), "tables listed")