# Databricks notebook source
import json
from datetime import date, timedelta
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
level = read_gold("mart_demand_level")
shape = read_gold("mart_demand_shape").select("hour_of_day", "is_weekend", "shape")
price = read_gold("dim_product").select("product_id", "price_inr")
print("Fact rows:", fact.count(), "| level rows:", level.count(), "| shape rows:", shape.count())

# COMMAND ----------

MIN_UNITS = 100

base = (
    fact.filter("NOT inventory_reading_missing")
    .join(level, ["store_id", "product_id"], "inner")
    .join(shape, ["hour_of_day", "is_weekend"], "inner")
    .withColumn("expected_base", F.col("level") * F.col("shape"))
    .withColumn("start_qty", F.col("end_of_hour_qty") + F.col("units_sold"))
    .withColumn("uncensored", F.col("start_qty") >= F.greatest(F.lit(3.0), 3 * F.col("expected_base")))
    .withColumn(
        "regime",
        F.when(F.col("is_rainy") & ~F.col("is_hot"), "rain")
        .when(F.col("is_hot") & ~F.col("is_rainy"), "heat")
        .when(~F.col("is_rainy") & ~F.col("is_hot"), "dry")
        .otherwise("both"),
    )
)
print("Hours usable for learning:", base.filter("uncensored AND regime != 'both'").count(), "of", base.count())

per = (
    base.filter("uncensored AND regime != 'both'")
    .groupBy("product_id", "regime")
    .agg(F.sum("units_sold").alias("u"), F.sum("expected_base").alias("e"))
)
wide = (
    per.groupBy("product_id")
    .pivot("regime", ["dry", "rain", "heat"])
    .agg(F.first("u").alias("u"), F.first("e").alias("e"))
)
est = wide.select(
    "product_id",
    F.col("rain_u").alias("rain_units"),
    F.col("heat_u").alias("heat_units"),
    F.try_divide(
        F.try_divide(F.col("rain_u"), F.col("rain_e")),
        F.try_divide(F.col("dry_u"), F.col("dry_e")),
    ).alias("est_rain_mult"),
    F.try_divide(
        F.try_divide(F.col("heat_u"), F.col("heat_e")),
        F.try_divide(F.col("dry_u"), F.col("dry_e")),
    ).alias("est_heat_mult"),
)

tot = {
    r.regime: (r.u, r.e)
    for r in per.groupBy("regime").agg(F.sum("u").alias("u"), F.sum("e").alias("e")).collect()
}
dry_ratio = tot["dry"][0] / tot["dry"][1]
pooled_rain = (tot["rain"][0] / tot["rain"][1]) / dry_ratio
pooled_heat = (tot["heat"][0] / tot["heat"][1]) / dry_ratio
print(f"Pooled rain multiplier (all products): {pooled_rain:.2f}")
print(f"Pooled heat multiplier (all products): {pooled_heat:.2f}")

# COMMAND ----------

effects = json.loads(dbutils.fs.head(f"{LANDING}/truth/weather_effects.json"))
names = read_gold("dim_product").select("product_id", "product_name")

truth_rows = [
    (
        r.product_id,
        float(effects["rain_boost_by_product"].get(r.product_id, effects["default_rain_boost"])),
        float(effects["heat_boost_by_product"].get(r.product_id, 1.0)),
    )
    for r in names.collect()
]
truth_mult = spark.createDataFrame(truth_rows, "product_id string, true_rain double, true_heat double")

(
    names.join(est, "product_id", "left")
    .join(truth_mult, "product_id", "left")
    .select(
        "product_id", "product_name",
        F.round("est_rain_mult", 2).alias("est_rain"), "true_rain", "rain_units",
        F.round("est_heat_mult", 2).alias("est_heat"), "true_heat", "heat_units",
    )
    .orderBy("product_id")
    .show(16, truncate=False)
)

# COMMAND ----------

final = (
    read_gold("dim_product").select("product_id")
    .join(est, "product_id", "left")
    .withColumn(
        "rain_mult_raw",
        F.when(F.col("rain_units") >= MIN_UNITS, F.col("est_rain_mult")).otherwise(F.lit(pooled_rain)),
    )
    .withColumn(
        "heat_mult_raw",
        F.when(F.col("heat_units") >= MIN_UNITS, F.col("est_heat_mult")).otherwise(F.lit(1.0)),
    )
    .withColumn("rain_mult", F.least(F.greatest(F.coalesce("rain_mult_raw", F.lit(pooled_rain)), F.lit(0.8)), F.lit(3.0)))
    .withColumn("heat_mult", F.least(F.greatest(F.coalesce("heat_mult_raw", F.lit(1.0)), F.lit(0.8)), F.lit(3.0)))
    .select("product_id", "rain_mult", "heat_mult", "rain_units", "heat_units")
)
write_gold(final, "mart_weather_multipliers")
final.orderBy("product_id").show(16)

# COMMAND ----------

mult = read_gold("mart_weather_multipliers")

lost2 = (
    fact.filter("is_stockout")
    .join(level, ["store_id", "product_id"], "left")
    .join(shape, ["hour_of_day", "is_weekend"], "left")
    .join(mult, "product_id", "left")
    .join(price, "product_id", "left")
    .withColumn(
        "expected_demand",
        F.col("level") * F.col("shape")
        * F.when(F.col("is_rainy"), F.col("rain_mult")).otherwise(F.lit(1.0))
        * F.when(F.col("is_hot"), F.col("heat_mult")).otherwise(F.lit(1.0)),
    )
    .withColumn(
        "est_lost_units",
        F.when(
            F.col("expected_demand").isNotNull(),
            F.greatest(F.col("expected_demand") - F.col("units_sold"), F.lit(0.0)),
        ).otherwise(F.lit(0.0)),
    )
    .withColumn("est_lost_revenue_inr", F.col("est_lost_units") * F.col("price_inr"))
    .select(
        "store_id", "product_id", "hour_ts", "date_key", "hour_of_day", "is_weekend",
        "is_rainy", "is_hot", "units_sold", "expected_demand", "est_lost_units", "est_lost_revenue_inr",
    )
)
write_gold(lost2, "mart_lost_sales_hourly_v2")

keys = ["store_id", "product_id", "hour_ts"]
truth = (
    spark.read.parquet(f"{LANDING}/truth/lost_sales_truth.parquet")
    .select(*keys, F.col("lost_units").alias("true_lost_units"))
)
v1 = read_gold("mart_lost_sales_hourly").select(*keys, F.col("est_lost_units").alias("est_v1"))
v2 = read_gold("mart_lost_sales_hourly_v2").select(*keys, F.col("est_lost_units").alias("est_v2"))

cmp = (
    fact.filter("is_stockout")
    .select(*keys, "is_rainy", "is_hot")
    .join(truth, keys, "left")
    .join(v1, keys, "left")
    .join(v2, keys, "left")
    .fillna({"true_lost_units": 0, "est_v1": 0.0, "est_v2": 0.0})
)

print("Estimate as a share of the truth (1.0 = perfect):")
slices = [
    ("all stockout hours", "true"),
    ("rainy hours", "is_rainy"),
    ("dry hours", "NOT is_rainy"),
    ("hot hours", "is_hot"),
    ("not-hot hours", "NOT is_hot"),
]
for label, cond in slices:
    r = cmp.filter(cond).agg(
        F.sum("true_lost_units").alias("t"),
        F.sum("est_v1").alias("v1"),
        F.sum("est_v2").alias("v2"),
    ).first()
    print(f"  {label:20} true={r.t:>7.0f}   before={r.v1 / r.t:6.1%}   after={r.v2 / r.t:6.1%}")

pairs = cmp.groupBy("store_id", "product_id").agg(
    F.sum("true_lost_units").alias("t"),
    F.sum("est_v1").alias("e1"),
    F.sum("est_v2").alias("e2"),
)
err = pairs.agg(
    F.sum("t").alias("t"),
    F.sum(F.abs(F.col("e1") - F.col("t"))).alias("a1"),
    F.sum(F.abs(F.col("e2") - F.col("t"))).alias("a2"),
).first()
print(f"Error per store-product (sum of |estimate - truth| / total truth): before={err.a1 / err.t:.1%}  after={err.a2 / err.t:.1%}")
print("Rupees lost, new estimate: Rs", f"{lost2.agg(F.sum('est_lost_revenue_inr')).first()[0]:,.0f}")

# COMMAND ----------

dim_store = read_gold("dim_store").select("store_id", "city", "area")
dim_prod = read_gold("dim_product").select("product_id", "product_name")


def restock_backtest(day):
    d = date.fromisoformat(day)
    nxt = d + timedelta(days=1)
    t_prev = F.lit(f"{d} 15:00:00").cast("timestamp")
    t_now = F.lit(f"{d} 16:00:00").cast("timestamp")
    t_last = F.lit(f"{nxt} 05:00:00").cast("timestamp")

    now_state = (
        fact.filter(F.col("hour_ts") == t_prev)
        .select("store_id", "product_id", F.col("end_of_hour_qty").alias("stock_now"))
        .filter("stock_now IS NOT NULL AND stock_now > 0")
    )

    window_rows = (
        fact.filter((F.col("hour_ts") >= t_now) & (F.col("hour_ts") <= t_last))
        .join(now_state, ["store_id", "product_id"], "inner")
        .join(level, ["store_id", "product_id"], "inner")
        .join(shape, ["hour_of_day", "is_weekend"], "inner")
        .join(mult, "product_id", "left")
        .join(price, "product_id", "left")
        .withColumn(
            "exp_dem",
            F.col("level") * F.col("shape")
            * F.when(F.col("is_rainy"), F.col("rain_mult")).otherwise(F.lit(1.0))
            * F.when(F.col("is_hot"), F.col("heat_mult")).otherwise(F.lit(1.0)),
        )
    )

    w = Window.partitionBy("store_id", "product_id").orderBy("hour_ts")
    cum = window_rows.withColumn(
        "cum_exp", F.sum("exp_dem").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow))
    )

    per_pair = (
        cum.groupBy("store_id", "product_id", "stock_now", "price_inr")
        .agg(
            F.count("*").alias("n_hours"),
            F.sum(F.col("end_of_hour_qty").isNotNull().cast("int")).alias("n_readings"),
            F.min(F.when(F.col("cum_exp") >= F.col("stock_now"), F.col("hour_ts"))).alias("pred_empty_hour"),
            F.min(F.when(F.col("is_stockout"), F.col("hour_ts"))).alias("actual_empty_hour"),
            F.max("cum_exp").alias("total_exp_demand"),
        )
        .filter("n_hours = 14 AND n_readings = 14")
    )

    return (
        per_pair
        .withColumn("predicted_run_out", F.col("pred_empty_hour").isNotNull())
        .withColumn("actual_run_out", F.col("actual_empty_hour").isNotNull())
        .withColumn("pred_hours_until_empty", (F.unix_timestamp("pred_empty_hour") - F.unix_timestamp(t_now)) / 3600 + 1)
        .withColumn("actual_hours_until_empty", (F.unix_timestamp("actual_empty_hour") - F.unix_timestamp(t_now)) / 3600 + 1)
        .withColumn(
            "revenue_at_risk_inr",
            F.greatest(F.col("total_exp_demand") - F.col("stock_now"), F.lit(0.0)) * F.col("price_inr"),
        )
    )


ranked = restock_backtest("2026-09-29")
out = (
    ranked.join(dim_store, "store_id").join(dim_prod, "product_id")
    .select(
        "store_id", "city", "area", "product_id", "product_name", "stock_now",
        "predicted_run_out", "pred_hours_until_empty",
        "actual_run_out", "actual_hours_until_empty", "revenue_at_risk_inr",
    )
)
write_gold(out, "mart_restock_priority_backtest")

(
    out.filter("predicted_run_out")
    .orderBy(F.col("revenue_at_risk_inr").desc())
    .select("store_id", "city", "product_name", "stock_now", "pred_hours_until_empty", "actual_hours_until_empty", F.round("revenue_at_risk_inr", 0).alias("revenue_at_risk_inr"))
    .show(10, truncate=False)
)

# COMMAND ----------

def score(ranked):
    m = ranked.agg(
        F.sum((F.col("predicted_run_out") & F.col("actual_run_out")).cast("int")).alias("tp"),
        F.sum((F.col("predicted_run_out") & ~F.col("actual_run_out")).cast("int")).alias("fp"),
        F.sum((~F.col("predicted_run_out") & F.col("actual_run_out")).cast("int")).alias("fn"),
        F.sum((~F.col("predicted_run_out") & ~F.col("actual_run_out")).cast("int")).alias("tn"),
    ).first()
    mae = (
        ranked.filter("predicted_run_out AND actual_run_out")
        .agg(F.avg(F.abs(F.col("pred_hours_until_empty") - F.col("actual_hours_until_empty"))))
        .first()[0]
    )
    precision = m.tp / (m.tp + m.fp) if (m.tp + m.fp) else float("nan")
    recall = m.tp / (m.tp + m.fn) if (m.tp + m.fn) else float("nan")
    return m.tp, m.fp, m.fn, m.tn, precision, recall, mae


print("day         | hit  false-alarm  miss  correct-ok | precision  recall | avg timing error (hours)")
all_tp = all_fp = all_fn = 0
for i in range(8):
    day = str(date(2026, 9, 22) + timedelta(days=i))
    tp, fp, fn, tn, p, r, mae = score(restock_backtest(day))
    all_tp, all_fp, all_fn = all_tp + tp, all_fp + fp, all_fn + fn
    print(f"{day} | {tp:4} {fp:12} {fn:5} {tn:11} | {p:9.1%} {r:7.1%} | {mae:.2f}")

print(f"All 8 days  | precision {all_tp / (all_tp + all_fp):.1%}   recall {all_tp / (all_tp + all_fn):.1%}")