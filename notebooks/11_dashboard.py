# Databricks notebook source
G = "abfss://gold@stdarkstore55.dfs.core.windows.net"
for t in ["fact_store_product_hour", "mart_lost_sales_hourly_v2", "dim_store",
          "mart_lost_sales_by_store_product", "ops_dq_results"]:
    spark.read.format("delta").load(f"{G}/{t}").createOrReplaceTempView(t)
print("views ready")

# COMMAND ----------

display(spark.sql("""
SELECT SUM(units_sold) AS units_sold,
       ROUND(SUM(revenue_inr)) AS revenue_inr,
       ROUND(AVG(CASE WHEN inventory_reading_missing THEN NULL ELSE CAST(is_stockout AS DOUBLE) END) * 100, 1) AS stockout_pct
FROM fact_store_product_hour
"""))

# COMMAND ----------

display(spark.sql("""
SELECT hour_of_day, COUNT_IF(is_stockout) AS empty_shelf_hours
FROM fact_store_product_hour
GROUP BY hour_of_day ORDER BY hour_of_day
"""))

# COMMAND ----------

display(spark.sql("""
SELECT to_date(hour_ts) AS day,
       ROUND(AVG(CASE WHEN inventory_reading_missing THEN NULL ELSE CAST(is_stockout AS DOUBLE) END) * 100, 1) AS stockout_pct
FROM fact_store_product_hour
GROUP BY to_date(hour_ts) ORDER BY day
"""))

# COMMAND ----------

display(spark.sql("""
SELECT s.city, ROUND(SUM(l.est_lost_revenue_inr)) AS est_lost_revenue_inr
FROM mart_lost_sales_hourly_v2 l
JOIN dim_store s ON l.store_id = s.store_id
GROUP BY s.city ORDER BY est_lost_revenue_inr DESC
"""))

# COMMAND ----------

display(spark.sql("""
SELECT concat(city, ' - ', product_name) AS store_product, est_lost_revenue_inr
FROM mart_lost_sales_by_store_product
ORDER BY est_lost_revenue_inr DESC LIMIT 10
"""))

# COMMAND ----------

display(spark.sql("""
SELECT run_id, from_utc_timestamp(MIN(run_at), 'Asia/Kolkata') AS run_time_ist,
       COUNT(*) AS checks, SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed_checks
FROM ops_dq_results
GROUP BY run_id ORDER BY run_time_ist
"""))