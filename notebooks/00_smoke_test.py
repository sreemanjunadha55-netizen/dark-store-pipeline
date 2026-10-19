# Databricks notebook source
print(spark.version)

df = spark.read.parquet("abfss://landing@stdarkstore55.dfs.core.windows.net/orders/")
print("Order rows:", df.count())
df.show(5)