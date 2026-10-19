from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def stockout_spells(fact: DataFrame) -> DataFrame:
    w = Window.partitionBy("store_id", "product_id").orderBy("hour_ts")
    indexed = fact.withColumn("hour_idx", F.row_number().over(w))
    grouped = (
        indexed.filter("is_stockout")
        .withColumn("rn", F.row_number().over(w))
        .withColumn("spell_key", F.col("hour_idx") - F.col("rn"))
    )
    return (
        grouped.groupBy("store_id", "product_id", "spell_key")
        .agg(
            F.min("hour_ts").alias("first_empty_hour"),
            F.max("hour_ts").alias("last_empty_hour"),
            F.count("*").alias("empty_hours"),
        )
        .drop("spell_key")
    )