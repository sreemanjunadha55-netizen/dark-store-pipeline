from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def dup_count(df: DataFrame, keys: list) -> int:
    return df.count() - df.select(*keys).distinct().count()


def null_count(df: DataFrame, cols: list) -> int:
    condition = " OR ".join([f"{c} IS NULL" for c in cols])
    return df.filter(condition).count()


def bad_count(df: DataFrame, bad_condition: str) -> int:
    return df.filter(bad_condition).count()


def orphan_count(df: DataFrame, key: str, ref_df: DataFrame, ref_key: str) -> int:
    refs = ref_df.select(F.col(ref_key).alias(key)).distinct()
    return (
        df.filter(F.col(key).isNotNull())
        .select(key)
        .distinct()
        .join(refs, key, "left_anti")
        .count()
    )