from darkstore.quality import bad_count, dup_count, null_count, orphan_count


def test_dup_count_finds_repeated_keys(spark):
    df = spark.createDataFrame([("O1",), ("O2",), ("O2",)], "order_id string")
    assert dup_count(df, ["order_id"]) == 1


def test_dup_count_is_zero_for_unique_keys(spark):
    df = spark.createDataFrame([("O1",), ("O2",), ("O3",)], "order_id string")
    assert dup_count(df, ["order_id"]) == 0


def test_null_count_flags_any_empty_key_column(spark):
    df = spark.createDataFrame(
        [("O1", "S001"), ("O2", None), (None, "S002")],
        "order_id string, store_id string",
    )
    assert null_count(df, ["order_id", "store_id"]) == 2


def test_bad_count_matches_condition(spark):
    df = spark.createDataFrame([(1,), (0,), (-5,)], "qty int")
    assert bad_count(df, "qty <= 0") == 2


def test_orphan_count_counts_distinct_unknown_ids_and_ignores_nulls(spark):
    orders = spark.createDataFrame(
        [("S001",), ("S999",), ("S999",), (None,)], "store_id string"
    )
    stores = spark.createDataFrame([("S001",), ("S002",)], "store_id string")
    assert orphan_count(orders, "store_id", stores, "store_id") == 1