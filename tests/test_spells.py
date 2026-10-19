from datetime import datetime, timedelta

from darkstore.spells import stockout_spells

SCHEMA = "store_id string, product_id string, hour_ts timestamp, is_stockout boolean"
T0 = datetime(2026, 9, 1, 0, 0, 0)


def _rows(store, product, flags):
    return [(store, product, T0 + timedelta(hours=i), flag) for i, flag in enumerate(flags)]


def test_spells_split_on_restock_and_on_unknown_hours(spark):
    s1 = [False, False, True, True, True, False, False, True, None, True, True, False]
    s2 = [True, True, False]
    df = spark.createDataFrame(_rows("S001", "P001", s1) + _rows("S002", "P001", s2), SCHEMA)

    out = stockout_spells(df).orderBy("store_id", "first_empty_hour").collect()

    assert [(r.store_id, r.empty_hours) for r in out] == [
        ("S001", 3),
        ("S001", 1),
        ("S001", 2),
        ("S002", 2),
    ]
    assert out[0].first_empty_hour == T0 + timedelta(hours=2)
    assert out[0].last_empty_hour == T0 + timedelta(hours=4)


def test_no_stockouts_means_no_spells(spark):
    df = spark.createDataFrame(_rows("S001", "P001", [False] * 6), SCHEMA)
    assert stockout_spells(df).count() == 0


def test_a_restock_between_two_empty_hours_makes_two_spells(spark):
    df = spark.createDataFrame(_rows("S001", "P001", [True, False, True]), SCHEMA)
    assert stockout_spells(df).count() == 2