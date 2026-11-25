from datetime import datetime

from lakehouse import scd2

TRACKED = ["city", "segment"]
SCHEMA = "op string, customer_id string, city string, segment string, updated_at timestamp"


def _candidates(spark, rows):
    df = spark.createDataFrame(rows, SCHEMA)
    return scd2.to_scd2_records(df, TRACKED, "updated_at", "op")


def _current(spark, rows):
    """Build `current` target rows from (customer_id, city, segment, effective_from)."""
    df = spark.createDataFrame([("I", *r) for r in rows], SCHEMA)
    return scd2.to_scd2_records(df, TRACKED, "updated_at", "op")


T0, T1, T2 = datetime(2026, 1, 1), datetime(2026, 11, 2, 9), datetime(2026, 11, 2, 12)


def test_row_hash_is_deterministic_and_null_safe(spark):
    a = _candidates(spark, [("I", "C1", "Atlanta", None, T0)]).collect()[0].row_hash
    b = _candidates(spark, [("U", "C1", "Atlanta", None, T1)]).collect()[0].row_hash
    c = _candidates(spark, [("U", "C1", "Atlanta", "MASS", T1)]).collect()[0].row_hash
    assert a == b  # op and timestamp are not tracked attributes
    assert a != c


def test_first_load_stages_every_row_with_merge_key(spark):
    cand = _candidates(spark, [("I", "C1", "Atlanta", "MASS", T0), ("I", "C2", "Austin", "MASS", T0)])
    staged = scd2.build_staged_updates(cand, None, ["customer_id"])
    rows = staged.collect()
    assert len(rows) == 2
    assert all(r["__mk_customer_id"] == r["customer_id"] for r in rows)
    assert "op" not in staged.columns


def test_changed_new_unchanged_deleted_and_stale(spark):
    current = _current(
        spark,
        [
            ("C1", "Atlanta", "MASS", T0),  # will change city
            ("C2", "Austin", "MASS", T0),  # unchanged re-send
            ("C3", "Denver", "MASS", T0),  # deleted via CDC
            ("C4", "Boston", "MASS", T2),  # receives a stale (older) update
        ],
    )
    cand = _candidates(
        spark,
        [
            ("U", "C1", "Seattle", "MASS", T1),
            ("U", "C2", "Austin", "MASS", T1),
            ("D", "C3", "Denver", "MASS", T1),
            ("U", "C4", "Miami", "MASS", T1),
            ("I", "C5", "Phoenix", "PRIVATE", T1),
        ],
    )
    staged = scd2.build_staged_updates(cand, current, ["customer_id"]).collect()

    by_id: dict[str, list] = {}
    for r in staged:
        by_id.setdefault(r.customer_id, []).append(r)

    assert set(by_id) == {"C1", "C3", "C5"}
    # Changed key -> one row that closes the current version + one insert with null merge key
    assert sorted(r["__mk_customer_id"] is None for r in by_id["C1"]) == [False, True]
    assert all(r.city == "Seattle" and r.effective_from == T1 for r in by_id["C1"])
    # Delete -> a new version with is_deleted=true, history kept
    assert len(by_id["C3"]) == 2 and all(r.is_deleted for r in by_id["C3"])
    # New key -> a single insert
    assert len(by_id["C5"]) == 1 and by_id["C5"][0]["__mk_customer_id"] == "C5"


def test_merge_condition():
    cond = scd2.scd2_merge_condition(["customer_id"])
    assert cond == "t.`customer_id` = s.`__mk_customer_id` AND t.is_current = true"
