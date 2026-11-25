import datetime as dt
from decimal import Decimal

from lakehouse import silver
from lakehouse.config import EntityConfig


def _entity(**overrides):
    base = dict(
        name="transactions",
        format="parquet",
        business_keys=["txn_id"],
        load_type="append_dedup",
        sequence_column="txn_ts",
        schema={"txn_id": "string", "txn_ts": "timestamp", "amount": "decimal(18,2)", "currency": "string"},
        derived_columns={"txn_date": "to_date(txn_ts)"},
    )
    base.update(overrides)
    return EntityConfig(**base)


def test_conform_casts_trims_and_derives(spark):
    raw = spark.createDataFrame(
        [(" t1 ", "2026-11-01 10:00:00", "12.345", "USD", "x"), ("t2", "not-a-date", "abc", "", "y")],
        "txn_id string, txn_ts string, amount string, currency string, unexpected_col string",
    )
    out = {r.txn_id: r for r in silver.conform(raw, _entity()).collect()}
    assert set(out) == {"t1", "t2"}
    assert out["t1"].txn_ts == dt.datetime(2026, 11, 1, 10, 0)
    assert out["t1"].amount == Decimal("12.35")
    assert out["t1"].txn_date == dt.date(2026, 11, 1)
    # Bad values become null instead of failing the job
    assert out["t2"].txn_ts is None and out["t2"].amount is None
    assert out["t2"].currency is None  # empty string -> null
    assert "unexpected_col" not in silver.conform(raw, _entity()).columns


def test_conform_keeps_cdc_op_and_lowercases_email(spark):
    ent = EntityConfig(
        name="customers",
        format="csv",
        business_keys=["customer_id"],
        load_type="scd2",
        sequence_column="updated_at",
        cdc_op_column="op",
        scd2_tracked_columns=["email"],
        schema={"customer_id": "string", "email": "string", "updated_at": "timestamp"},
    )
    raw = spark.createDataFrame(
        [("u", "C1", "John.Doe@Example.COM", "2026-11-01 00:00:00")],
        "op string, customer_id string, email string, updated_at string",
    )
    row = silver.conform(raw, ent).collect()[0]
    assert row.op == "U" and row.email == "john.doe@example.com"


def test_deduplicate_latest_keeps_newest_and_null_keys(spark):
    df = spark.createDataFrame(
        [
            ("C1", "2026-11-01 01:00:00", "old"),
            ("C1", "2026-11-01 05:00:00", "new"),
            ("C2", "2026-11-01 01:00:00", "only"),
            (None, "2026-11-01 01:00:00", "nokey-a"),
            (None, "2026-11-01 02:00:00", "nokey-b"),
        ],
        "id string, ts string, v string",
    )
    out = silver.deduplicate_latest(df, ["id"], "ts")
    vals = sorted(r.v for r in out.collect())
    assert vals == ["new", "nokey-a", "nokey-b", "only"]
