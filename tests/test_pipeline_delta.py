"""
End-to-end integration test on real Delta tables.

Runs in CI (LAKEHOUSE_DELTA_TESTS=1). Locally it needs the Delta jars from
Maven Central, which configure_spark_with_delta_pip downloads on first use.
"""

from datetime import date

import pytest
from pyspark.sql import functions as F

from generate_banking_data import generate
from lakehouse import delta_io
from lakehouse.config import load_config
from lakehouse.pipeline import run

pytestmark = pytest.mark.delta
DATES = ["2026-11-01", "2026-11-02", "2026-11-03"]


@pytest.fixture(scope="module")
def lake(spark, repo_root, tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    generate(root / "landing", date(2026, 11, 1), days=3, seed=11, customers=60, txns=400)
    cfg = load_config(repo_root / "conf" / "pipeline.yaml", root_override=str(root))
    results = run(cfg, DATES, ["bronze", "silver", "gold"], spark=spark)
    return cfg, results


def _t(spark, cfg, layer, name):
    return delta_io.read(spark, cfg.path(layer, name))


def test_all_steps_succeeded(lake):
    _, results = lake
    assert {r.status for r in results} == {"SUCCEEDED"}
    assert sum(r.rows_quarantined for r in results) > 0


def test_silver_transactions_unique(spark, lake):
    cfg, _ = lake
    txns = _t(spark, cfg, "silver", "transactions")
    assert txns.count() == txns.select("txn_id").distinct().count()


def test_scd2_one_current_row_per_customer(spark, lake):
    cfg, _ = lake
    cust = _t(spark, cfg, "silver", "customers")
    per_key = cust.filter("is_current").groupBy("customer_id").count()
    assert per_key.filter("count > 1").count() == 0
    assert cust.filter(~F.col("is_current")).count() > 0, "history versions expected"
    assert cust.filter("is_current AND is_deleted").count() > 0, "CDC deletes expected"
    closed = cust.filter(~F.col("is_current"))
    assert closed.filter("effective_to IS NULL").count() == 0


def test_rerun_is_idempotent(spark, lake):
    cfg, _ = lake
    before = {n: _t(spark, cfg, "silver", n).count() for n in ("customers", "accounts", "transactions")}
    run(cfg, ["2026-11-02"], ["bronze", "silver"], spark=spark)
    after = {n: _t(spark, cfg, "silver", n).count() for n in ("customers", "accounts", "transactions")}
    assert before == after


def test_gold_reconciles_with_silver(spark, lake):
    cfg, _ = lake
    silver_txn = _t(spark, cfg, "silver", "transactions")
    fact = _t(spark, cfg, "gold", "fact_transactions")
    assert fact.count() == silver_txn.count()
    assert fact.filter("customer_sk IS NULL").count() == 0, "every txn maps to a customer version"
    daily = _t(spark, cfg, "gold", "agg_daily_account_activity")
    assert daily.agg(F.sum("txn_count")).first()[0] == fact.count()


def test_audit_log_written(spark, lake):
    cfg, _ = lake
    audit = delta_io.read(spark, cfg.path("audit"))
    assert audit.filter("layer = 'gold'").count() >= 1
