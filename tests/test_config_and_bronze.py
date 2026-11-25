import pytest

from generate_banking_data import generate
from lakehouse import bronze, quality, silver
from lakehouse.config import EntityConfig, load_config


def test_repo_config_loads(repo_root):
    cfg = load_config(repo_root / "conf" / "pipeline.yaml", root_override="/tmp/lh")
    assert [e.name for e in cfg.entities] == ["customers", "accounts", "transactions"]
    assert cfg.path("silver", "accounts") == "/tmp/lh/lakehouse/silver/accounts"
    abfss = load_config(
        repo_root / "conf" / "pipeline.yaml", root_override="abfss://lake@acct.dfs.core.windows.net"
    )
    assert abfss.path("gold", "dim_customer").startswith(
        "abfss://lake@acct.dfs.core.windows.net/lakehouse/gold"
    )


def test_invalid_config_rejected():
    with pytest.raises(ValueError):
        EntityConfig(
            name="x",
            format="csv",
            business_keys=["id"],
            load_type="nope",
            schema={"id": "string"},
            sequence_column="id",
        )
    with pytest.raises(ValueError):
        EntityConfig(
            name="x",
            format="csv",
            business_keys=["missing"],
            load_type="scd1",
            schema={"id": "string"},
            sequence_column="id",
        )


def test_landing_to_conformed_for_every_entity(spark, repo_root, tmp_path):
    """Generator output -> bronze read -> conform -> DQ, for all three formats."""
    generate(tmp_path, __import__("datetime").date(2026, 11, 1), days=2, seed=7, customers=40, txns=300)
    cfg = load_config(repo_root / "conf" / "pipeline.yaml", root_override=str(tmp_path))

    accounts_ref = None
    for ent in cfg.entities:
        raw = bronze.read_landing(spark, ent, str(tmp_path / ent.name / "batch_date=2026-11-02"))
        df = silver.conform(bronze.add_ingestion_metadata(raw, "2026-11-02"), ent)
        df = silver.deduplicate_latest(df, ent.business_keys, ent.sequence_column)
        refs = {"accounts": accounts_ref} if ent.name == "transactions" else {}
        valid, invalid = quality.split_valid_invalid(quality.apply_rules(df, ent.quality_rules, refs))
        failures = quality.failure_summary(invalid)

        assert valid.count() > 0, ent.name
        if ent.name == "customers":
            assert {"customer_id_not_null", "email_format"} <= set(failures)
        if ent.name == "accounts":
            assert failures.get("not_corrupt") == 1
            # Accounts from day 1 are needed as the FK reference for day 2 transactions
            raw1 = bronze.read_landing(spark, ent, str(tmp_path / ent.name / "batch_date=2026-11-01"))
            accounts_ref = silver.conform(raw1, ent).unionByName(
                valid.select(*silver.conform(raw1, ent).columns)
            )
        if ent.name == "transactions":
            assert {"txn_id_not_null", "account_exists", "amount_positive", "currency_supported"} <= set(
                failures
            )
            ids = [r.txn_id for r in valid.select("txn_id").collect()]
            assert len(ids) == len(set(ids)), "duplicates must be removed"
