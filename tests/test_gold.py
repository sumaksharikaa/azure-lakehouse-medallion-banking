from datetime import date, datetime
from decimal import Decimal

import pytest

from lakehouse import gold

FX = {"USD": 1.0, "EUR": 1.10}


@pytest.fixture
def dims(spark):
    customers = spark.createDataFrame(
        [
            (
                "C1",
                "A",
                "B",
                "a@b.com",
                "Atlanta",
                "GA",
                "MASS",
                "VERIFIED",
                datetime(2026, 1, 1),
                datetime(2026, 11, 2, 12),
                False,
                False,
            ),
            (
                "C1",
                "A",
                "B",
                "a@b.com",
                "Seattle",
                "WA",
                "AFFLUENT",
                "VERIFIED",
                datetime(2026, 11, 2, 12),
                None,
                True,
                False,
            ),
        ],
        "customer_id string, first_name string, last_name string, email string, city string, state string, "
        "segment string, kyc_status string, effective_from timestamp, effective_to timestamp, "
        "is_current boolean, is_deleted boolean",
    )
    accounts = spark.createDataFrame(
        [
            (
                "A1",
                "C1",
                "CHECKING",
                "USD",
                "ACTIVE",
                date(2020, 1, 1),
                Decimal("1000.00"),
                None,
                datetime(2026, 11, 1),
            )
        ],
        "account_id string, customer_id string, account_type string, currency string, status string, "
        "opened_date date, opening_balance decimal(18,2), credit_limit decimal(18,2), updated_at timestamp",
    )
    return gold.build_dim_customer(customers), gold.build_dim_account(accounts)


@pytest.fixture
def txns(spark):
    rows = [
        (
            "t1",
            "A1",
            datetime(2026, 11, 1, 10),
            date(2026, 11, 1),
            Decimal("100.00"),
            "USD",
            "DEBIT",
            "POS",
            "GROCERY",
        ),
        (
            "t2",
            "A1",
            datetime(2026, 11, 2, 15),
            date(2026, 11, 2),
            Decimal("200.00"),
            "EUR",
            "DEBIT",
            "ONLINE",
            "TRAVEL",
        ),
        (
            "t3",
            "A1",
            datetime(2026, 11, 2, 16),
            date(2026, 11, 2),
            Decimal("500.00"),
            "USD",
            "CREDIT",
            "ACH",
            "PAYROLL",
        ),
    ]
    return spark.createDataFrame(
        rows,
        "txn_id string, account_id string, txn_ts timestamp, txn_date date, amount decimal(18,2), "
        "currency string, txn_type string, channel string, merchant_category string",
    )


def test_dim_customer_surrogate_keys_and_open_end_date(dims):
    dim_customer, _ = dims
    rows = dim_customer.collect()
    assert len({r.customer_sk for r in rows}) == 2
    current = [r for r in rows if r.is_current][0]
    assert current.effective_to == datetime(9999, 12, 31)


def test_fact_point_in_time_join_and_fx(dims, txns):
    dim_customer, dim_account = dims
    fact = {r.txn_id: r for r in gold.build_fact_transactions(txns, dim_account, dim_customer, FX).collect()}
    sk = {r.city: r.customer_sk for r in dim_customer.collect()}
    assert fact["t1"].customer_sk == sk["Atlanta"]  # before the relocation
    assert fact["t2"].customer_sk == sk["Seattle"]  # after the relocation
    assert fact["t2"].amount_usd == Decimal("220.00")
    assert fact["t2"].signed_amount_usd == Decimal("-220.00")
    assert fact["t3"].signed_amount_usd == Decimal("500.00")


def test_fact_early_arriving_txn_maps_to_first_version(spark, dims, txns):
    dim_customer, dim_account = dims
    early = spark.createDataFrame(
        [
            (
                "t0",
                "A1",
                datetime(2025, 6, 1),
                date(2025, 6, 1),
                Decimal("1.00"),
                "USD",
                "DEBIT",
                "POS",
                "FUEL",
            )
        ],
        txns.schema,
    )
    fact = gold.build_fact_transactions(txns.unionByName(early), dim_account, dim_customer, FX)
    assert fact.count() == 4  # no fan-out
    sk = {r.city: r.customer_sk for r in dim_customer.collect()}
    assert fact.filter("txn_id = 't0'").first().customer_sk == sk["Atlanta"]


def test_daily_activity_running_balance(dims, txns):
    dim_customer, dim_account = dims
    fact = gold.build_fact_transactions(txns, dim_account, dim_customer, FX)
    daily = {r.txn_date: r for r in gold.build_daily_account_activity(fact, dim_account).collect()}
    assert daily[date(2026, 11, 1)].closing_balance_usd == Decimal("900.00")
    assert daily[date(2026, 11, 2)].net_flow_usd == Decimal("280.00")
    assert daily[date(2026, 11, 2)].closing_balance_usd == Decimal("1180.00")


def test_customer_monthly_summary(dims, txns):
    dim_customer, dim_account = dims
    fact = gold.build_fact_transactions(txns, dim_account, dim_customer, FX)
    row = gold.build_customer_monthly(fact, dim_customer).collect()[0]
    assert row.txn_count == 3
    assert row.spend_usd == Decimal("320.00")
    assert row.income_usd == Decimal("500.00")
    assert row.top_merchant_category == "TRAVEL"
    assert row.segment == "AFFLUENT"  # current segment
