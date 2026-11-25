"""
Gold layer: a star schema and analytics-ready aggregates.

    dim_customer                 SCD2 history with a surrogate key
    dim_account                  current account state
    fact_transactions            point-in-time joined to the customer version valid at txn time
    agg_daily_account_activity   daily flows plus a running balance (window functions)
    agg_customer_monthly         monthly spend/income per customer plus top merchant category
"""

from __future__ import annotations

from itertools import chain

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

HIGH_DATE = "9999-12-31 00:00:00"
LOW_DATE = "1900-01-01 00:00:00"


def build_dim_customer(silver_customers: DataFrame) -> DataFrame:
    return silver_customers.select(
        F.xxhash64("customer_id", "effective_from").alias("customer_sk"),
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "state",
        "segment",
        "kyc_status",
        "effective_from",
        F.coalesce("effective_to", F.lit(HIGH_DATE).cast("timestamp")).alias("effective_to"),
        "is_current",
        "is_deleted",
    )


def build_dim_account(silver_accounts: DataFrame) -> DataFrame:
    return silver_accounts.select(
        F.xxhash64("account_id").alias("account_sk"),
        "account_id",
        "customer_id",
        "account_type",
        "currency",
        "status",
        "opened_date",
        "opening_balance",
        "credit_limit",
        F.col("updated_at").alias("last_updated_at"),
    )


def _fx_map(rates: dict[str, float]):
    return F.create_map(*chain.from_iterable((F.lit(k), F.lit(v)) for k, v in rates.items()))


def build_fact_transactions(
    silver_txns: DataFrame,
    dim_account: DataFrame,
    dim_customer: DataFrame,
    fx_rates_to_usd: dict[str, float],
) -> DataFrame:
    acct = dim_account.select("account_id", "account_sk", "customer_id", "account_type")
    # Early-arriving facts: a txn can precede the first recorded customer version
    # (e.g. onboarding and a same-day update collapse into one batch record).
    # The first version is treated as valid from LOW_DATE so these facts still resolve.
    first_w = Window.partitionBy("customer_id")
    cust = dim_customer.select(
        F.col("customer_id").alias("__cust_id"),
        "customer_sk",
        F.when(
            F.col("effective_from") == F.min("effective_from").over(first_w),
            F.lit(LOW_DATE).cast("timestamp"),
        )
        .otherwise(F.col("effective_from"))
        .alias("effective_from"),
        "effective_to",
    )
    txn = silver_txns.join(F.broadcast(acct), "account_id", "left")

    # Point-in-time join: pick the customer version that was valid when the txn happened.
    pit = txn.join(
        cust,
        (txn["customer_id"] == cust["__cust_id"])
        & (txn["txn_ts"] >= cust["effective_from"])
        & (txn["txn_ts"] < cust["effective_to"]),
        "left",
    )
    amount_usd = (F.col("amount") * _fx_map(fx_rates_to_usd)[F.col("currency")]).cast("decimal(18,2)")
    return pit.select(
        "txn_id",
        "txn_ts",
        "txn_date",
        "account_sk",
        "account_id",
        "customer_sk",
        "customer_id",
        "account_type",
        "txn_type",
        "channel",
        "merchant_category",
        "currency",
        "amount",
        amount_usd.alias("amount_usd"),
        F.when(F.col("txn_type") == "DEBIT", -amount_usd).otherwise(amount_usd).alias("signed_amount_usd"),
    )


def build_daily_account_activity(fact: DataFrame, dim_account: DataFrame) -> DataFrame:
    daily = fact.groupBy("account_id", "txn_date").agg(
        F.count("*").alias("txn_count"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount_usd")).otherwise(0)).alias("debits_usd"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount_usd")).otherwise(0)).alias("credits_usd"),
        F.sum("signed_amount_usd").alias("net_flow_usd"),
    )
    w = (
        Window.partitionBy("account_id")
        .orderBy("txn_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return (
        daily.join(dim_account.select("account_id", "opening_balance"), "account_id", "left")
        .withColumn("running_net_flow_usd", F.sum("net_flow_usd").over(w))
        .withColumn(
            "closing_balance_usd",
            (F.coalesce("opening_balance", F.lit(0)) + F.col("running_net_flow_usd")).cast("decimal(18,2)"),
        )
        .drop("opening_balance")
    )


def build_customer_monthly(fact: DataFrame, dim_customer: DataFrame) -> DataFrame:
    f = fact.filter(F.col("customer_id").isNotNull()).withColumn("month", F.trunc("txn_date", "month"))
    monthly = f.groupBy("customer_id", "month").agg(
        F.count("*").alias("txn_count"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount_usd"))).alias("spend_usd"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount_usd"))).alias("income_usd"),
        F.avg("amount_usd").cast("decimal(18,2)").alias("avg_txn_usd"),
        F.countDistinct("merchant_category").alias("distinct_categories"),
    )
    by_category = (
        f.filter(F.col("txn_type") == "DEBIT")
        .groupBy("customer_id", "month", "merchant_category")
        .agg(F.sum("amount_usd").alias("cat_spend"))
    )
    rank_w = Window.partitionBy("customer_id", "month").orderBy(
        F.col("cat_spend").desc(), F.col("merchant_category")
    )
    top = (
        by_category.withColumn("rk", F.row_number().over(rank_w))
        .filter("rk = 1")
        .select("customer_id", "month", F.col("merchant_category").alias("top_merchant_category"))
    )
    current = dim_customer.filter("is_current").select("customer_id", "segment", "state")
    return (
        monthly.join(top, ["customer_id", "month"], "left")
        .join(current, "customer_id", "left")
        .fillna({"spend_usd": 0, "income_usd": 0})
    )
