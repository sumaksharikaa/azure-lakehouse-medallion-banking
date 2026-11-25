"""
Pipeline entry point: Bronze -> Silver -> Gold for one or more batch dates.

    python -m lakehouse.pipeline --batch-date 2026-11-01
    python -m lakehouse.pipeline --start-date 2026-11-01 --end-date 2026-11-03
    python -m lakehouse.pipeline --batch-date 2026-11-02 --layers silver,gold

The same entry point runs locally, as a Databricks job task, or from an
Azure Data Factory Databricks activity (see orchestration/).
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from lakehouse import bronze, delta_io, gold, quality, scd2, silver
from lakehouse.config import EntityConfig, PipelineConfig, load_config
from lakehouse.spark import get_spark

log = logging.getLogger("lakehouse")

AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("batch_date", StringType()),
        StructField("layer", StringType()),
        StructField("entity", StringType()),
        StructField("status", StringType()),
        StructField("rows_in", LongType()),
        StructField("rows_out", LongType()),
        StructField("rows_quarantined", LongType()),
        StructField("dq_failures", StringType()),
        StructField("started_at", StringType()),
        StructField("duration_s", DoubleType()),
        StructField("error", StringType()),
    ]
)


@dataclass
class StepResult:
    run_id: str
    batch_date: str
    layer: str
    entity: str
    status: str = "RUNNING"
    rows_in: int = 0
    rows_out: int = 0
    rows_quarantined: int = 0
    dq_failures: str = "{}"
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    duration_s: float = 0.0
    error: str | None = None


# --------------------------------------------------------------------- bronze
def run_bronze(
    spark: SparkSession, cfg: PipelineConfig, entity: EntityConfig, batch_date: str, res: StepResult
) -> None:
    src = cfg.path("landing", entity.name, f"batch_date={batch_date}")
    try:
        raw = bronze.read_landing(spark, entity, src)
    except AnalysisException as exc:
        if "PATH_NOT_FOUND" in str(exc) or "Path does not exist" in str(exc):
            log.warning("No landing data for %s on %s, skipping", entity.name, batch_date)
            res.status = "SKIPPED_NO_DATA"
            return
        raise
    df = bronze.add_ingestion_metadata(raw, batch_date)
    delta_io.overwrite_partition(df, cfg.path("bronze", entity.name), "_batch_date", batch_date)
    res.rows_in = res.rows_out = (
        delta_io.read(spark, cfg.path("bronze", entity.name))
        .filter(F.col("_batch_date") == batch_date)
        .count()
    )


# --------------------------------------------------------------------- silver
def _references(spark: SparkSession, cfg: PipelineConfig, entity: EntityConfig) -> dict[str, DataFrame]:
    refs = {}
    for rule in entity.quality_rules:
        if rule["type"] == "foreign_key":
            ref_path = cfg.path("silver", rule["reference"])
            if not delta_io.table_exists(spark, ref_path):
                raise RuntimeError(
                    f"{entity.name}: reference table '{rule['reference']}' must be loaded first"
                )
            refs[rule["reference"]] = delta_io.read(spark, ref_path)
    return refs


def run_silver(
    spark: SparkSession, cfg: PipelineConfig, entity: EntityConfig, batch_date: str, res: StepResult
) -> None:
    bronze_path = cfg.path("bronze", entity.name)
    if not delta_io.table_exists(spark, bronze_path):
        res.status = "SKIPPED_NO_DATA"
        return
    batch = delta_io.read(spark, bronze_path).filter(F.col("_batch_date") == batch_date)

    conformed = silver.conform(batch, entity)
    deduped = silver.deduplicate_latest(conformed, entity.business_keys, entity.sequence_column)
    checked = quality.apply_rules(deduped, entity.quality_rules, _references(spark, cfg, entity)).cache()
    try:
        valid, invalid = quality.split_valid_invalid(checked)
        res.rows_in = batch.count()
        res.rows_quarantined = invalid.count()
        res.dq_failures = json.dumps(quality.failure_summary(invalid), sort_keys=True)

        delta_io.overwrite_partition(invalid, cfg.path("quarantine", entity.name), "_batch_date", batch_date)

        valid = silver.drop_technical_columns(valid)
        target = cfg.path("silver", entity.name)
        if entity.load_type == "scd1":
            delta_io.merge_scd1(
                spark, valid, target, entity.business_keys, entity.sequence_column, entity.partition_by
            )
        elif entity.load_type == "append_dedup":
            delta_io.merge_insert_only(spark, valid, target, entity.business_keys, entity.partition_by)
        elif entity.load_type == "scd2":
            candidates = scd2.to_scd2_records(
                valid, entity.scd2_tracked_columns, entity.sequence_column, entity.cdc_op_column
            )
            current = (
                delta_io.read(spark, target).filter("is_current")
                if delta_io.table_exists(spark, target)
                else None
            )
            staged = scd2.build_staged_updates(candidates, current, entity.business_keys)
            delta_io.merge_scd2(spark, staged, target, entity.business_keys)
        res.rows_out = res.rows_in - res.rows_quarantined
    finally:
        checked.unpersist()


# ----------------------------------------------------------------------- gold
def run_gold(spark: SparkSession, cfg: PipelineConfig, res: StepResult, optimize: bool = True) -> None:
    customers = delta_io.read(spark, cfg.path("silver", "customers"))
    accounts = delta_io.read(spark, cfg.path("silver", "accounts"))
    txns = delta_io.read(spark, cfg.path("silver", "transactions"))

    dim_customer = gold.build_dim_customer(customers)
    dim_account = gold.build_dim_account(accounts)
    delta_io.overwrite_table(dim_customer, cfg.path("gold", "dim_customer"))
    delta_io.overwrite_table(dim_account, cfg.path("gold", "dim_account"))

    # Re-read the dims just written so every gold table comes from one consistent snapshot.
    dim_customer = delta_io.read(spark, cfg.path("gold", "dim_customer"))
    dim_account = delta_io.read(spark, cfg.path("gold", "dim_account"))

    fact = gold.build_fact_transactions(txns, dim_account, dim_customer, cfg.fx_rates_to_usd)
    delta_io.overwrite_table(fact, cfg.path("gold", "fact_transactions"), partition_by=["txn_date"])
    fact = delta_io.read(spark, cfg.path("gold", "fact_transactions"))

    delta_io.overwrite_table(
        gold.build_daily_account_activity(fact, dim_account),
        cfg.path("gold", "agg_daily_account_activity"),
    )
    delta_io.overwrite_table(
        gold.build_customer_monthly(fact, dim_customer),
        cfg.path("gold", "agg_customer_monthly"),
    )

    if optimize:
        for table, opts in cfg.gold.get("optimize", {}).items():
            delta_io.optimize_zorder(spark, cfg.path("gold", table), opts["zorder_by"])

    res.rows_in = txns.count()
    res.rows_out = fact.count()


# ------------------------------------------------------------------ orchestration
def _timed(step, res: StepResult, results: list[StepResult]) -> None:
    t0 = time.perf_counter()
    try:
        step(res)
        if res.status == "RUNNING":
            res.status = "SUCCEEDED"
    except Exception as exc:
        res.status = "FAILED"
        res.error = f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        res.duration_s = round(time.perf_counter() - t0, 2)
        results.append(res)
        log.info(
            "%-6s %-12s %-10s %-15s in=%-6d out=%-6d quarantined=%-4d %s",
            res.layer,
            res.entity,
            res.batch_date,
            res.status,
            res.rows_in,
            res.rows_out,
            res.rows_quarantined,
            res.dq_failures,
        )


def run(
    cfg: PipelineConfig,
    batch_dates: list[str],
    layers: list[str],
    optimize: bool = True,
    spark: SparkSession | None = None,
) -> list[StepResult]:
    spark = spark or get_spark()
    run_id = str(uuid.uuid4())
    results: list[StepResult] = []
    try:
        for d in batch_dates:
            for ent in cfg.entities:
                if "bronze" in layers:
                    _timed(
                        lambda r, e=ent, d=d: run_bronze(spark, cfg, e, d, r),
                        StepResult(run_id, d, "bronze", ent.name),
                        results,
                    )
            for ent in cfg.entities:
                if "silver" in layers:
                    _timed(
                        lambda r, e=ent, d=d: run_silver(spark, cfg, e, d, r),
                        StepResult(run_id, d, "silver", ent.name),
                        results,
                    )
        if "gold" in layers:
            _timed(
                lambda r: run_gold(spark, cfg, r, optimize),
                StepResult(run_id, batch_dates[-1], "gold", "all"),
                results,
            )
    finally:
        if results:
            audit_df = spark.createDataFrame([asdict(r) for r in results], AUDIT_SCHEMA)
            delta_io.append(audit_df, cfg.path("audit"))
    return results


def _date_range(start: str, end: str) -> list[str]:
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    return [(s + timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]


def main() -> None:
    p = argparse.ArgumentParser(description="Medallion lakehouse pipeline")
    p.add_argument("--config", default="conf/pipeline.yaml")
    p.add_argument("--root", help="Override storage root (local path or abfss:// URI)")
    p.add_argument("--batch-date")
    p.add_argument("--start-date")
    p.add_argument("--end-date")
    p.add_argument("--layers", default="bronze,silver,gold")
    p.add_argument("--no-optimize", action="store_true")
    a = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.batch_date:
        dates = [a.batch_date]
    elif a.start_date and a.end_date:
        dates = _date_range(a.start_date, a.end_date)
    else:
        p.error("Provide --batch-date or --start-date/--end-date")

    cfg = load_config(a.config, root_override=a.root)
    run(cfg, dates, [x.strip() for x in a.layers.split(",")], optimize=not a.no_optimize)


if __name__ == "__main__":
    main()
