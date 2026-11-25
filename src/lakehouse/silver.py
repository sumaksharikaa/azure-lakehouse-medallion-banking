"""
Silver layer transformations: conform types, standardize, de-duplicate.

Everything here is a pure DataFrame -> DataFrame function, so the business
logic can be unit-tested without any storage.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from lakehouse.config import EntityConfig

METADATA_COLUMNS = ["_source_file", "_ingest_ts", "_batch_date"]


def standardize_strings(df: DataFrame) -> DataFrame:
    """Trim every string column and turn empty strings into nulls."""
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType) and f.name not in METADATA_COLUMNS:
            trimmed = F.trim(F.col(f.name))
            df = df.withColumn(f.name, F.when(trimmed == "", None).otherwise(trimmed))
    return df


def conform(df: DataFrame, entity: EntityConfig) -> DataFrame:
    """Project to the configured schema with safe casts, then add derived columns.

    try_cast turns unparseable values into nulls instead of failing the job,
    whatever the cluster's ANSI setting is; the data-quality rules then catch
    the nulls on required columns.
    """
    df = standardize_strings(df)
    cols = []
    if entity.cdc_op_column:
        cols.append(F.upper(F.col(entity.cdc_op_column)).alias(entity.cdc_op_column))
    for name, dtype in entity.schema.items():
        cols.append(F.expr(f"try_cast(`{name}` AS {dtype})").alias(name))
    extras = [c for c in ("_corrupt_record", *METADATA_COLUMNS) if c in df.columns]
    out = df.select(*cols, *extras)

    if "email" in out.columns:
        out = out.withColumn("email", F.lower("email"))
    for name, expr in entity.derived_columns.items():
        out = out.withColumn(name, F.expr(expr))
    return out


def deduplicate_latest(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    """Keep the latest record per business key (by sequence column, then ingestion time).

    Rows with a null key are kept as they are, so the not_null rule can
    quarantine them rather than having them silently collapse into one row.
    """
    tie_breakers = [F.col(order_col).desc_nulls_last()]
    if "_ingest_ts" in df.columns:
        tie_breakers.append(F.col("_ingest_ts").desc())
    w = Window.partitionBy(*keys).orderBy(*tie_breakers)
    null_key = F.lit(False)
    for k in keys:
        null_key = null_key | F.col(k).isNull()
    ranked = df.withColumn("__rn", F.row_number().over(w))
    return ranked.filter((F.col("__rn") == 1) | null_key).drop("__rn")


def drop_technical_columns(df: DataFrame) -> DataFrame:
    return df.drop("_corrupt_record", "_source_file")
