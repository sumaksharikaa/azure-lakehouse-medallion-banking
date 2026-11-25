"""
SCD Type 2 change detection.

Builds the "staged updates" input for one Delta MERGE that, in a single
atomic commit:
  * closes the current version of every changed key (is_current=false,
    effective_to = change time), and
  * inserts the new version of changed keys and brand-new keys.

How it works: a changed key appears twice in the staged DataFrame.
  1. With its merge key set: it MATCHES the current target row, which is closed.
  2. With a NULL merge key: it can never match, so it is INSERTED as the new version.
New keys appear once (merge key set, no match, inserted). Keys whose tracked
attributes have not changed are filtered out before the MERGE, so they cost
nothing.

CDC deletes (op = 'D') become a new version with is_deleted = true. History is
kept, which audit and regulatory reporting need.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SCD2_COLUMNS = ["row_hash", "effective_from", "effective_to", "is_current", "is_deleted"]


def merge_key_col(key: str) -> str:
    return f"__mk_{key}"


def add_row_hash(df: DataFrame, tracked_columns: list[str]) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in tracked_columns]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def to_scd2_records(
    incoming: DataFrame,
    tracked_columns: list[str],
    sequence_column: str,
    op_column: str | None,
) -> DataFrame:
    """Shape de-duplicated incoming changes as candidate SCD2 rows."""
    is_deleted = (F.col(op_column) == "D") if op_column else F.lit(False)
    out = (
        incoming.withColumn("is_deleted", F.coalesce(is_deleted, F.lit(False)))
        .withColumn("effective_from", F.col(sequence_column))
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )
    if op_column:
        out = out.drop(op_column)
    return add_row_hash(out, [*tracked_columns, "is_deleted"])


def build_staged_updates(
    candidates: DataFrame,
    current: DataFrame | None,
    keys: list[str],
) -> DataFrame:
    """Return the staged-updates DataFrame for the SCD2 MERGE.

    `current` holds the target's rows with is_current = true (None on first load).
    """
    if current is None:
        return candidates.select(*[F.col(k).alias(merge_key_col(k)) for k in keys], *candidates.columns)

    cur = current.select(
        *[F.col(k).alias(f"__cur_{k}") for k in keys],
        F.col("row_hash").alias("__cur_hash"),
        F.col("effective_from").alias("__cur_from"),
    )
    join_cond = [candidates[k] == cur[f"__cur_{k}"] for k in keys]
    joined = candidates.join(cur, join_cond, "left")

    is_new = F.col(f"__cur_{keys[0]}").isNull()
    # Ignore stale changes, e.g. a late re-delivery of a version already superseded.
    is_changed = (
        ~is_new & (F.col("row_hash") != F.col("__cur_hash")) & (F.col("effective_from") > F.col("__cur_from"))
    )

    relevant = joined.filter(is_new | is_changed)

    # Copy 1: merge key set -> matches and closes the current row (or inserts a new key)
    with_key = relevant.select(*[F.col(k).alias(merge_key_col(k)) for k in keys], *candidates.columns)
    # Copy 2: null merge key -> always inserted as the new current version
    new_versions = relevant.filter(~is_new).select(
        *[F.lit(None).cast(candidates.schema[k].dataType).alias(merge_key_col(k)) for k in keys],
        *candidates.columns,
    )
    return with_key.unionByName(new_versions)


def scd2_merge_condition(keys: list[str]) -> str:
    on_keys = " AND ".join(f"t.`{k}` = s.`{merge_key_col(k)}`" for k in keys)
    return f"{on_keys} AND t.is_current = true"
