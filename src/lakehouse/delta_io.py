"""
Delta Lake read/write operations. Every write is idempotent, so a failed or
repeated run of the same batch never duplicates data.

    bronze      overwrite_partition  (replaceWhere on _batch_date)
    silver      merge_scd1 / merge_insert_only / merge_scd2
    gold        overwrite_table + optimize_zorder
    audit       append
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from lakehouse.scd2 import merge_key_col, scd2_merge_condition


def table_exists(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def read(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def overwrite_partition(df: DataFrame, path: str, partition_col: str, value: str) -> None:
    """Replace just one partition. Re-running a batch replaces it rather than appending twice."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{partition_col} = '{value}'")
        .option("mergeSchema", "true")
        .partitionBy(partition_col)
        .save(path)
    )


def overwrite_table(df: DataFrame, path: str, partition_by: list[str] | None = None) -> None:
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.save(path)


def append(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)


def _key_condition(keys: list[str], partition_by: list[str] | None = None) -> str:
    # Partition columns in the ON clause let Delta prune files during the MERGE.
    cols = [*keys, *(partition_by or [])]
    return " AND ".join(f"t.`{c}` = s.`{c}`" for c in cols)


def merge_scd1(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: list[str],
    sequence_column: str,
    partition_by: list[str] | None = None,
) -> None:
    """Upsert latest state. Older late-arriving records never overwrite newer ones."""
    if not table_exists(spark, path):
        overwrite_table(df, path, partition_by)
        return
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), _key_condition(keys, partition_by))
        .whenMatchedUpdateAll(condition=f"s.`{sequence_column}` >= t.`{sequence_column}`")
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_insert_only(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: list[str],
    partition_by: list[str] | None = None,
) -> None:
    """Insert rows whose business key is not already present (immutable events)."""
    if not table_exists(spark, path):
        overwrite_table(df, path, partition_by)
        return
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), _key_condition(keys, partition_by))
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_scd2(spark: SparkSession, staged: DataFrame, path: str, keys: list[str]) -> None:
    """Apply staged SCD2 updates (see lakehouse.scd2) in one atomic MERGE."""
    target_cols = [c for c in staged.columns if not c.startswith("__mk_")]
    if not table_exists(spark, path):
        first_load = staged.filter(" AND ".join(f"`{merge_key_col(k)}` IS NOT NULL" for k in keys))
        overwrite_table(first_load.select(*target_cols), path)
        return
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(staged.alias("s"), scd2_merge_condition(keys))
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={"is_current": "false", "effective_to": "s.effective_from"},
        )
        .whenNotMatchedInsert(values={c: f"s.`{c}`" for c in target_cols})
        .execute()
    )


def optimize_zorder(spark: SparkSession, path: str, zorder_by: list[str]) -> None:
    """Compact small files and co-locate rows by the given columns for data skipping."""
    DeltaTable.forPath(spark, path).optimize().executeZOrderBy(*zorder_by)
