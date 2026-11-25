"""
Bronze layer: land source files as-is, plus ingestion metadata.

Bronze keeps the raw values (CSV and JSON as strings, Parquet with its own
types) so any batch can be replayed into silver after a logic fix without
going back to the source system.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse.config import EntityConfig

CORRUPT_COL = "_corrupt_record"


def _raw_string_schema(entity: EntityConfig) -> StructType:
    fields = []
    if entity.cdc_op_column:
        fields.append(StructField(entity.cdc_op_column, StringType()))
    fields += [StructField(c, StringType()) for c in entity.schema]
    if entity.format == "json":
        fields.append(StructField(CORRUPT_COL, StringType()))
    return StructType(fields)


def read_landing(spark: SparkSession, entity: EntityConfig, path: str) -> DataFrame:
    """Read one landing batch for an entity."""
    reader = spark.read.options(**entity.read_options)
    if entity.format in ("csv", "json"):
        # Explicit all-string schema: no type inference surprises, and malformed
        # JSON lands in _corrupt_record instead of failing the job.
        return reader.schema(_raw_string_schema(entity)).format(entity.format).load(path)
    if entity.format == "parquet":
        return reader.parquet(path)
    raise ValueError(f"Unsupported format: {entity.format}")


def add_ingestion_metadata(df: DataFrame, batch_date: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_batch_date", F.lit(batch_date).cast("date"))
    )
