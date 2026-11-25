"""SparkSession factory: reuses the Databricks session, or builds a local Delta-enabled one."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "lakehouse-medallion", enable_delta: bool = True) -> SparkSession:
    # On Databricks a session already exists and Delta is built in.
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return SparkSession.builder.getOrCreate()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if enable_delta:
        from delta import configure_spark_with_delta_pip

        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        )
        builder = configure_spark_with_delta_pip(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
