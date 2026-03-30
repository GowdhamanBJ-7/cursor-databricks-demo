import logging
import os
from dataclasses import dataclass
from typing import Dict

from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """
    Configuration for the Medallion pipeline Unity Catalog tables.

    All table names must follow the catalog.schema.table convention.
    """

    bronze_table: str
    silver_table: str
    gold_table: str


def get_pipeline_config() -> PipelineConfig:
    """
    Build the PipelineConfig from environment variables.

    Returns
    -------
    PipelineConfig
        Dataclass instance populated from environment variables.
    """

    bronze_table = os.environ.get("PIPELINE_BRONZE_TABLE", "main.nyc_taxi.bronze_trips")
    silver_table = os.environ.get("PIPELINE_SILVER_TABLE", "main.nyc_taxi.silver_trips")
    gold_table = os.environ.get("PIPELINE_GOLD_TABLE", "main.nyc_taxi.gold_metrics")

    logger.info(
        "Using pipeline tables - bronze=%s, silver=%s, gold=%s",
        bronze_table,
        silver_table,
        gold_table,
    )

    return PipelineConfig(
        bronze_table=bronze_table,
        silver_table=silver_table,
        gold_table=gold_table,
    )


def _spark_extra_configs_from_env() -> Dict[str, str]:
    """
    Build additional Spark configuration from environment variables.

    Environment variables with the prefix ``SPARK_CONFIG__`` will be translated to
    Spark configuration entries. For example, ``SPARK_CONFIG__spark_sql_shuffle_partitions``
    becomes ``spark.sql.shuffle.partitions``.

    Returns
    -------
    dict
        Mapping of Spark configuration keys to values.
    """

    configs: Dict[str, str] = {}
    prefix = "SPARK_CONFIG__"
    for key, value in os.environ.items():
        if key.startswith(prefix):
            spark_key = key[len(prefix) :].replace("__", ".").replace("_", ".")
            configs[spark_key] = value
    return configs


def get_spark(app_name: str = "nyc-taxi-medallion") -> SparkSession:
    """
    Create or retrieve a SparkSession configured for Databricks with Delta support.

    Parameters
    ----------
    app_name : str, optional
        Spark application name, by default "nyc-taxi-medallion".

    Returns
    -------
    SparkSession
        Active SparkSession.
    """

    logger.info("Creating SparkSession for app: %s", app_name)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "200"))
        .config("spark.databricks.io.cache.enabled", "true")
    )

    extra_configs = _spark_extra_configs_from_env()
    for k, v in extra_configs.items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    logger.info("SparkSession created with master: %s", spark.sparkContext.master)
    return spark

