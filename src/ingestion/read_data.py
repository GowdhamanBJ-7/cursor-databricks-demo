import logging
import os
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from config.databricks_config import PipelineConfig
from config.schema_config import get_bronze_schema


logger = logging.getLogger(__name__)


def read_nyc_taxi_csv(
    spark: SparkSession,
    source_path: str = "",
    csv_glob: str = "",
) -> DataFrame:
    """
    Read the NYC Taxi CSV dataset from the Databricks datasets path.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    source_path : str, optional
        Base path of the NYC Taxi dataset, by default "/databricks-datasets/nyctaxi/".
    csv_glob : str, optional
        Glob pattern below the source path, by default "**/*.csv".

    Returns
    -------
    DataFrame
        Raw CSV dataset with headers.
    """

    configured_source = os.environ.get("NYC_TAXI_SOURCE_PATH", "").strip()
    if source_path and csv_glob:
        candidates = [f"{source_path.rstrip('/')}/{csv_glob}"]
    elif configured_source:
        candidates = [configured_source]
    else:
        candidates = [
            "/databricks-datasets/nyctaxi/tripdata/yellow/*.csv",
            "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_*.csv",
            "/databricks-datasets/nyctaxi/tables/yellow_tripdata.csv",
        ]

    # Read only source data columns from Bronze schema (exclude audit columns).
    bronze_schema = get_bronze_schema()
    input_schema = T.StructType([f for f in bronze_schema.fields if not f.name.startswith("_")])
    last_error: Optional[Exception] = None
    for path in candidates:
        try:
            logger.info("Reading NYC Taxi CSV from: %s", path)
            df = (
                spark.read.option("header", True)
                .option("inferSchema", False)
                .option("mode", "PERMISSIVE")
                .schema(input_schema)
                .csv(path)
            )
            # Trigger lightweight analysis to fail fast on invalid paths.
            df.limit(1).count()
            return df
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if "PATH_NOT_FOUND" in str(exc):
                logger.warning("Source path not found, trying next candidate: %s", path)
                continue
            raise

    raise ValueError(
        "NYC Taxi source path not found in this workspace. "
        "Set NYC_TAXI_SOURCE_PATH environment variable to a valid CSV path. "
        f"Tried: {candidates}. Last error: {last_error}"
    )


def add_bronze_audit_and_partitions(df: DataFrame) -> DataFrame:
    """
    Add Bronze audit columns and partition columns (year, month).

    Audit columns:
    - _ingested_at: current timestamp (string)
    - _source_file: input_file_name()

    Partition columns:
    - pickup_year
    - pickup_month

    Parameters
    ----------
    df : DataFrame
        Raw dataframe read from CSV.

    Returns
    -------
    DataFrame
        Dataframe with audit + partition columns.
    """

    pickup_ts = F.to_timestamp(F.col("tpep_pickup_datetime"))
    with_partitions = (
        df.withColumn("_ingested_at", F.current_timestamp().cast("string"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("pickup_year", F.year(pickup_ts))
        .withColumn("pickup_month", F.month(pickup_ts))
    )
    return with_partitions


def write_bronze_table(
    df: DataFrame,
    config: PipelineConfig,
    mode: str = "append",
    optimize_write: Optional[bool] = None,
) -> None:
    """
    Write the Bronze Delta table partitioned by year and month.

    Parameters
    ----------
    df : DataFrame
        Bronze dataframe.
    config : PipelineConfig
        Pipeline configuration containing the Bronze table name.
    mode : str, optional
        Write mode, by default "append".
    optimize_write : Optional[bool], optional
        Whether to enable optimized writes; if None, uses Spark defaults.
    """

    writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")
    if optimize_write is not None:
        writer = writer.option("delta.autoOptimize.optimizeWrite", str(optimize_write).lower())

    logger.info(
        "Writing Bronze Delta table: %s (mode=%s, partitionBy=pickup_year,pickup_month)",
        config.bronze_table,
        mode,
    )
    (
        writer.partitionBy("pickup_year", "pickup_month")
        .saveAsTable(config.bronze_table)
    )


def ingest_to_bronze(spark: SparkSession, config: PipelineConfig) -> None:
    """
    End-to-end ingestion from Databricks datasets to Bronze Delta table.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : PipelineConfig
        Pipeline configuration containing the Bronze table name.
    """

    raw = read_nyc_taxi_csv(spark)
    bronze = add_bronze_audit_and_partitions(raw)
    write_bronze_table(bronze, config=config)

