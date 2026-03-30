import logging
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from config.databricks_config import PipelineConfig


logger = logging.getLogger(__name__)


def _parse_pickup_dropoff(df: DataFrame) -> DataFrame:
    """
    Parse pickup and dropoff timestamps from Bronze string columns.

    Parameters
    ----------
    df : DataFrame
        Bronze dataframe.

    Returns
    -------
    DataFrame
        Dataframe with parsed timestamp columns.
    """

    pickup_ts = F.to_timestamp(F.col("tpep_pickup_datetime"))
    dropoff_ts = F.to_timestamp(F.col("tpep_dropoff_datetime"))

    return (
        df.withColumn("pickup_datetime", pickup_ts)
        .withColumn("dropoff_datetime", dropoff_ts)
        .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
    )


def cast_bronze_to_silver_types(df: DataFrame) -> DataFrame:
    """
    Cast Bronze string columns into correct Silver types.

    Parameters
    ----------
    df : DataFrame
        Bronze dataframe containing string columns.

    Returns
    -------
    DataFrame
        Dataframe with correct column types and canonical names.
    """

    typed = _parse_pickup_dropoff(df)

    def to_int(col_name: str) -> F.Column:
        return F.col(col_name).cast(T.IntegerType())

    def to_double(col_name: str) -> F.Column:
        return F.col(col_name).cast(T.DoubleType())

    typed = (
        typed.withColumn("passenger_count", to_int("passenger_count"))
        .withColumn("trip_distance", to_double("trip_distance"))
        .withColumn("rate_code_id", to_int("rate_code_id"))
        .withColumn("pulocation_id", to_int("pulocation_id"))
        .withColumn("dolocation_id", to_int("dolocation_id"))
        .withColumn("payment_type", to_int("payment_type"))
        .withColumn("fare_amount", to_double("fare_amount"))
        .withColumn("extra", to_double("extra"))
        .withColumn("mta_tax", to_double("mta_tax"))
        .withColumn("tip_amount", to_double("tip_amount"))
        .withColumn("tolls_amount", to_double("tolls_amount"))
        .withColumn("improvement_surcharge", to_double("improvement_surcharge"))
        .withColumn("total_amount", to_double("total_amount"))
        .withColumn("congestion_surcharge", to_double("congestion_surcharge"))
        .withColumn("_ingested_at", F.to_timestamp(F.col("_ingested_at")))
    )

    typed = typed.withColumn("pickup_year", F.year(F.col("pickup_datetime"))).withColumn(
        "pickup_month", F.month(F.col("pickup_datetime"))
    )

    return typed


def filter_invalid_records(df: DataFrame) -> DataFrame:
    """
    Filter invalid records.

    Rules:
    - fare_amount must be >= 0
    - passenger_count must be > 0
    - dropoff_datetime must be >= pickup_datetime

    Parameters
    ----------
    df : DataFrame
        Typed Silver-like dataframe.

    Returns
    -------
    DataFrame
        Filtered dataframe.
    """

    return df.filter(
        (F.col("fare_amount") >= F.lit(0.0))
        & (F.col("passenger_count") > F.lit(0))
        & (F.col("dropoff_datetime") >= F.col("pickup_datetime"))
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Add derived columns for the Silver layer.

    Derived columns:
    - trip_duration_minutes
    - time_of_day (night/morning/afternoon/evening by pickup hour)
    - is_weekend
    - tip_percentage

    Parameters
    ----------
    df : DataFrame
        Validated dataframe.

    Returns
    -------
    DataFrame
        Dataframe with derived columns.
    """

    duration_minutes = (
        (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long"))
        / F.lit(60.0)
    )

    pickup_hour = F.hour(F.col("pickup_datetime"))
    time_of_day = (
        F.when((pickup_hour >= 5) & (pickup_hour <= 11), F.lit("morning"))
        .when((pickup_hour >= 12) & (pickup_hour <= 16), F.lit("afternoon"))
        .when((pickup_hour >= 17) & (pickup_hour <= 20), F.lit("evening"))
        .otherwise(F.lit("night"))
    )

    day_of_week = F.dayofweek(F.col("pickup_datetime"))  # 1=Sunday,7=Saturday
    is_weekend = day_of_week.isin([1, 7])

    tip_percentage = F.when(
        F.col("fare_amount") > F.lit(0.0),
        (F.col("tip_amount") / F.col("fare_amount")) * F.lit(100.0),
    ).otherwise(F.lit(0.0))

    return (
        df.withColumn("trip_duration_minutes", duration_minutes)
        .withColumn("time_of_day", time_of_day)
        .withColumn("is_weekend", is_weekend.cast(T.BooleanType()))
        .withColumn("tip_percentage", tip_percentage)
    )


def deduplicate_trips(df: DataFrame) -> DataFrame:
    """
    Deduplicate trips using a stable subset of columns.

    Parameters
    ----------
    df : DataFrame
        Silver-like dataframe.

    Returns
    -------
    DataFrame
        Dataframe with duplicates removed.
    """

    dedup_cols = [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pulocation_id",
        "dolocation_id",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]
    existing = [c for c in dedup_cols if c in df.columns]
    return df.dropDuplicates(existing)


def transform_bronze_to_silver(bronze_df: DataFrame) -> Tuple[DataFrame, int, int]:
    """
    Transform Bronze dataframe into Silver dataframe.

    Parameters
    ----------
    bronze_df : DataFrame
        Dataframe read from Bronze Delta table.

    Returns
    -------
    Tuple[DataFrame, int, int]
        (silver_df, input_count, output_count)
    """

    input_count = bronze_df.count()
    typed = cast_bronze_to_silver_types(bronze_df)
    filtered = filter_invalid_records(typed)
    derived = add_derived_columns(filtered)
    silver = deduplicate_trips(derived)
    output_count = silver.count()
    logger.info("Bronze->Silver counts: input=%s output=%s", input_count, output_count)
    return silver, input_count, output_count


def write_silver_table(silver_df: DataFrame, config: PipelineConfig, mode: str = "overwrite") -> None:
    """
    Write Silver Delta table.

    Parameters
    ----------
    silver_df : DataFrame
        Silver dataframe.
    config : PipelineConfig
        Pipeline configuration containing the Silver table name.
    mode : str, optional
        Write mode, by default "overwrite".
    """

    logger.info("Writing Silver Delta table: %s (mode=%s)", config.silver_table, mode)
    (
        silver_df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(config.silver_table)
    )


def run_silver_transformation(spark: SparkSession, config: PipelineConfig) -> None:
    """
    End-to-end Silver transformation: read from Bronze, transform, write to Silver.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : PipelineConfig
        Pipeline configuration containing the Bronze/Silver table names.
    """

    logger.info("Reading Bronze Delta table: %s", config.bronze_table)
    bronze_df = spark.read.table(config.bronze_table)
    silver_df, _, _ = transform_bronze_to_silver(bronze_df)
    write_silver_table(silver_df, config=config)

