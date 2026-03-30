import logging

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


logger = logging.getLogger(__name__)


def get_bronze_schema() -> StructType:
    """
    Define the Bronze schema for NYC Taxi data.

    All data columns are strings, plus audit columns.

    Returns
    -------
    StructType
        Bronze layer schema.
    """

    logger.info("Building Bronze schema")
    return StructType(
        [
            StructField("vendor_id", StringType(), True),
            StructField("tpep_pickup_datetime", StringType(), True),
            StructField("tpep_dropoff_datetime", StringType(), True),
            StructField("passenger_count", StringType(), True),
            StructField("trip_distance", StringType(), True),
            StructField("rate_code_id", StringType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("pulocation_id", StringType(), True),
            StructField("dolocation_id", StringType(), True),
            StructField("payment_type", StringType(), True),
            StructField("fare_amount", StringType(), True),
            StructField("extra", StringType(), True),
            StructField("mta_tax", StringType(), True),
            StructField("tip_amount", StringType(), True),
            StructField("tolls_amount", StringType(), True),
            StructField("improvement_surcharge", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("congestion_surcharge", StringType(), True),
            # Audit columns
            StructField("_ingested_at", StringType(), True),
            StructField("_source_file", StringType(), True),
        ]
    )


def get_silver_schema() -> StructType:
    """
    Define the Silver schema for NYC Taxi trips.

    The Silver schema applies correct data types and includes derived columns.

    Returns
    -------
    StructType
        Silver layer schema.
    """

    logger.info("Building Silver schema")
    return StructType(
        [
            StructField("vendor_id", StringType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("pickup_year", IntegerType(), True),
            StructField("pickup_month", IntegerType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("pulocation_id", IntegerType(), True),
            StructField("dolocation_id", IntegerType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            # Derived columns
            StructField("trip_duration_minutes", DoubleType(), True),
            StructField("time_of_day", StringType(), True),
            StructField("is_weekend", BooleanType(), True),
            StructField("tip_percentage", DoubleType(), True),
            # Audit columns
            StructField("_ingested_at", TimestampType(), True),
            StructField("_source_file", StringType(), True),
        ]
    )


def get_gold_schema() -> StructType:
    """
    Define the Gold schema for aggregated NYC Taxi business metrics.

    Gold aggregates are at zone and month grain.

    Returns
    -------
    StructType
        Gold layer schema.
    """

    logger.info("Building Gold schema")
    return StructType(
        [
            StructField("zone_id", IntegerType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("total_trips", IntegerType(), True),
            StructField("total_revenue", DoubleType(), True),
            StructField("avg_fare", DoubleType(), True),
            StructField("avg_tip", DoubleType(), True),
            StructField("revenue_per_mile", DoubleType(), True),
        ]
    )

