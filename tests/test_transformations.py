import os
import sys

import pytest
from pyspark.sql import SparkSession

from src.transformation.transform_data import (
    add_derived_columns,
    cast_bronze_to_silver_types,
    deduplicate_trips,
    filter_invalid_records,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Create a local SparkSession for unit tests.

    Returns
    -------
    SparkSession
        Local SparkSession.
    """

    # On some Windows environments, Spark can fail with:
    # "Python worker failed to connect back" due to networking/bind issues.
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    return (
        SparkSession.builder.master("local[2]")
        .appName("nyc-taxi-medallion-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.local.ip", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .config("spark.python.worker.reuse", "false")
        .config("spark.python.worker.connectTimeout", "120s")
        .config("spark.network.timeout", "300s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )


def _bronze_rows():
    return [
        # valid base record
        {
            "vendor_id": "V1",
            "tpep_pickup_datetime": "2024-01-06 06:00:00",  # Saturday
            "tpep_dropoff_datetime": "2024-01-06 06:30:00",
            "passenger_count": "1",
            "trip_distance": "3.0",
            "rate_code_id": "1",
            "store_and_fwd_flag": "N",
            "pulocation_id": "10",
            "dolocation_id": "20",
            "payment_type": "1",
            "fare_amount": "10.0",
            "extra": "0.5",
            "mta_tax": "0.5",
            "tip_amount": "2.0",
            "tolls_amount": "0.0",
            "improvement_surcharge": "0.3",
            "total_amount": "13.3",
            "congestion_surcharge": "0.0",
            "_ingested_at": "2024-01-07 00:00:00",
            "_source_file": "fileA.csv",
        },
        # negative fare
        {
            "vendor_id": "V2",
            "tpep_pickup_datetime": "2024-01-06 10:00:00",
            "tpep_dropoff_datetime": "2024-01-06 10:10:00",
            "passenger_count": "1",
            "trip_distance": "1.0",
            "rate_code_id": "1",
            "store_and_fwd_flag": "N",
            "pulocation_id": "10",
            "dolocation_id": "20",
            "payment_type": "1",
            "fare_amount": "-1.0",
            "extra": "0.0",
            "mta_tax": "0.0",
            "tip_amount": "0.0",
            "tolls_amount": "0.0",
            "improvement_surcharge": "0.0",
            "total_amount": "-1.0",
            "congestion_surcharge": "0.0",
            "_ingested_at": "2024-01-07 00:00:00",
            "_source_file": "fileB.csv",
        },
        # zero passengers
        {
            "vendor_id": "V3",
            "tpep_pickup_datetime": "2024-01-06 12:00:00",
            "tpep_dropoff_datetime": "2024-01-06 12:20:00",
            "passenger_count": "0",
            "trip_distance": "2.0",
            "rate_code_id": "1",
            "store_and_fwd_flag": "N",
            "pulocation_id": "10",
            "dolocation_id": "20",
            "payment_type": "1",
            "fare_amount": "5.0",
            "extra": "0.0",
            "mta_tax": "0.0",
            "tip_amount": "1.0",
            "tolls_amount": "0.0",
            "improvement_surcharge": "0.0",
            "total_amount": "6.0",
            "congestion_surcharge": "0.0",
            "_ingested_at": "2024-01-07 00:00:00",
            "_source_file": "fileC.csv",
        },
        # dropoff before pickup
        {
            "vendor_id": "V4",
            "tpep_pickup_datetime": "2024-01-06 08:00:00",
            "tpep_dropoff_datetime": "2024-01-06 07:59:00",
            "passenger_count": "1",
            "trip_distance": "1.0",
            "rate_code_id": "1",
            "store_and_fwd_flag": "N",
            "pulocation_id": "10",
            "dolocation_id": "20",
            "payment_type": "1",
            "fare_amount": "5.0",
            "extra": "0.0",
            "mta_tax": "0.0",
            "tip_amount": "1.0",
            "tolls_amount": "0.0",
            "improvement_surcharge": "0.0",
            "total_amount": "6.0",
            "congestion_surcharge": "0.0",
            "_ingested_at": "2024-01-07 00:00:00",
            "_source_file": "fileD.csv",
        },
    ]


def test_filters_invalid_records(spark: SparkSession):
    """
    Ensure invalid record filters remove bad rows.
    """

    bronze_df = spark.createDataFrame(_bronze_rows())
    typed = cast_bronze_to_silver_types(bronze_df)
    filtered = filter_invalid_records(typed)
    assert filtered.count() == 1


def test_trip_duration_minutes_time_of_day_is_weekend_tip_percentage(spark: SparkSession):
    """
    Validate derived column logic.
    """

    bronze_df = spark.createDataFrame([_bronze_rows()[0]])
    typed = cast_bronze_to_silver_types(bronze_df)
    filtered = filter_invalid_records(typed)
    derived = add_derived_columns(filtered)

    row = derived.select(
        "trip_duration_minutes",
        "time_of_day",
        "is_weekend",
        "tip_percentage",
    ).collect()[0]

    assert abs(row["trip_duration_minutes"] - 30.0) < 1e-6
    assert row["time_of_day"] == "morning"
    assert row["is_weekend"] is True
    assert abs(row["tip_percentage"] - 20.0) < 1e-6


def test_deduplication(spark: SparkSession):
    """
    Ensure deduplication drops exact duplicates on the configured key columns.
    """

    base = _bronze_rows()[0]
    bronze_df = spark.createDataFrame([base, base])
    typed = cast_bronze_to_silver_types(bronze_df)
    filtered = filter_invalid_records(typed)
    derived = add_derived_columns(filtered)
    deduped = deduplicate_trips(derived)
    assert deduped.count() == 1

