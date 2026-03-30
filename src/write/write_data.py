import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.databricks_config import PipelineConfig


logger = logging.getLogger(__name__)


def aggregate_silver_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Aggregate Silver trips into Gold business metrics.

    Metrics are grouped by pickup zone and month.

    Parameters
    ----------
    silver_df : DataFrame
        Silver dataframe.

    Returns
    -------
    DataFrame
        Gold aggregated dataframe.
    """

    zone_col = F.col("pulocation_id").alias("zone_id")
    year_col = F.col("pickup_year").alias("year")
    month_col = F.col("pickup_month").alias("month")

    grouped = silver_df.groupBy(zone_col, year_col, month_col)

    total_revenue = F.sum(F.col("total_amount")).alias("total_revenue")
    total_trips = F.count(F.lit(1)).cast("int").alias("total_trips")
    avg_fare = F.avg(F.col("fare_amount")).alias("avg_fare")
    avg_tip = F.avg(F.col("tip_amount")).alias("avg_tip")

    revenue_per_mile = F.when(
        F.sum(F.col("trip_distance")) > F.lit(0.0),
        F.sum(F.col("total_amount")) / F.sum(F.col("trip_distance")),
    ).otherwise(F.lit(0.0)).alias("revenue_per_mile")

    return grouped.agg(total_trips, total_revenue, avg_fare, avg_tip, revenue_per_mile)


def write_gold_table(gold_df: DataFrame, config: PipelineConfig, mode: str = "overwrite") -> None:
    """
    Write Gold Delta table.

    Parameters
    ----------
    gold_df : DataFrame
        Gold aggregated dataframe.
    config : PipelineConfig
        Pipeline configuration containing the Gold table name.
    mode : str, optional
        Write mode, by default "overwrite".
    """

    logger.info("Writing Gold Delta table: %s (mode=%s)", config.gold_table, mode)
    (
        gold_df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(config.gold_table)
    )


def optimize_gold_table(spark: SparkSession, gold_table: str) -> None:
    """
    Run OPTIMIZE and ZORDER on the Gold table (Databricks runtime).

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    gold_table : str
        Unity Catalog table name.
    """

    logger.info("Optimizing Gold table with ZORDER: %s", gold_table)
    spark.sql(f"OPTIMIZE {gold_table} ZORDER BY (zone_id, year, month)")


def run_gold_write(spark: SparkSession, config: PipelineConfig, optimize: Optional[bool] = True) -> None:
    """
    End-to-end Gold step: read from Silver, aggregate, write to Gold, optimize.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : PipelineConfig
        Pipeline configuration containing the Silver/Gold table names.
    optimize : Optional[bool], optional
        If True, run OPTIMIZE/ZORDER after write, by default True.
    """

    logger.info("Reading Silver Delta table: %s", config.silver_table)
    silver_df = spark.read.table(config.silver_table)
    gold_df = aggregate_silver_to_gold(silver_df)
    write_gold_table(gold_df, config=config)

    if optimize:
        optimize_gold_table(spark, config.gold_table)

