import argparse
import logging
import sys
import time
from typing import Dict, List

from config.databricks_config import get_pipeline_config, get_spark
from src.ingestion.read_data import ingest_to_bronze
from src.transformation.transform_data import run_silver_transformation
from src.write.write_data import run_gold_write


logger = logging.getLogger(__name__)


def _parse_args(argv: List[str]) -> argparse.Namespace:
    """
    Parse CLI arguments for the ETL pipeline.

    Parameters
    ----------
    argv : List[str]
        Command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed arguments.
    """

    parser = argparse.ArgumentParser(description="NYC Taxi Medallion ETL Pipeline (Bronze->Silver->Gold)")
    parser.add_argument(
        "--stages",
        type=str,
        default="bronze,silver,gold",
        help="Comma-separated stages to run: bronze,silver,gold",
    )
    return parser.parse_args(argv)


def _normalize_stages(stages_arg: str) -> List[str]:
    """
    Normalize the --stages argument to a list.

    Parameters
    ----------
    stages_arg : str
        Comma-separated list.

    Returns
    -------
    List[str]
        Normalized list of stages.
    """

    stages = [s.strip().lower() for s in stages_arg.split(",") if s.strip()]
    allowed = {"bronze", "silver", "gold"}
    unknown = [s for s in stages if s not in allowed]
    if unknown:
        raise ValueError(f"Unknown stages: {unknown}. Allowed: {sorted(allowed)}")
    return stages


def run_pipeline(stages: List[str]) -> Dict[str, bool]:
    """
    Run the pipeline stages in sequence and track success per stage.

    Parameters
    ----------
    stages : List[str]
        Stages to run, in order.

    Returns
    -------
    Dict[str, bool]
        Mapping from stage name to success boolean.
    """

    spark = get_spark(app_name="nyc-taxi-medallion-etl")
    config = get_pipeline_config()

    results: Dict[str, bool] = {}
    timings: Dict[str, float] = {}

    for stage in stages:
        start = time.perf_counter()
        try:
            logger.info("Starting stage: %s", stage)
            if stage == "bronze":
                ingest_to_bronze(spark, config)
            elif stage == "silver":
                run_silver_transformation(spark, config)
            elif stage == "gold":
                run_gold_write(spark, config, optimize=True)
            else:
                raise ValueError(f"Unsupported stage: {stage}")
            results[stage] = True
            logger.info("Stage succeeded: %s", stage)
        except Exception:
            logger.exception("Stage failed: %s", stage)
            results[stage] = False
        finally:
            timings[stage] = time.perf_counter() - start
            logger.info("Stage timing: %s took %.2fs", stage, timings[stage])

    passed = all(results.get(s, False) for s in stages)
    logger.info("Pipeline summary: %s", "PASS" if passed else "FAIL")
    for s in stages:
        logger.info(" - %s: %s (%.2fs)", s, "PASS" if results.get(s) else "FAIL", timings.get(s, 0.0))

    return results


def main(argv: List[str]) -> int:
    """
    CLI entrypoint.

    Parameters
    ----------
    argv : List[str]
        Command-line arguments excluding the program name.

    Returns
    -------
    int
        Process exit code (0 on success).
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    args = _parse_args(argv)
    stages = _normalize_stages(args.stages)
    results = run_pipeline(stages)
    return 0 if all(results.get(s, False) for s in stages) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

