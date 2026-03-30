import logging
import os
from pathlib import Path
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute


logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    """
    Read an integer environment variable with a default.

    Parameters
    ----------
    name : str
        Environment variable name.
    default : int
        Default if env var not set.

    Returns
    -------
    int
        Parsed integer value.
    """

    value = os.environ.get(name)
    return int(value) if value is not None and value != "" else default


def _build_job_settings(job_name: str) -> jobs.JobSettings:
    """
    Build the Databricks Workflow Job settings with 3 tasks (bronze, silver, gold).

    Parameters
    ----------
    job_name : str
        Job name.

    Returns
    -------
    jobs.JobSettings
        Job settings object.
    """

    base_parameters = os.environ.get("DATABRICKS_JOB_PIPELINE_PARAMS", "")
    shared_params = [p for p in base_parameters.split() if p.strip()]

    # Serverless workflows use environments + environment_key on tasks.
    environment_key = "default"
    environments = [
        jobs.JobEnvironment(
            environment_key=environment_key,
            spec=compute.Environment(client="1"),
        )
    ]

    pipeline_path = os.environ.get("DATABRICKS_PIPELINE_WORKSPACE_PATH", "pipeline/etl_pipeline.py")
    python_file = pipeline_path

    bronze_task = jobs.Task(
        task_key="bronze",
        environment_key=environment_key,
        spark_python_task=jobs.SparkPythonTask(
            python_file=python_file,
            parameters=["--stages", "bronze", *shared_params],
        ),
    )

    silver_task = jobs.Task(
        task_key="silver",
        depends_on=[jobs.TaskDependency(task_key="bronze")],
        environment_key=environment_key,
        spark_python_task=jobs.SparkPythonTask(
            python_file=python_file,
            parameters=["--stages", "silver", *shared_params],
        ),
    )

    gold_task = jobs.Task(
        task_key="gold",
        depends_on=[jobs.TaskDependency(task_key="silver")],
        environment_key=environment_key,
        spark_python_task=jobs.SparkPythonTask(
            python_file=python_file,
            parameters=["--stages", "gold", *shared_params],
        ),
    )

    return jobs.JobSettings(
        name=job_name,
        environments=environments,
        performance_target=jobs.PerformanceTarget.PERFORMANCE_OPTIMIZED,
        tasks=[bronze_task, silver_task, gold_task],
        max_concurrent_runs=1,
        timeout_seconds=_env_int("DATABRICKS_JOB_TIMEOUT_SECONDS", 0),
    )


def _find_existing_job_id(w: WorkspaceClient, job_name: str) -> Optional[int]:
    """
    Find an existing job by name.

    Parameters
    ----------
    w : WorkspaceClient
        Databricks SDK client.
    job_name : str
        Job name.

    Returns
    -------
    Optional[int]
        Job id if found, else None.
    """

    for j in w.jobs.list(name=job_name):
        if j.settings and j.settings.name == job_name:
            return j.job_id
    return None


def create_or_update_job(w: WorkspaceClient, job_name: str) -> int:
    """
    Create a Databricks Workflow Job or update it if it already exists (idempotent).

    Parameters
    ----------
    w : WorkspaceClient
        Databricks SDK client.
    job_name : str
        Job name.

    Returns
    -------
    int
        Job id.
    """

    settings = _build_job_settings(job_name)
    existing_job_id = _find_existing_job_id(w, job_name)

    if existing_job_id is None:
        logger.info("Creating new job: %s", job_name)
        created = w.jobs.create(
            name=settings.name,
            environments=settings.environments,
            performance_target=settings.performance_target,
            tasks=settings.tasks,
            max_concurrent_runs=settings.max_concurrent_runs,
            timeout_seconds=settings.timeout_seconds,
        )
        job_id = int(created.job_id)
        logger.info("Created job_id=%s", job_id)
        return job_id

    logger.info("Updating existing job: %s (job_id=%s)", job_name, existing_job_id)
    w.jobs.reset(job_id=existing_job_id, new_settings=settings)
    return int(existing_job_id)


def save_job_id(job_id: int, output_path: str = "deploy/job_id.txt") -> None:
    """
    Save the job id to a text file for later scheduling.

    Parameters
    ----------
    job_id : int
        Databricks job id.
    output_path : str, optional
        Output path, by default "deploy/job_id.txt".
    """

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{job_id}\n", encoding="utf-8")
    logger.info("Saved job_id=%s to %s", job_id, str(path))


def main() -> int:
    """
    Script entrypoint for creating/updating the job.

    Credentials are sourced by the Databricks SDK from environment variables.

    Returns
    -------
    int
        Exit code.
    """

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    job_name = os.environ.get("DATABRICKS_JOB_NAME", "nyc-taxi-medallion-etl").strip() or "nyc-taxi-medallion-etl"
    w = WorkspaceClient()
    job_id = create_or_update_job(w, job_name=job_name)
    save_job_id(job_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

