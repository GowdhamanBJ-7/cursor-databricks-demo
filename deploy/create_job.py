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


def _derive_git_url_from_github_actions() -> str:
    """
    Derive git URL from GitHub Actions environment when explicit value is missing.

    Returns
    -------
    str
        Derived git URL or empty string if not derivable.
    """

    server = os.environ.get("GITHUB_SERVER_URL", "").strip()
    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if server and repo:
        return f"{server}/{repo}.git"
    return ""


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

    # Two supported execution modes for spark_python_task:
    # 1) GIT source (recommended for CI/CD): provide DATABRICKS_GIT_URL (+ provider, branch/tag/commit)
    # 2) WORKSPACE source: provide absolute DATABRICKS_PIPELINE_WORKSPACE_PATH (e.g. /Workspace/...)
    git_url = os.environ.get("DATABRICKS_GIT_URL", "").strip() or _derive_git_url_from_github_actions()
    git_provider_raw = os.environ.get("DATABRICKS_GIT_PROVIDER", "gitHub").strip()
    git_branch = os.environ.get("DATABRICKS_GIT_BRANCH", "").strip() or os.environ.get("GITHUB_REF_NAME", "").strip()
    git_tag = os.environ.get("DATABRICKS_GIT_TAG", "").strip()
    git_commit = os.environ.get("DATABRICKS_GIT_COMMIT", "").strip()

    if git_url:
        try:
            git_provider = jobs.GitProvider(git_provider_raw)
        except ValueError as exc:
            valid = ", ".join(g.value for g in jobs.GitProvider)
            raise ValueError(f"Invalid DATABRICKS_GIT_PROVIDER={git_provider_raw!r}. Use one of: {valid}") from exc

        git_source = jobs.GitSource(git_url=git_url, git_provider=git_provider)
        if git_commit:
            git_source.git_commit = git_commit
        elif git_tag:
            git_source.git_tag = git_tag
        else:
            git_source.git_branch = git_branch or "main"

        python_file = os.environ.get("DATABRICKS_GIT_PYTHON_FILE", "pipeline/etl_pipeline.py").strip()
        task_source = jobs.Source.GIT
    else:
        git_source = None
        python_file = os.environ.get("DATABRICKS_PIPELINE_WORKSPACE_PATH", "").strip()
        if not python_file:
            raise ValueError(
                "Set DATABRICKS_GIT_URL for Git-based jobs or DATABRICKS_PIPELINE_WORKSPACE_PATH for workspace-based jobs."
            )
        if not python_file.startswith("/"):
            raise ValueError(
                f"DATABRICKS_PIPELINE_WORKSPACE_PATH must be an absolute workspace path, got: {python_file!r}"
            )
        task_source = jobs.Source.WORKSPACE

    bronze_task = jobs.Task(
        task_key="bronze",
        environment_key=environment_key,
        spark_python_task=jobs.SparkPythonTask(
            python_file=python_file,
            parameters=["--stages", "bronze", *shared_params],
            source=task_source,
        ),
    )

    silver_task = jobs.Task(
        task_key="silver",
        depends_on=[jobs.TaskDependency(task_key="bronze")],
        environment_key=environment_key,
        spark_python_task=jobs.SparkPythonTask(
            python_file=python_file,
            parameters=["--stages", "silver", *shared_params],
            source=task_source,
        ),
    )

    gold_task = jobs.Task(
        task_key="gold",
        depends_on=[jobs.TaskDependency(task_key="silver")],
        environment_key=environment_key,
        spark_python_task=jobs.SparkPythonTask(
            python_file=python_file,
            parameters=["--stages", "gold", *shared_params],
            source=task_source,
        ),
    )

    return jobs.JobSettings(
        name=job_name,
        git_source=git_source,
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
            git_source=settings.git_source,
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

