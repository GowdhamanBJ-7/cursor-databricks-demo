import logging
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs


logger = logging.getLogger(__name__)


def read_job_id(path: str = "deploy/job_id.txt") -> int:
    """
    Read the job_id saved by create_job.py.

    Parameters
    ----------
    path : str, optional
        Path to the job id file, by default "deploy/job_id.txt".

    Returns
    -------
    int
        Parsed job id.
    """

    job_id_str = Path(path).read_text(encoding="utf-8").strip()
    return int(job_id_str)


def schedule_daily_6am_utc(w: WorkspaceClient, job_id: int) -> None:
    """
    Update job schedule to run daily at 6AM UTC using a Quartz cron expression.

    Parameters
    ----------
    w : WorkspaceClient
        Databricks SDK client.
    job_id : int
        Job id to schedule.
    """

    current = w.jobs.get(job_id)
    if not current.settings:
        raise ValueError(f"Job {job_id} has no settings")

    cron = jobs.CronSchedule(quartz_cron_expression="0 0 6 * * ?", timezone_id="UTC")
    new_settings = current.settings
    new_settings.schedule = jobs.Schedule(cron_schedule=cron)

    logger.info("Setting daily schedule 6AM UTC for job_id=%s", job_id)
    w.jobs.reset(job_id=job_id, new_settings=new_settings)


def trigger_run_now(w: WorkspaceClient, job_id: int) -> int:
    """
    Trigger an immediate manual run of the job.

    Parameters
    ----------
    w : WorkspaceClient
        Databricks SDK client.
    job_id : int
        Job id to run.

    Returns
    -------
    int
        Run id.
    """

    logger.info("Triggering run_now for job_id=%s", job_id)
    run = w.jobs.run_now(job_id=job_id)
    run_id = int(run.run_id)
    logger.info("Triggered run_id=%s", run_id)
    print(run_id)
    return run_id


def main() -> int:
    """
    Script entrypoint for scheduling and triggering a run.

    Returns
    -------
    int
        Exit code.
    """

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    w = WorkspaceClient()
    job_id = read_job_id()
    schedule_daily_6am_utc(w, job_id=job_id)
    trigger_run_now(w, job_id=job_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

