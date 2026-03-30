# NYC Taxi Medallion (Databricks)

A complete Databricks Data Engineering project implementing **Medallion Architecture** (Bronze → Silver → Gold) using the **NYC Taxi dataset** from Databricks datasets.

### Architecture

```
                 /databricks-datasets/nyctaxi/ (CSV)
                              |
                              v
   +--------------------+  ingestion  +-----------------------+
   |      Bronze        | ----------> |        Silver         |
   | raw strings + audit|             | typed + validated +   |
   | Delta table        |             | derived + deduped     |
   +--------------------+             +-----------------------+
              |                                  |
              |                                  v
              |                           +------------------+
              |                           |       Gold       |
              +-------------------------> | aggregated metrics|
                                          | Delta table       |
                                          +------------------+
```

### Project layout

- `config/`: Spark + pipeline table config, explicit schemas
- `src/ingestion/`: read from Databricks datasets → Bronze Delta
- `src/transformation/`: Bronze → Silver (types, filters, derived, dedupe)
- `src/write/`: Silver → Gold (aggregations + OPTIMIZE/ZORDER)
- `pipeline/etl_pipeline.py`: orchestration CLI (`--stages bronze,silver,gold`)
- `deploy/`: Databricks SDK scripts to create/update and schedule the workflow job
- `.github/workflows/`: CI/CD pipeline
- `tests/`: local Spark unit tests (no Databricks cluster needed)

### Environment variables

All credentials are read from environment variables only (no hardcoding).

| Variable | Purpose | Example |
|---|---|---|
| `DATABRICKS_HOST` | Databricks workspace URL | `https://dbc-xxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | PAT token for Databricks SDK | `dapi...` |
| `DATABRICKS_JOB_NAME` | Workflow job name | `nyc-taxi-medallion-etl` |
| `DATABRICKS_GIT_URL` | Git repo URL for Databricks job source (recommended) | `https://github.com/org/repo.git` |
| `DATABRICKS_GIT_PROVIDER` | Databricks git provider value | `gitHub` |
| `DATABRICKS_GIT_BRANCH` | Git branch for job source (if tag/commit not set) | `main` |
| `DATABRICKS_GIT_TAG` | Optional git tag for job source | `v1.0.0` |
| `DATABRICKS_GIT_COMMIT` | Optional git commit SHA for job source | `abc123...` |
| `DATABRICKS_GIT_PYTHON_FILE` | Python file path inside git repo | `pipeline/etl_pipeline.py` |
| `DATABRICKS_PIPELINE_WORKSPACE_PATH` | Absolute workspace fallback if not using git source | `/Workspace/Users/.../pipeline/etl_pipeline.py` |
| `DATABRICKS_SERVERLESS_CLIENT` | Serverless REPL channel/client version | `2` |
| `PIPELINE_BRONZE_TABLE` | Bronze UC table | `catalog.schema.bronze_trips` |
| `PIPELINE_SILVER_TABLE` | Silver UC table | `catalog.schema.silver_trips` |
| `PIPELINE_GOLD_TABLE` | Gold UC table | `catalog.schema.gold_metrics` |
| `SPARK_SQL_SHUFFLE_PARTITIONS` | Spark shuffle partitions | `200` |
| `SPARK_CONFIG__...` | Extra Spark conf overrides | `SPARK_CONFIG__spark_sql_adaptive_enabled=true` |

### Setup

1. Create a Python environment.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Set environment variables (locally) or GitHub Secrets (for CI/CD).

3. (Databricks) Ensure your Unity Catalog catalog and schema exist (example `main.nyc_taxi`).

### Run manually

Run all stages:

```bash
python pipeline/etl_pipeline.py --stages bronze,silver,gold
```

Run just one stage:

```bash
python pipeline/etl_pipeline.py --stages silver
```

### CI/CD (GitHub Actions)

- **Pull requests to `main`**: runs `pytest` only.
- **Push to `main`**: runs `pytest`, then (only if tests pass) runs:
  - `python deploy/create_job.py`
  - `python deploy/schedule_job.py`

### GitHub Secrets to configure

Configure these secrets in your repository settings:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_JOB_NAME` (optional; defaults to `nyc-taxi-medallion-etl`)
- `DATABRICKS_GIT_URL` (recommended for CI deploy)
- `DATABRICKS_GIT_PROVIDER` (e.g. `gitHub`)
- `DATABRICKS_GIT_BRANCH` (e.g. `main`)
- `DATABRICKS_GIT_PYTHON_FILE` (optional, defaults to `pipeline/etl_pipeline.py`)
- `PIPELINE_BRONZE_TABLE`
- `PIPELINE_SILVER_TABLE`
- `PIPELINE_GOLD_TABLE`

