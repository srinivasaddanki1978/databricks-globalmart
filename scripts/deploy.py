#!/usr/bin/env python3
"""
GlobalMart Data Intelligence Platform -- Databricks Deployment Script

Uses the Databricks Python SDK to:
1. Authenticate to the workspace
2. Create Unity Catalog structure (catalog, schemas, volume)
3. Upload source data files to the managed volume
4. Upload DLT notebooks (01-04) and GenAI notebooks (05-08)
5. Create the DLT pipeline (Bronze -> Silver -> Gold -> Metrics)
6. Create the end-to-end Workflow (DLT pipeline + GenAI notebooks 05-08)
7. Print summary with all URLs and next steps

After the Workflow job completes successfully, create the Genie Space:
    python scripts/setup_genie.py

Usage:
    python scripts/deploy.py
    python scripts/deploy.py --skip-data       # Skip data file upload
    python scripts/deploy.py --skip-pipeline   # Skip DLT pipeline creation
    python scripts/deploy.py --skip-job        # Skip Workflow creation

Prerequisites:
    export DATABRICKS_HOST="https://<your-workspace>.cloud.databricks.com"
    export DATABRICKS_TOKEN="dapi..."
    pip install databricks-sdk
"""

import argparse
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service import pipelines, jobs, compute
from databricks.sdk.service.workspace import ImportFormat, Language


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG       = "globalmart"
SCHEMAS       = ["bronze", "silver", "gold", "metrics"]
VOLUME_SCHEMA = "bronze"
VOLUME_NAME   = "source_data"
VOLUME_PATH   = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME_NAME}"

PROJECT_ROOT  = Path(__file__).resolve().parent.parent
DATA_DIR      = PROJECT_ROOT / "data"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"

DLT_NOTEBOOKS = [
    "01_bronze_ingestion",
    "02_silver_harmonization",
    "03_gold_dimensional",
    "04_metrics_aggregation",
]
GENAI_NOTEBOOKS = [
    "05_uc1_dq_reporter",
    "06_uc2_fraud_investigator",
    "07_uc3_product_rag",
    "08_uc4_executive_intel",
]

PIPELINE_NAME = "GlobalMart-Data-Intelligence-Pipeline"
JOB_NAME      = "GlobalMart-End-to-End-Intelligence"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    print(f"[deploy] {msg}")


def safe_create(label: str, fn, *args, **kwargs):
    """Call *fn* and swallow 'already exists' errors."""
    try:
        result = fn(*args, **kwargs)
        log(f"  Created {label}")
        return result
    except Exception as exc:
        msg = str(exc).lower()
        if any(s in msg for s in [
            "already exists", "already_exists", "resource_already_exists",
            "metastore storage root url does not exist",
        ]):
            log(f"  {label} already exists -- skipped")
        else:
            raise


# ---------------------------------------------------------------------------
# Step 1 -- Authenticate
# ---------------------------------------------------------------------------
def authenticate():
    log("Step 1: Authenticating ...")
    host  = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        log("  ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set.")
        sys.exit(1)
    w  = WorkspaceClient(host=host, token=token)
    me = w.current_user.me()
    log(f"  Logged in as {me.user_name}")
    return w, me.user_name


# ---------------------------------------------------------------------------
# Step 2 -- Create Unity Catalog structure
# ---------------------------------------------------------------------------
def create_catalog_structure(w: WorkspaceClient) -> None:
    log("Step 2: Creating Unity Catalog structure ...")

    safe_create(
        f"catalog '{CATALOG}'",
        w.catalogs.create,
        name=CATALOG,
        comment="GlobalMart Data Intelligence Platform",
    )

    for schema in SCHEMAS:
        safe_create(
            f"schema '{CATALOG}.{schema}'",
            w.schemas.create,
            name=schema,
            catalog_name=CATALOG,
            comment=f"GlobalMart {schema} layer",
        )

    safe_create(
        f"volume '{CATALOG}.{VOLUME_SCHEMA}.{VOLUME_NAME}'",
        w.volumes.create,
        catalog_name=CATALOG,
        schema_name=VOLUME_SCHEMA,
        name=VOLUME_NAME,
        volume_type=VolumeType.MANAGED,
        comment="Source data files for Auto Loader ingestion",
    )


# ---------------------------------------------------------------------------
# Step 3 -- Upload source data files
# ---------------------------------------------------------------------------
def upload_data(w: WorkspaceClient) -> None:
    log("Step 3: Uploading source data files ...")

    if not DATA_DIR.exists():
        log(f"  WARNING: data directory not found at {DATA_DIR}")
        log("  Skipping data upload -- place your 16 source files in data/ and re-run.")
        return

    count = 0
    for local_path in sorted(DATA_DIR.rglob("*")):
        if local_path.is_dir():
            continue
        if local_path.name.startswith(".") or "_checkpoints" in str(local_path):
            continue

        relative    = local_path.relative_to(DATA_DIR)
        remote_path = f"{VOLUME_PATH}/{relative.as_posix()}"

        with open(local_path, "rb") as f:
            w.files.upload(remote_path, f, overwrite=True)
        count += 1
        log(f"  Uploaded {relative.as_posix()}")

    log(f"  {count} file(s) uploaded to {VOLUME_PATH}")


# ---------------------------------------------------------------------------
# Steps 4 & 5 -- Upload notebooks
# ---------------------------------------------------------------------------
def upload_notebooks(w: WorkspaceClient, user_email: str):
    base_folder  = f"/Users/{user_email}/globalmart-data-intelligence"
    dlt_folder   = f"{base_folder}/pipeline"
    genai_folder = f"{base_folder}/genai"

    for folder in [base_folder, dlt_folder, genai_folder]:
        try:
            w.workspace.mkdirs(folder)
        except Exception:
            pass  # already exists

    log("Step 4: Uploading DLT notebooks ...")
    for name in DLT_NOTEBOOKS:
        _upload_notebook(w, name, dlt_folder)

    log("Step 5: Uploading GenAI notebooks ...")
    for name in GENAI_NOTEBOOKS:
        _upload_notebook(w, name, genai_folder)

    return dlt_folder, genai_folder


def _upload_notebook(w: WorkspaceClient, name: str, folder: str) -> None:
    local_file = NOTEBOOKS_DIR / f"{name}.py"
    if not local_file.exists():
        log(f"  WARNING: {local_file} not found -- skipping")
        return

    remote_path = f"{folder}/{name}"
    content     = local_file.read_bytes()

    w.workspace.upload(
        remote_path,
        content,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    log(f"  Uploaded {name} -> {remote_path}")


# ---------------------------------------------------------------------------
# Step 6 -- Create DLT pipeline
# ---------------------------------------------------------------------------
def create_pipeline(w: WorkspaceClient, dlt_folder: str) -> str:
    """Create the DLT pipeline and return its pipeline_id."""
    log("Step 6: Creating DLT pipeline ...")

    # Return existing pipeline ID if already created (idempotent)
    existing = list(w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'"))
    if existing:
        pid  = existing[0].pipeline_id
        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        log(f"  Pipeline already exists: {pid}")
        log(f"  Pipeline URL: {host}/#joblist/pipelines/{pid}")
        return pid

    created = w.pipelines.create(
        name=PIPELINE_NAME,
        catalog=CATALOG,
        target="bronze",
        continuous=False,
        development=True,
        channel="CURRENT",
        serverless=True,
        libraries=[
            pipelines.PipelineLibrary(
                notebook=pipelines.NotebookLibrary(path=f"{dlt_folder}/{nb}")
            )
            for nb in DLT_NOTEBOOKS
        ],
    )

    pid  = created.pipeline_id
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    log(f"  Pipeline created: {pid}")
    log(f"  Pipeline URL: {host}/#joblist/pipelines/{pid}")
    return pid


# ---------------------------------------------------------------------------
# Step 7 -- Create end-to-end Workflow
# ---------------------------------------------------------------------------
def create_workflow(w: WorkspaceClient, pipeline_id: str, genai_folder: str, user_email: str) -> int:
    """Create the end-to-end Workflow (DLT pipeline + GenAI notebooks) and return job_id."""
    log("Step 7: Creating end-to-end Workflow ...")

    # Return existing job ID if already created (idempotent)
    existing = [j for j in w.jobs.list(name=JOB_NAME)]
    if existing:
        job_id = existing[0].job_id
        host   = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        log(f"  Workflow already exists: {job_id}")
        log(f"  Workflow URL: {host}/#job/{job_id}")
        return job_id

    # Serverless environment for notebook tasks (client='2' = current serverless REPL)
    ENV_KEY = "serverless"

    created = w.jobs.create(
        name=JOB_NAME,
        environments=[
            jobs.JobEnvironment(
                environment_key=ENV_KEY,
                spec=compute.Environment(client="2"),
            )
        ],
        tasks=[
            # Task 1 -- DLT pipeline: Bronze -> Silver -> Gold -> Metrics
            jobs.Task(
                task_key="dlt_pipeline",
                description="Run DLT pipeline: Bronze -> Silver -> Gold -> Metrics",
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=pipeline_id,
                    full_refresh=False,
                ),
            ),
            # Task 2 -- UC1: AI Data Quality Reporter
            jobs.Task(
                task_key="uc1_dq_reporter",
                description="UC1: AI Data Quality Reporter",
                depends_on=[jobs.TaskDependency(task_key="dlt_pipeline")],
                environment_key=ENV_KEY,
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{genai_folder}/05_uc1_dq_reporter",
                    source=jobs.Source.WORKSPACE,
                ),
            ),
            # Task 3 -- UC2: Returns Fraud Investigator
            jobs.Task(
                task_key="uc2_fraud_investigator",
                description="UC2: Returns Fraud Investigator",
                depends_on=[jobs.TaskDependency(task_key="uc1_dq_reporter")],
                environment_key=ENV_KEY,
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{genai_folder}/06_uc2_fraud_investigator",
                    source=jobs.Source.WORKSPACE,
                ),
            ),
            # Task 4 -- UC3: Product Intelligence Assistant (RAG)
            jobs.Task(
                task_key="uc3_product_rag",
                description="UC3: Product Intelligence Assistant (RAG)",
                depends_on=[jobs.TaskDependency(task_key="uc2_fraud_investigator")],
                environment_key=ENV_KEY,
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{genai_folder}/07_uc3_product_rag",
                    source=jobs.Source.WORKSPACE,
                ),
            ),
            # Task 5 -- UC4: Executive Business Intelligence
            jobs.Task(
                task_key="uc4_executive_intel",
                description="UC4: Executive Business Intelligence",
                depends_on=[jobs.TaskDependency(task_key="uc3_product_rag")],
                environment_key=ENV_KEY,
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{genai_folder}/08_uc4_executive_intel",
                    source=jobs.Source.WORKSPACE,
                ),
            ),
        ],
    )

    job_id = created.job_id
    host   = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    log(f"  Workflow created: {job_id}")
    log(f"  Workflow URL: {host}/#job/{job_id}")
    return job_id


# ---------------------------------------------------------------------------
# Step 8 -- Print summary
# ---------------------------------------------------------------------------
def print_summary(user_email: str, pipeline_id: str, job_id: int) -> None:
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    base = f"/Users/{user_email}/globalmart-data-intelligence"

    log("")
    log("=" * 60)
    log("  DEPLOYMENT COMPLETE")
    log("=" * 60)
    log("")
    log("Workspace objects:")
    log(f"  Catalog:    {CATALOG}")
    log(f"  Schemas:    {', '.join(SCHEMAS)}")
    log(f"  Volume:     {VOLUME_PATH}")
    log(f"  DLT folder: {base}/pipeline/")
    log(f"  GenAI:      {base}/genai/")
    log("")
    log("Key URLs:")
    log(f"  DLT Pipeline : {host}/#joblist/pipelines/{pipeline_id}")
    log(f"  Workflow Job : {host}/#job/{job_id}")
    log("")
    log("Notebook URLs:")
    for nb in DLT_NOTEBOOKS:
        log(f"  {host}/#workspace{base}/pipeline/{nb}")
    for nb in GENAI_NOTEBOOKS:
        log(f"  {host}/#workspace{base}/genai/{nb}")
    log("")
    log("=" * 60)
    log("  NEXT STEPS")
    log("=" * 60)
    log("")
    log("Step 1: Run the end-to-end Workflow")
    log(f"  Open: {host}/#job/{job_id}")
    log("  Click 'Run now' to execute all 5 tasks in sequence.")
    log("  Wait for all tasks to complete (15-30 min on Free Edition).")
    log("")
    log("Step 2: Create the Genie Space (AFTER the Workflow completes)")
    log("  The Genie Space requires all tables to exist first.")
    log("  Run:  python scripts/setup_genie.py")
    log("")
    log("Step 3: Take screenshots for hackathon submission")
    log("  - Unity Catalog: all Bronze/Silver/Gold/Metrics tables")
    log("  - Successful pipeline run (all green)")
    log("  - Pipeline lineage graph")
    log("  - Sample rows from each Gold table")
    log("  - Sample rows from each GenAI output table")
    log("  - Genie Space with 3 NL queries answered")
    log("")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Deploy GlobalMart Data Intelligence Platform to Databricks"
    )
    parser.add_argument("--skip-data",     action="store_true", help="Skip data file upload")
    parser.add_argument("--skip-pipeline", action="store_true", help="Skip DLT pipeline creation")
    parser.add_argument("--skip-job",      action="store_true", help="Skip Workflow creation")
    args = parser.parse_args()

    w, user_email = authenticate()

    create_catalog_structure(w)

    if args.skip_data:
        log("Step 3: Skipping data upload (--skip-data)")
    else:
        upload_data(w)

    dlt_folder, genai_folder = upload_notebooks(w, user_email)

    if args.skip_pipeline:
        log("Step 6: Skipping DLT pipeline creation (--skip-pipeline)")
        # Still need pipeline_id for job creation -- look it up
        existing = list(w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'"))
        pipeline_id = existing[0].pipeline_id if existing else None
    else:
        pipeline_id = create_pipeline(w, dlt_folder)

    if args.skip_job:
        log("Step 7: Skipping Workflow creation (--skip-job)")
        existing = [j for j in w.jobs.list(name=JOB_NAME)]
        job_id = existing[0].job_id if existing else 0
    elif pipeline_id is None:
        log("Step 7: Skipping Workflow creation (no pipeline ID available)")
        job_id = 0
    else:
        job_id = create_workflow(w, pipeline_id, genai_folder, user_email)

    print_summary(user_email, pipeline_id or "unknown", job_id)


if __name__ == "__main__":
    main()
