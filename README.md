# GlobalMart Data Intelligence Platform

A complete Data Intelligence Platform built on **Databricks Free Edition** that solves three critical business failures for GlobalMart, a fictional US retail chain operating across 6 disconnected regional data systems.

## Business Problems Solved

| # | Problem | Impact |
|---|---------|--------|
| 1 | **Revenue Audit** — Duplicate customers across 6 regional systems | 9% revenue overstatement |
| 2 | **Returns Fraud** — No cross-region visibility into return patterns | $2.3M annual loss |
| 3 | **Inventory Blindspot** — Products misallocated across regions | 12–18% revenue lost |

## Platform Architecture

```
16 Source Files (CSV/JSON)
        │
        ▼ Auto Loader (cloudFiles)
┌─────────────────┐
│  Bronze Layer   │  6 streaming tables — raw ingestion, zero transformations
└────────┬────────┘
         │ 23 Quality Rules
         ▼
┌─────────────────┐
│  Silver Layer   │  6 clean tables + rejected_records + pipeline_audit_log
└────────┬────────┘
         │ Galaxy Schema (Fact Constellation)
         ▼
┌─────────────────┐
│   Gold Layer    │  4 dims + 2 facts + 1 bridge + 3 materialized views
└────────┬────────┘
         │ Pre-computed Aggregations
         ▼
┌─────────────────┐
│ Metrics Layer   │  6 materialized views (2 per business failure)
└────────┬────────┘
         │ LLM (databricks-gpt-oss-20b)
         ▼
┌─────────────────┐
│  GenAI Layer    │  4 use cases — DQ audit, fraud detection, RAG, executive intel
└─────────────────┘
```

## Project Structure

```
globalmart/
├── README.md                        ← this file
├── data/                            ← 16 source files (Region 1-6 + root)
├── documents/
│   ├── genie_setup.md               ← Genie Space manual setup guide
│   ├── setup_guide.md               ← Databricks account & token setup (detailed)
│   ├── bronze_data_review.md        ← 27 source data issues catalogued
│   ├── dimensional_model_design.md  ← full dimensional model documentation
│   ├── quality_rules.md             ← all 23 quality rules documented
│   ├── reflection_answers.md        ← cross-use-case reflection answers
│   └── team_summary.md              ← 1-page project summary
├── notebooks/
│   ├── 01_bronze_ingestion.py       ← DLT: Auto Loader → Bronze
│   ├── 02_silver_harmonization.py   ← DLT: Quality rules → Silver
│   ├── 03_gold_dimensional.py       ← DLT: Dimensional model → Gold
│   ├── 04_metrics_aggregation.py    ← DLT: Aggregations → Metrics
│   ├── 05_uc1_dq_reporter.py        ← GenAI: DQ audit explanations
│   ├── 06_uc2_fraud_investigator.py ← GenAI: Anomaly scoring + briefs
│   ├── 07_uc3_product_rag.py        ← GenAI: RAG with FAISS + embeddings
│   └── 08_uc4_executive_intel.py    ← GenAI: Executive summaries + ai_query()
└── scripts/
    ├── deploy.py                    ← one-command full deployment
    └── setup_genie.py               ← Genie Space creation
```

---

## Quick Start

### 1 — Get Databricks Free Edition

1. Go to **https://www.databricks.com/try-databricks**
2. Click **"Get Started for Free"**
3. Sign up with your email and select **Community Edition (AWS)**
4. Verify your email and complete account setup
5. Once logged in, your **workspace URL** appears in the browser address bar:
   ```
   https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
   ```
   Copy this — it is your `DATABRICKS_HOST`.

> **Important:** Select Community Edition on AWS. Azure and GCP require a paid plan.

---

### 2 — Create a Personal Access Token

1. Log in to your Databricks workspace
2. Click your **profile icon** (top-right corner)
3. Go to **Settings → Developer → Manage** (Access Tokens)
4. Click **"Generate new token"**
5. Enter a comment (e.g., `globalmart-deploy`) and click **Generate**
6. Copy the token — it starts with `dapi` and is shown **only once**:
   ```
   dapi_your_token_here
   ```

> **Security:** Never commit your token to Git. If accidentally exposed, revoke it immediately in Settings → Developer → Access Tokens.

---

### 3 — Set Environment Variables

**Windows — Command Prompt:**
```cmd
set DATABRICKS_HOST=https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
set DATABRICKS_TOKEN=dapi...
```

**Windows — PowerShell:**
```powershell
$env:DATABRICKS_HOST = "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com"
$env:DATABRICKS_TOKEN = "dapi..."
```

**Mac / Linux:**
```bash
export DATABRICKS_HOST="https://dbc-xxxxxxxx-xxxx.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
```

---

### 4 — Install Dependencies

```bash
pip install databricks-sdk requests
```

---

### 5 — Place Source Data Files

Put the 16 source data files in the `data/` folder, preserving the folder structure:

```
data/
├── products.json
├── returns_2.json
├── transactions_3.csv
├── vendors.csv
├── Region 1/  → customers_1.csv, orders_1.csv
├── Region 2/  → customers_2.csv, transactions_1.csv
├── Region 3/  → customers_3.csv, orders_2.csv
├── Region 4/  → customers_4.csv, transactions_2.csv
├── Region 5/  → customers_5.csv, orders_3.csv
└── Region 6/  → customers_6.csv, returns_1.json
```

---

### 6 — Deploy Everything

```bash
python scripts/deploy.py
```

This single command:
- Creates Unity Catalog structure (`globalmart` catalog + 4 schemas)
- Uploads all 16 source files to the managed volume
- Uploads all 8 notebooks to your workspace
- Creates the DLT pipeline (Bronze → Silver → Gold → Metrics)
- Creates the end-to-end Workflow (pipeline + 4 GenAI notebooks)
- Creates the Genie Space with 20 tables and descriptions
- Prints all URLs and next steps

**Optional flags:**
```bash
python scripts/deploy.py --skip-data      # skip data upload (already uploaded)
python scripts/deploy.py --skip-pipeline  # skip pipeline creation (already exists)
python scripts/deploy.py --skip-job       # skip Workflow creation (already exists)
python scripts/deploy.py --skip-genie     # skip Genie Space creation (already exists)
```

---

### 7 — Run the End-to-End Workflow

> **Important:** Do NOT trigger the DLT pipeline directly. Always trigger the **Workflow Job** instead.
> The Workflow runs the pipeline first and then automatically runs all 4 GenAI notebooks (UC1–UC4)
> in sequence. If you run only the pipeline, you will miss the AI outputs entirely.

#### What the Workflow does (5 tasks in order):
| Task | Name | What it does |
|------|------|-------------|
| 1 | `dlt_pipeline` | Runs the full DLT pipeline: Bronze → Silver → Gold → Metrics |
| 2 | `uc1_dq_reporter` | AI Data Quality Reporter — explains data rejections in plain English |
| 3 | `uc2_fraud_investigator` | Returns Fraud Investigator — scores customers, generates investigation briefs |
| 4 | `uc3_product_rag` | Product Intelligence Assistant — RAG with FAISS + embeddings |
| 5 | `uc4_executive_intel` | Executive Business Intelligence — LLM summaries + ai_query() demos |

#### Steps to run:

1. The deploy script (Step 6) printed a **Workflow URL** that looks like:
   ```
   https://dbc-xxxxxxxx-xxxx.cloud.databricks.com/#job/<job_id>
   ```
   Open that URL in your browser.

2. You will land on the **`GlobalMart-End-to-End-Intelligence`** job page.

3. Click the **"Run now"** button (top right).

4. The job will execute all 5 tasks in sequence — each one starts automatically after the previous completes.

5. Monitor progress by clicking on the running job run. Each task shows green (success) or red (failed).

6. When all 5 tasks show green, the full platform is populated:
   - All Bronze / Silver / Gold / Metrics tables contain data
   - `dq_audit_report` — AI data quality findings
   - `flagged_return_customers` — fraud-scored customers with LLM briefs
   - `rag_query_history` — RAG query results
   - `ai_business_insights` — executive summaries

> **Expected run time:** 15–30 minutes on Databricks Free Edition (serverless compute).

#### If a task fails

1. Click on the failed task (shown in red) to open its logs
2. Read the error message at the bottom of the logs
3. Common causes:
   - **`dlt_pipeline` fails** — check the DLT pipeline event log for table-level errors
   - **UC notebooks fail** — usually a missing table (pipeline didn't complete) or LLM timeout; re-run the task individually from the job run page
4. To re-run a single failed task: open the job run → click the failed task → click **"Repair run"**
5. Do not re-run the full job from scratch unless the pipeline itself failed — this wastes compute time

---

### 8 — Set Up the Genie Space

> **Timing:** Complete this step only AFTER the Workflow (Step 7) has finished successfully and all tables are visible in the Unity Catalog. If you add Joins before the tables exist, the table dropdowns in the Genie UI will be empty.

The Genie Space is created automatically by `deploy.py` (Step 6 above) with all 20 tables and per-table descriptions. If you need to recreate it standalone at any point (e.g., after dropping the space), run:

```bash
python scripts/setup_genie.py
```

This will:
- Drop the existing `GlobalMart-Genie` space (if it exists)
- Create a fresh space with all 20 tables and descriptions
- Print the direct URLs to the Instructions and Joins tabs

After running `setup_genie.py`, two tabs must be filled in manually via the Genie UI (no public API exists for them). Follow the step-by-step guide in:

```
documents/genie_setup.md
```

---

## Key Numbers

| Metric | Value |
|--------|-------|
| Source files | 16 |
| Bronze tables | 6 |
| Silver tables | 8 (6 clean + rejected_records + audit_log) |
| Gold tables | 10 (4 dims + 2 facts + 1 bridge + 3 MVs) |
| Metrics tables | 6 |
| Quality rules | 23 |
| Raw → Unique customers | 748 → 374 |
| GenAI use cases | 4 |
| LLM model | databricks-gpt-oss-20b (free) |
| Embedding model | all-MiniLM-L6-v2 (local, no API cost) |

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `DATABRICKS_HOST and DATABRICKS_TOKEN must be set` | Env vars not set | Re-run `set` / `export` in the same terminal |
| `403 Forbidden` | Token expired or revoked | Generate a new token in Settings → Developer |
| `No SQL warehouses found` | No warehouse running | Go to SQL → Warehouses and start the Serverless Starter Warehouse |
| `Catalog 'globalmart' already exists` | Script run before | Safe to ignore — script skips existing objects |
| `pip install` fails | Python not installed | Install Python 3.9+ from https://python.org |
