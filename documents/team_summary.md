# GlobalMart Data Intelligence Platform — Team Summary

## What We Built

A complete Data Intelligence Platform on Databricks Free Edition that solves three business failures for GlobalMart, a fictional US retail chain with 6 disconnected regional data systems.

| Layer | Objects | Purpose |
|-------|---------|---------|
| Bronze | 6 streaming tables | Raw ingestion via Auto Loader — zero transformations |
| Silver | 8 tables (6 clean + rejected_records + audit_log) | 23 quality rules, standardization, quarantine |
| Gold | 10 tables (4 dims + 2 facts + 1 bridge + 3 MVs) | Dimensional model with deduplication |
| Metrics | 6 materialized views | Pre-computed aggregations per business failure |
| GenAI | 4 notebooks, 4 output tables | LLM-powered audit, fraud, RAG, executive summaries |
| **Total** | **30 tables + 4 GenAI outputs** | |

---

## What Surprised Us

1. **Column name chaos was worse than expected.** Customers had 4 different ID column names across 6 regions. Returns had *every single column* named differently between the two source files. Auto Loader's `schemaEvolutionMode = addNewColumns` combined with COALESCE was the only viable approach — schema merging would have lost data.

2. **The city/state swap in Region 4 was invisible without cross-region validation.** A simple NULL check would never catch it. We detected it by comparing Region 4's "cities" against known US state names, then applied a conditional swap keyed on `_source_file`. This is exactly the kind of silent corruption that causes a 9% revenue overstatement.

3. **`databricks-gpt-oss-20b` returns a Python object, not a JSON string.** The response format is `[{"type": "reasoning", ...}, {"type": "text", "text": "answer"}]` — but it's already a Python list of dicts, not a JSON string. Calling `json.loads()` fails. We had to iterate with `list()` and `dict()` to extract the text block. This required a custom parser in all 4 GenAI notebooks.

4. **Proportional allocation for the bridge table was non-trivial.** Returns are at the order level, but products are at the line-item level. We had to join fact_returns with fact_sales, compute each line's share of total order sales, then multiply by the refund amount. Without this bridge, product-level return cost analysis would be impossible.

---

## What We'd Do Differently

1. **Add a proper CDC pipeline.** Currently, the entire pipeline re-processes all data on each run. In production, we'd use Change Data Capture (CDC) with Auto Loader's file tracking to process only new/changed files, reducing compute cost by 80%+.

2. **Replace FAISS with a persistent vector store.** The RAG pipeline (UC3) builds an ephemeral FAISS index in memory on each notebook run. In production, we'd use Databricks Vector Search or a managed Mosaic AI vector database with incremental updates and a serving endpoint.

3. **Add data contracts between layers.** Silver should enforce schema contracts with explicit column types and NOT NULL constraints, rather than relying on runtime validation. DLT expectations in "fail" mode would prevent bad data from ever reaching Gold.

4. **Implement a monitoring dashboard.** The `pipeline_audit_log` and `rejected_records` tables contain everything needed for a real-time data quality dashboard. We'd add Databricks SQL dashboards with alerts when rejection rates exceed thresholds.

---

## Key Numbers

| Metric | Value |
|--------|-------|
| Source files ingested | 16 |
| Total source records | ~5,000+ |
| Pipeline objects (DLT) | 30 tables across 4 schemas |
| Quality rules | 23 |
| Raw customers → Unique customers | 748 → 374 (50% duplicate rate) |
| Metrics aggregation tables | 6 (2 per business failure) |
| GenAI use cases | 4 (DQ audit, fraud, RAG, executive) |
| LLM model | databricks-gpt-oss-20b (free) |
| Embedding model | all-MiniLM-L6-v2 (local, no API) |

---

## Quantified Business Value

- **Revenue Audit:** Identified that 50% of customer records were duplicates across regions, directly causing the 9% revenue overstatement. After deduplication, true revenue figures are available per region and segment.

- **Returns Fraud:** Built anomaly scoring across 5 weighted rules, providing cross-region visibility that didn't exist before. Flagged high-risk customers with investigation briefs, enabling targeted fraud review instead of manual auditing of all returns.

- **Inventory Blindspot:** Identified slow-moving products per region using bottom-25% quantity threshold. Combined with proportional return cost allocation, operations can now see exactly which products are underperforming where and reallocate inventory accordingly.
