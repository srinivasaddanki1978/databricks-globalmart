#!/usr/bin/env python3
"""
GlobalMart Genie Space Setup Script

Automates:
  - Drops (if exists) and recreates the Genie Space
  - Adds all 20 Gold + Metrics tables with per-table descriptions
  - Sets space description (visible in 'About this space')

MANUAL STEPS REQUIRED after running this script:
  The Genie Instructions tab and Joins tab have no public API.
  Run this script first, then follow the printed instructions to
  copy-paste the content into the Genie UI.

Usage:
    python scripts/setup_genie.py

Prerequisites:
    export DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"
    export DATABRICKS_TOKEN="dapi..."
"""

import json
import os
import sys

from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SPACE_TITLE = "GlobalMart-Genie"

# ---------------------------------------------------------------------------
# About / Description  (shows in "About this space" panel)
# ---------------------------------------------------------------------------
ABOUT_DESCRIPTION = (
    "GlobalMart Data Intelligence Platform - 5-region US retail chain. "
    "Solves Revenue Audit (9% overstatement), Returns Fraud ($2.3M loss), "
    "and Inventory Blindspot (12-18% revenue loss). "
    "Catalog: globalmart | Schemas: gold, metrics | Tables: 20"
)

# ---------------------------------------------------------------------------
# INSTRUCTIONS_TAB  — copy-paste this into the Genie Instructions tab
# (Edit space -> Instructions tab)
# ---------------------------------------------------------------------------
INSTRUCTIONS_TAB = """\
You are a data analyst assistant for GlobalMart, a 5-region US retail chain.

BUSINESS CONTEXT
This platform solves three critical business failures:
1. Revenue Audit: 9% revenue overstatement from duplicate customers across 6 regional systems.
2. Returns Fraud: $2.3M annual loss from no cross-region visibility into return patterns.
3. Inventory Blindspot: 12-18% revenue lost from products misallocated across regions.

DATA MODEL
- dim_customers: 374 unique customers (deduped from 748 raw). Join on customer_sk. Segments: Consumer, Corporate, Home Office. Regions: East, West, South, Central, North.
- dim_products: Product catalog. Join on product_sk.
- dim_vendors: Vendor reference. Join on vendor_sk.
- dim_dates: Calendar 2016-2018. Join on date_sk (fact_sales uses order_date_sk, fact_returns uses return_date_sk).
- fact_sales: One row per order line item. Measures: sales, quantity, discount, profit.
- fact_returns: One row per return event. Measures: refund_amount. return_status: Approved/Pending/Rejected.
- bridge_return_products: Proportional refund allocation per product line item.
- mv_revenue_by_region: Pre-aggregated monthly revenue by region (Revenue Audit).
- mv_return_rate_by_vendor: Vendor-level return rates (Returns Fraud).
- mv_slow_moving_products: Products flagged as slow-moving (bottom 25% by quantity in region).
- dq_audit_report: AI data quality findings from UC1.
- flagged_return_customers: Fraud-scored customers (anomaly_score >= 40 = flagged).
- rag_query_history: Product intelligence Q&A log from UC3.
- ai_business_insights: Executive summaries from UC4.
- mv_monthly_revenue_by_region: Monthly revenue with avg_order_value (metrics schema).
- mv_segment_profitability: Profit margin by segment x region (metrics schema).
- mv_customer_return_history: Customer-level return profiles (metrics schema).
- mv_return_reason_analysis: Return reasons by region (metrics schema).
- mv_vendor_product_performance: Vendor x product x region performance (metrics schema).
- mv_product_return_impact: Product return cost by region (metrics schema).

COMMON QUESTIONS
- Total revenue by region: use mv_revenue_by_region or mv_monthly_revenue_by_region
- Vendors with highest return rate: use mv_return_rate_by_vendor
- Slow-moving products: filter mv_slow_moving_products WHERE is_slow_moving = true
- Top customers by spend: SUM(sales) from fact_sales JOIN dim_customers ON customer_sk
- Fraud risk customers: use flagged_return_customers WHERE anomaly_score >= 40
- Profit margin by segment: use mv_segment_profitability
"""

# ---------------------------------------------------------------------------
# JOINS  — add each row in the Genie Joins tab
# (Edit space -> Joins tab -> add join for each row below)
# Format: Left Table | Right Table | Condition
# ---------------------------------------------------------------------------
JOINS = [
    # fact_sales joins
    ("globalmart.gold.fact_sales",           "globalmart.gold.dim_customers",
     "`fact_sales`.`customer_sk` = `dim_customers`.`customer_sk`"),
    ("globalmart.gold.fact_sales",           "globalmart.gold.dim_products",
     "`fact_sales`.`product_sk` = `dim_products`.`product_sk`"),
    ("globalmart.gold.fact_sales",           "globalmart.gold.dim_vendors",
     "`fact_sales`.`vendor_sk` = `dim_vendors`.`vendor_sk`"),
    ("globalmart.gold.fact_sales",           "globalmart.gold.dim_dates",
     "`fact_sales`.`order_date_sk` = `dim_dates`.`date_sk`"),
    # fact_returns joins
    ("globalmart.gold.fact_returns",         "globalmart.gold.dim_customers",
     "`fact_returns`.`customer_sk` = `dim_customers`.`customer_sk`"),
    ("globalmart.gold.fact_returns",         "globalmart.gold.dim_dates",
     "`fact_returns`.`return_date_sk` = `dim_dates`.`date_sk`"),
    # bridge joins
    ("globalmart.gold.bridge_return_products", "globalmart.gold.dim_products",
     "`bridge_return_products`.`product_sk` = `dim_products`.`product_sk`"),
    ("globalmart.gold.bridge_return_products", "globalmart.gold.fact_sales",
     "`bridge_return_products`.`order_id` = `fact_sales`.`order_id`"),
]

# ---------------------------------------------------------------------------
# Tables: (identifier, [per-table description strings])
# MUST be sorted alphabetically by identifier (API requirement).
# ---------------------------------------------------------------------------
TABLES = [
    (
        "globalmart.gold.ai_business_insights",
        ["LLM executive summaries (UC4). "
         "insight_type values: revenue_performance, vendor_return_rate, "
         "slow_moving_inventory. "
         "Columns: insight_type, executive_summary, kpi_data, generated_at."],
    ),
    (
        "globalmart.gold.bridge_return_products",
        ["Bridge table allocating return refunds proportionally across order line items. "
         "Join to dim_products ON product_sk, "
         "fact_sales ON order_id AND product_sk. "
         "Measures: allocated_refund, allocation_weight, line_sales, total_order_sales."],
    ),
    (
        "globalmart.gold.dim_customers",
        ["Deduplicated customer dimension. 374 unique customers deduped from 748 raw. "
         "Key join column: customer_sk (surrogate key). "
         "Business key: customer_id. Attributes: customer_name, segment "
         "(Consumer/Corporate/Home Office), region (East/West/South/Central/North), "
         "city, state."],
    ),
    (
        "globalmart.gold.dim_dates",
        ["Calendar dimension spanning 2016-2018. "
         "Key join column: date_sk (surrogate key). "
         "Business key: date_key (yyyy-MM-dd). "
         "Attributes: year, quarter, month, month_name, day, day_of_week, is_weekend."],
    ),
    (
        "globalmart.gold.dim_products",
        ["Product catalog dimension. "
         "Key join column: product_sk (surrogate key). "
         "Business key: product_id. Attributes: product_name, brand, category, upc."],
    ),
    (
        "globalmart.gold.dim_vendors",
        ["Vendor reference dimension. "
         "Key join column: vendor_sk (surrogate key). "
         "Business key: vendor_id. Attributes: vendor_name."],
    ),
    (
        "globalmart.gold.dq_audit_report",
        ["AI-generated data quality audit report (UC1). "
         "Columns: entity, affected_field, issue_type, rejected_count, "
         "total_entity_records, rejection_rate_pct, sample_records, ai_explanation, "
         "generated_at."],
    ),
    (
        "globalmart.gold.fact_returns",
        ["Returns fact table - one row per return event. "
         "Join to dim_customers ON customer_sk, "
         "dim_dates ON return_date_sk = date_sk. "
         "Measure: refund_amount. "
         "Attributes: order_id, return_reason, return_status (Approved/Pending/Rejected)."],
    ),
    (
        "globalmart.gold.fact_sales",
        ["Sales fact table - one row per order line item. "
         "Join to dim_customers ON customer_sk, "
         "dim_products ON product_sk, "
         "dim_vendors ON vendor_sk, "
         "dim_dates ON order_date_sk = date_sk. "
         "Measures: sales (revenue), quantity, discount (0-1), profit. "
         "Attributes: order_id, ship_mode, order_status, payment_type."],
    ),
    (
        "globalmart.gold.flagged_return_customers",
        ["Fraud-scored customers with LLM investigation briefs (UC2). "
         "Customers with anomaly_score >= 40 are flagged. "
         "Columns: customer_id, customer_name, anomaly_score (0-100), "
         "rules_violated, total_returns, total_refund_value, investigation_brief."],
    ),
    (
        "globalmart.gold.mv_return_rate_by_vendor",
        ["Vendor-level return rates. Use for Returns Fraud analysis. "
         "Grain: one row per vendor. "
         "Measures: total_orders, total_sales, return_order_count, total_refunded, "
         "return_rate_pct."],
    ),
    (
        "globalmart.gold.mv_revenue_by_region",
        ["Pre-computed monthly revenue by region. Use for Revenue Audit analysis. "
         "Grain: year x month x region. "
         "Measures: total_sales, total_profit, order_count, total_quantity, unique_customers."],
    ),
    (
        "globalmart.gold.mv_slow_moving_products",
        ["Product x region performance with slow-moving flag. "
         "Use for Inventory Blindspot analysis. "
         "is_slow_moving=true means bottom 25% by quantity in that region. "
         "Grain: product_id x region. "
         "Measures: total_sales, total_quantity, order_count, total_profit."],
    ),
    (
        "globalmart.gold.rag_query_history",
        ["RAG query history from Product Intelligence Assistant (UC3). "
         "Columns: question, answer, retrieved_documents, retrieved_count, "
         "top_distance, generated_at."],
    ),
    (
        "globalmart.metrics.mv_customer_return_history",
        ["Customer return profiles for fraud detection. "
         "Grain: one row per customer. "
         "Measures: total_returns, total_refund_value, avg_refund_per_return, "
         "approval_rate_pct, distinct_reason_count. "
         "Attributes: return_reasons (comma-separated)."],
    ),
    (
        "globalmart.metrics.mv_monthly_revenue_by_region",
        ["Monthly revenue with avg_order_value. "
         "Grain: year x month x region. "
         "Measures: total_revenue, total_profit, total_quantity, unique_customers, "
         "avg_order_value."],
    ),
    (
        "globalmart.metrics.mv_product_return_impact",
        ["Product return cost impact by region. "
         "Grain: product_id x region. "
         "Measures: return_line_count, total_return_cost, avg_return_cost, "
         "total_allocation_weight."],
    ),
    (
        "globalmart.metrics.mv_return_reason_analysis",
        ["Return reasons broken down by region. "
         "Grain: return_reason x region. "
         "Measures: return_count, total_refunded, avg_refund, unique_customers, "
         "approved_count, rejected_count."],
    ),
    (
        "globalmart.metrics.mv_segment_profitability",
        ["Segment profitability by region. "
         "Grain: segment x region. "
         "Measures: total_revenue, total_profit, profit_margin_pct, avg_discount, "
         "order_count, customer_count."],
    ),
    (
        "globalmart.metrics.mv_vendor_product_performance",
        ["Vendor x product x region performance with return metrics. "
         "Grain: vendor_id x product_id x region. "
         "Measures: order_count, total_sales, total_quantity, total_profit, "
         "return_count, total_allocated_refund."],
    ),
]


# ---------------------------------------------------------------------------
# Core function — importable from deploy.py
# ---------------------------------------------------------------------------
def _get_org_id(host: str, token: str) -> str:
    """Return the workspace numeric org ID (from response header)."""
    import requests as _req
    r = _req.get(f"{host}/api/2.0/genie/spaces",
                 headers={"Authorization": f"Bearer {token}"})
    return r.headers.get("x-databricks-org-id", "")


def _get_warehouse_id(w: WorkspaceClient, log_fn) -> str:
    """Return the first available SQL warehouse ID."""
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouses found. Create one in Databricks SQL -> Warehouses.")
    wh = warehouses[0]
    log_fn(f"  Using warehouse: {wh.name} ({wh.id})")
    return wh.id


def create_genie_space(w: WorkspaceClient, log_fn=None) -> str:
    """
    Trash any existing GlobalMart-Genie space and create a fresh one.
    Returns the new space_id.
    """
    if log_fn is None:
        log_fn = lambda msg: print(f"[genie] {msg}")

    host  = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    org_id = _get_org_id(host, token)

    warehouse_id = _get_warehouse_id(w, log_fn)

    # Trash existing space if present
    resp = w.genie.list_spaces()
    for space in (resp.spaces or []):
        if space.title == SPACE_TITLE:
            w.genie.trash_space(space.space_id)
            log_fn(f"  Trashed existing space: {space.space_id}")

    # Build serialized_space — tables already sorted alphabetically above
    serialized = json.dumps({
        "version": 2,
        "data_sources": {
            "tables": [
                {"identifier": tid, "description": desc}
                for tid, desc in TABLES
            ]
        },
    })

    # Create new space
    # NOTE: 'description' shows in 'About this space' panel (not Instructions tab).
    # Instructions tab and Joins tab must be filled manually — see printed guide below.
    new_space = w.genie.create_space(
        warehouse_id=warehouse_id,
        title=SPACE_TITLE,
        description=ABOUT_DESCRIPTION,
        serialized_space=serialized,
    )

    space_id = new_space.space_id
    log_fn(f"  Space ID : {space_id}")
    log_fn(f"  Title    : {new_space.title}")
    log_fn(f"  Tables   : {len(TABLES)}")
    log_fn(f"  Open     : {host}/genie/rooms/{space_id}")
    log_fn("")
    _print_manual_steps(host, space_id, org_id, log_fn)

    return space_id


def _print_manual_steps(host: str, space_id: str, org_id: str, log_fn) -> None:
    """Print the exact text to copy-paste into Genie UI Instructions + Joins tabs."""
    o_param = f"?o={org_id}" if org_id else ""
    instr_url = f"{host}/genie/rooms/{space_id}{o_param}&sp=context.instructions"
    joins_url  = f"{host}/genie/rooms/{space_id}{o_param}&sp=context.joins"

    log_fn("=" * 70)
    log_fn("  MANUAL STEPS: complete these in the Genie UI")
    log_fn("=" * 70)
    log_fn("")
    log_fn("  STEP 1 — Instructions tab")
    log_fn(f"  Open this URL directly: {instr_url}")
    log_fn("  Paste the text below into the Instructions text box and Save:")
    log_fn("-" * 70)
    for line in INSTRUCTIONS_TAB.strip().split("\n"):
        log_fn(f"  {line}")
    log_fn("")
    log_fn("-" * 70)
    log_fn("  STEP 2 — Joins tab")
    log_fn(f"  Open this URL directly: {joins_url}")
    log_fn("  Click '+ Add join' for each row below:")
    log_fn(f"  {'Left Table':<42} | {'Right Table':<36} | Condition")
    log_fn("-" * 70)
    for left, right, cond in JOINS:
        left_short  = left.split(".")[-1]
        right_short = right.split(".")[-1]
        log_fn(f"  {left_short:<42} | {right_short:<36} | {cond}")
    log_fn("=" * 70)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def log(msg):
    print(f"[genie] {msg}")


def main():
    host  = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        log("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN must be set.")
        sys.exit(1)

    w = WorkspaceClient(host=host, token=token)
    log("Creating GlobalMart Genie Space ...")
    create_genie_space(w, log)
    log("Done.")


if __name__ == "__main__":
    main()
