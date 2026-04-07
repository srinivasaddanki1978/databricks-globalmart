# GlobalMart Genie Space — Manual Setup Guide

After running `python scripts/setup_genie.py` (or `python scripts/deploy.py`), the space
is created automatically with all 20 tables and per-table descriptions.

Two things must be added manually via the Genie UI — the Instructions tab and the Joins tab
have no public Databricks API:

1. **Instructions tab** — business context for the AI
2. **Joins tab** — foreign-key relationships between tables

> **No hardcoded URLs here.** The Space ID changes every time the space is recreated.
> Always use the URLs printed by the script at the end of its run — they are always fresh.

---

## Step 0 — Get the URLs from the script output

Run the setup script:

```bash
python scripts/setup_genie.py
```

At the end of its output, it prints two direct URLs — copy them:

```
[genie]   STEP 1 — Instructions tab
[genie]   Open this URL directly: https://<host>/genie/rooms/<space_id>?o=<org_id>&sp=context.instructions

[genie]   STEP 2 — Joins tab
[genie]   Open this URL directly: https://<host>/genie/rooms/<space_id>?o=<org_id>&sp=context.joins
```

Use these URLs for Steps 1 and 2 below. Do not bookmark them — they change each time
the space is recreated.

---

## Step 1 — Instructions Tab

1. Open the **Instructions URL** printed by the script
2. Click the text box
3. Paste the text below exactly as shown
4. Click **Save**

```
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
- flagged_return_customers: Fraud-scored customers (anomaly_score >= 40 = flagged).
- dq_audit_report: AI data quality findings — rejection rates and LLM explanations per field (UC1).
- rag_query_history: Product intelligence Q&A log from RAG assistant (UC3).
- ai_business_insights: LLM executive summaries by business domain — revenue_performance, vendor_return_rate, slow_moving_inventory (UC4).
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
```

---

## Step 2 — Joins Tab

1. Open the **Joins URL** printed by the script
2. Click **+ Add join** for each row in the table below
3. The dialog has 4 fields — fill them exactly as shown

| # | Left Table | Right Table | Join Condition (SQL expression) | Relationship Type |
|---|---|---|---|---|
| 1 | `fact_sales` | `dim_customers` | `customer_sk = customer_sk` | Many to One |
| 2 | `fact_sales` | `dim_products` | `product_sk = product_sk` | Many to One |
| 3 | `fact_sales` | `dim_vendors` | `vendor_sk = vendor_sk` | Many to One |
| 4 | `fact_sales` | `dim_dates` | `order_date_sk = date_sk` | Many to One |
| 5 | `fact_returns` | `dim_customers` | `customer_sk = customer_sk` | Many to One |
| 6 | `fact_returns` | `dim_dates` | `return_date_sk = date_sk` | Many to One |
| 7 | `bridge_return_products` | `dim_products` | `product_sk = product_sk` | Many to One |
| 8 | `bridge_return_products` | `fact_sales` | `order_id = order_id` | Many to One |

### Dialog field reference

When you click **+ Add join**, the dialog looks like this:

```
Define join relationship
─────────────────────────────────────
Left Table      [ fact_sales          ▼ ]
Right Table     [ dim_customers       ▼ ]

Join condition
  ● Use SQL expression
  [ customer_sk = customer_sk         ]

Relationship Type
  [ Many to One                       ▼ ]
─────────────────────────────────────
                        [ Cancel ] [ Save ]
```

- **Left Table / Right Table** — type the table name and select from the dropdown
- **Join condition** — select "Use SQL expression", then type the expression from the table above
- **Relationship Type** — select "Many to One"
- Click **Save** after each join

---

## Step 3 — Test the Space

Open the Genie space (URL printed by the script) and try these 3 natural language queries:

| Query | Expected result |
|---|---|
| `What is the total revenue by region?` | Bar/table showing revenue for East, West, South, Central, North |
| `Which vendors have the highest return rate?` | Ranked vendor list with return_rate_pct |
| `Show me the top 5 products by revenue in the West region` | Top 5 products filtered to West |

---

## Notes

- **Instructions and Joins have no public Databricks API** — they can only be set via the UI.
- **All 20 tables and descriptions are added automatically** by the script — no manual action needed for tables.
- **The Space ID changes every time** the space is recreated. Always get fresh URLs from the script output — never from this file or from bookmarks.
