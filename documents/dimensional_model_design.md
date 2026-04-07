# GlobalMart Dimensional Model Design

## Why Galaxy Schema (Fact Constellation)?

This platform uses a **Galaxy Schema** (also called a **Fact Constellation Schema**) — a dimensional modeling pattern where **multiple fact tables share conformed dimensions**. Unlike a simple Star Schema (one fact, many dimensions) or a Snowflake Schema (normalized dimensions), the Galaxy Schema is the right choice here because GlobalMart has two distinct business processes that need to be analyzed both independently and together:

1. **`fact_sales`** — captures the revenue/order process (orders × transactions)
2. **`fact_returns`** — captures the returns/refund process (returns × orders)

Both fact tables share the same conformed dimensions (`dim_customers`, `dim_dates`, `dim_products`, `dim_vendors`), enabling cross-process analysis such as: "Which customers generate high revenue but also have high return rates?" or "Which vendors sell the most but also have the highest refund costs?"

**Why not Star Schema?** A single fact table cannot cleanly represent both sales and returns — they have different grains (order line item vs. return event), different measures (sales/profit vs. refund_amount), and different business processes. Forcing them into one table would create NULLs, ambiguous metrics, and incorrect aggregations.

**Why not Snowflake Schema?** Our dimensions are already compact (374 customers, ~800 products, ~50 vendors). Normalizing them into sub-dimensions would add join complexity without meaningful storage savings.

**The bridge table (`bridge_return_products`)** is a natural extension of the Galaxy Schema — it resolves the many-to-many relationship between returns (order-level) and products (line-item-level) that exists across the two fact tables, enabling product-level return cost attribution.

---

## 1. Model Overview

| Layer | Table | Type | Grain | Source |
|-------|-------|------|-------|--------|
| Gold | `dim_customers` | Dimension | 1 per customer_id | Silver `clean_customers` (ROW_NUMBER dedup) |
| Gold | `dim_products` | Dimension | 1 per product_id | Silver `clean_products` |
| Gold | `dim_vendors` | Dimension | 1 per vendor_id | Silver `clean_vendors` |
| Gold | `dim_dates` | Dimension | 1 per calendar day | Generated (sequence/explode) |
| Gold | `fact_sales` | Fact | 1 per order line item | Silver `clean_orders` INNER JOIN `clean_transactions` |
| Gold | `fact_returns` | Fact | 1 per return event | Silver `clean_returns` LEFT JOIN `clean_orders` |
| Gold | `bridge_return_products` | Bridge | 1 per return × product | Gold `fact_returns` INNER JOIN `fact_sales` |
| Gold | `mv_revenue_by_region` | Materialized View | year × month × region | Gold facts + dims |
| Gold | `mv_return_rate_by_vendor` | Materialized View | 1 per vendor | Gold facts + dims |
| Gold | `mv_slow_moving_products` | Materialized View | product × region | Gold facts + dims |
| Metrics | `mv_monthly_revenue_by_region` | Materialized View | year × month × region | Gold facts + dims |
| Metrics | `mv_segment_profitability` | Materialized View | segment × region | Gold facts + dims |
| Metrics | `mv_customer_return_history` | Materialized View | 1 per customer | Gold facts + dims |
| Metrics | `mv_return_reason_analysis` | Materialized View | return_reason × region | Gold facts + dims |
| Metrics | `mv_vendor_product_performance` | Materialized View | vendor × product × region | Gold facts + dims + bridge |
| Metrics | `mv_product_return_impact` | Materialized View | product × region | Gold bridge + facts + dims |

**Total objects:** 4 dimensions + 2 facts + 1 bridge + 3 Gold MVs + 6 Metrics MVs = **16 Gold/Metrics tables**

---

## 2. Dimension Tables

### dim_customers
- **Primary Key:** `customer_sk` (surrogate), `customer_id` (natural/business key)
- **Deduplication:** ROW_NUMBER partitioned by `customer_id`, ordered by `_load_timestamp` DESC and email preference (non-null email preferred). Reduces 748 raw records to 374 unique customers.
- **Columns:**

| Column | Type | Description |
|--------|------|-------------|
| customer_sk | long | Surrogate key — used as FK in fact tables |
| customer_id | string | Business key (COALESCE of 4 source variants) |
| customer_name | string | Full name |
| customer_email | string | Email address (nullable) |
| segment | string | Consumer, Corporate, or Home Office |
| city | string | City (swapped for Region 4) |
| state | string | State (swapped for Region 4) |
| region | string | East, West, South, Central, or North |

### dim_products
- **Primary Key:** `product_sk` (surrogate), `product_id` (natural/business key)
- **Columns:**

| Column | Type | Description |
|--------|------|-------------|
| product_sk | long | Surrogate key — used as FK in fact tables |
| product_id | string | Business key |
| product_name | string | Product display name |
| brand | string | Brand name |
| category | string | Product category |
| sub_category | string | Product sub-category |
| upc | string | UPC barcode (converted from scientific notation) |

### dim_vendors
- **Primary Key:** `vendor_sk` (surrogate), `vendor_id` (natural/business key)
- **Columns:**

| Column | Type | Description |
|--------|------|-------------|
| vendor_sk | long | Surrogate key — used as FK in fact tables |
| vendor_id | string | Business key |
| vendor_name | string | Vendor company name |

### dim_dates
- **Primary Key:** `date_sk` (surrogate), `date_key` (natural/business key)
- **Generated:** Full calendar years from min(order_date, return_date) to max(order_date, return_date) using `explode(sequence(...))`.
- **Columns:**

| Column | Type | Description |
|--------|------|-------------|
| date_sk | long | Surrogate key — used as FK in fact tables |
| date_key | date | Business key (yyyy-MM-dd) |
| year | integer | Calendar year |
| quarter | integer | Calendar quarter (1-4) |
| month | integer | Month number (1-12) |
| month_name | string | Full month name |
| day | integer | Day of month |
| day_of_week | integer | Day of week (1=Sun, 7=Sat) |
| is_weekend | boolean | True if Saturday or Sunday |

---

## 3. Fact Tables

### fact_sales
- **Grain:** One row per order line item
- **Source Join:** `clean_orders INNER JOIN clean_transactions ON order_id`
- **Foreign Keys:** customer_sk → dim_customers, product_sk → dim_products, vendor_sk → dim_vendors, order_date_sk → dim_dates
- **Columns:**

| Column | Type | Role |
|--------|------|------|
| order_id | string | Degenerate dimension |
| customer_sk | long | FK → dim_customers.customer_sk |
| customer_id | string | Business key (carried for reporting) |
| product_sk | long | FK → dim_products.product_sk |
| product_id | string | Business key |
| vendor_sk | long | FK → dim_vendors.vendor_sk |
| vendor_id | string | Business key |
| order_date_sk | long | FK → dim_dates.date_sk |
| sales | double | Measure: line item revenue |
| quantity | integer | Measure: units sold |
| discount | double | Measure: discount rate (0-1) |
| profit | double | Measure: line item profit |
| ship_mode | string | Attribute |
| order_status | string | Attribute |
| order_purchase_date | timestamp | Full timestamp |
| payment_type | string | Attribute |

### fact_returns
- **Grain:** One row per return event
- **Source Join:** `clean_returns LEFT JOIN clean_orders ON order_id` (LEFT JOIN preserves returns without matching orders)
- **Foreign Keys:** customer_sk → dim_customers, return_date_sk → dim_dates
- **Columns:**

| Column | Type | Role |
|--------|------|------|
| order_id | string | Degenerate dimension |
| customer_sk | long | FK → dim_customers.customer_sk (nullable from LEFT JOIN) |
| customer_id | string | Business key |
| return_date_sk | long | FK → dim_dates.date_sk |
| refund_amount | double | Measure: refund dollars |
| return_reason | string | Attribute |
| return_status | string | Attribute: Approved/Pending/Rejected |
| return_date | timestamp | Full timestamp |

---

## 4. Bridge Table

### bridge_return_products

**Why a bridge table is needed:**

In the Galaxy Schema, `fact_returns` captures refunds at the **order level** (one refund per returned order), while `fact_sales` captures revenue at the **line-item level** (one row per product in an order). This creates a **grain mismatch** — a single return with a $150 refund might cover an order containing 3 different products. Without the bridge table, we cannot answer critical questions like:
- "Which specific products are driving the highest return costs?"
- "Is a vendor's return problem caused by one bad product or across their entire catalog?"
- "Which product categories should be investigated for quality issues?"

The bridge table resolves this many-to-many relationship by **proportionally allocating** each refund across the products in that order based on their share of total order revenue. If Product A was 60% of the order value and Product B was 40%, the $150 refund is split as $90 and $60 respectively.

This directly enables the **Inventory Blindspot** business failure analysis (12–18% revenue lost from product misallocation) — without it, return costs cannot be traced back to specific products and vendors.

- **Grain:** One row per return × product combination
- **Source:** `gold.fact_returns INNER JOIN gold.fact_sales ON order_id` (reads from Gold for correct DLT lineage)

| Column | Type | Description |
|--------|------|-------------|
| order_id | string | The returned order |
| product_id | string | A product in that order |
| return_date_key | date | Date of return |
| return_reason | string | Reason for return |
| return_status | string | Approved/Pending/Rejected |
| refund_amount | double | Total refund for the order |
| line_sales | double | Sales amount for this line item |
| total_order_sales | double | Total sales for the entire order |
| allocation_weight | double | line_sales / total_order_sales (0-1) |
| allocated_refund | double | refund_amount × allocation_weight |

**Allocation Formula:**
```
allocation_weight = line_sales / total_order_sales
allocated_refund  = refund_amount × allocation_weight
```

---

## 5. Gold Materialized Views

### mv_revenue_by_region
- **Grain:** year × month × region
- **Source:** fact_sales + dim_customers + dim_dates
- **Measures:** order_count, total_sales, total_profit, total_quantity, unique_customers

### mv_return_rate_by_vendor
- **Grain:** 1 per vendor
- **Source:** fact_sales + fact_returns + dim_vendors
- **Measures:** total_orders, total_sales, return_order_count, total_refunded, return_rate_pct

### mv_slow_moving_products
- **Grain:** product × region
- **Source:** fact_sales + dim_products + dim_customers
- **Measures:** total_sales, total_quantity, order_count, total_profit, is_slow_moving (bottom 25% by quantity within region)

---

## 6. Metrics Layer (6 Aggregation Tables)

| Table | Business Failure | Grain | Key Measures |
|-------|-----------------|-------|--------------|
| `mv_monthly_revenue_by_region` | Revenue Audit | year × month × region | total_revenue, total_profit, order_count, avg_order_value |
| `mv_segment_profitability` | Revenue Audit | segment × region | total_revenue, total_profit, profit_margin_pct |
| `mv_customer_return_history` | Returns Fraud | 1 per customer | total_returns, total_refund_value, approval_rate_pct |
| `mv_return_reason_analysis` | Returns Fraud | return_reason × region | return_count, total_refunded, approved/rejected counts |
| `mv_vendor_product_performance` | Inventory Blindspot | vendor × product × region | total_sales, return_count, total_allocated_refund |
| `mv_product_return_impact` | Inventory Blindspot | product × region | total_return_cost, avg_return_cost |

---

## 7. Relationship Diagram

```
                    ┌──────────────┐
                    │  dim_dates   │
                    │  (date_key)  │
                    └──────┬───────┘
                           │
   ┌──────────────┐  ┌─────┴──────┐  ┌──────────────┐
   │dim_customers │──│ fact_sales  │──│ dim_products  │
   │(customer_id) │  │(order line) │  │(product_id)  │
   └──────┬───────┘  └─────┬──────┘  └──────┬───────┘
          │                │                 │
          │          ┌─────┴──────┐          │
          └──────────│fact_returns│          │
                     │(return evt)│          │
                     └─────┬──────┘          │
                           │                 │
                     ┌─────┴──────────┐      │
                     │bridge_return_  │──────┘
                     │   products     │
                     └────────────────┘
                           │
                     ┌─────┴──────┐
                     │dim_vendors │
                     │(vendor_id) │
                     └────────────┘
```

---

## 8. Query Walkthroughs

### Q1: "What is total revenue by region in 2018?"
```sql
SELECT c.region, SUM(f.sales) AS total_revenue
FROM globalmart.gold.fact_sales f
JOIN globalmart.gold.dim_customers c ON f.customer_sk = c.customer_sk
JOIN globalmart.gold.dim_dates d ON f.order_date_sk = d.date_sk
WHERE d.year = 2018
GROUP BY c.region
ORDER BY total_revenue DESC;
```
**Tables used:** fact_sales → dim_customers (for region) → dim_dates (for year filter)

### Q2: "Which customers have the highest return refunds?"
```sql
SELECT c.customer_name, c.region, SUM(r.refund_amount) AS total_refunded
FROM globalmart.gold.fact_returns r
JOIN globalmart.gold.dim_customers c ON r.customer_sk = c.customer_sk
GROUP BY c.customer_name, c.region
ORDER BY total_refunded DESC
LIMIT 10;
```
**Tables used:** fact_returns → dim_customers

### Q3: "What is the return rate by vendor?"
```sql
SELECT vendor_name, total_orders, return_order_count, return_rate_pct
FROM globalmart.gold.mv_return_rate_by_vendor
ORDER BY return_rate_pct DESC;
```
**Tables used:** Pre-computed MV (no joins needed)

### Q4: "Which products have the highest allocated return costs?"
```sql
SELECT p.product_name, SUM(b.allocated_refund) AS total_allocated_refund
FROM globalmart.gold.bridge_return_products b
JOIN globalmart.gold.dim_products p ON b.product_sk = p.product_sk
GROUP BY p.product_name
ORDER BY total_allocated_refund DESC
LIMIT 10;
```
**Tables used:** bridge_return_products → dim_products

### Q5: "Show slow-moving products by region"
```sql
SELECT product_name, region, total_sales, total_quantity, is_slow_moving
FROM globalmart.gold.mv_slow_moving_products
WHERE is_slow_moving = TRUE
ORDER BY total_quantity ASC;
```
**Tables used:** Pre-computed MV

---

## 9. GenAI Output Tables (Gold Schema)

Four additional tables are written to `globalmart.gold` by the GenAI notebooks (05–08).
They are NOT part of the DLT pipeline — they are created by the Workflow job after the pipeline completes.

| Table | Notebook | Description |
|-------|----------|-------------|
| `dq_audit_report` | 05_uc1_dq_reporter | AI-generated data quality findings. One row per (entity, field, issue_type). Columns: entity, affected_field, issue_type, rejected_count, total_entity_records, rejection_rate_pct, sample_records, ai_explanation, generated_at. |
| `flagged_return_customers` | 06_uc2_fraud_investigator | Fraud-scored customers (anomaly_score ≥ 40). Columns: customer_id, customer_name, segment, region, total_returns, total_refund_value, avg_refund_per_return, approval_rate, anomaly_score, rules_violated, investigation_brief, generated_at. |
| `rag_query_history` | 07_uc3_product_rag | Product intelligence Q&A log. Columns: question, answer, retrieved_documents, retrieved_count, top_distance, generated_at. |
| `ai_business_insights` | 08_uc4_executive_intel | LLM executive summaries. insight_type values: revenue_performance, vendor_return_rate, slow_moving_inventory. Columns: insight_type, executive_summary, kpi_data, generated_at. |

**Total Gold objects:** 10 DLT tables + 4 GenAI output tables = **14 tables in globalmart.gold**

---

## 10. Business Failure Resolution

### Business Failure 1: Revenue Audit (9% Overstatement)
- **Root Cause:** Duplicate customers across 6 regional systems inflated revenue figures
- **Solution:** `dim_customers` deduplicates 748 records to 374 unique customers using ROW_NUMBER. `mv_revenue_by_region` provides accurate per-region revenue after dedup. `mv_segment_profitability` shows true profitability by segment.

### Business Failure 2: Returns Fraud ($2.3M Annual Loss)
- **Root Cause:** No cross-region visibility into customer return patterns
- **Solution:** `fact_returns` centralizes all returns. `mv_return_rate_by_vendor` reveals vendor-level exposure. `mv_customer_return_history` profiles every customer's return behavior. The Fraud Investigator (UC2) scores anomalies across all regions.

### Business Failure 3: Inventory Blindspot (12-18% Revenue Lost)
- **Root Cause:** Products misallocated across regions with no visibility into regional demand
- **Solution:** `mv_slow_moving_products` flags bottom-25% products per region. `bridge_return_products` allocates return costs to specific products. `mv_vendor_product_performance` shows which vendor-product combinations underperform by region.
