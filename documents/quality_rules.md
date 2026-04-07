# GlobalMart Data Quality Rules

## Overview

The Silver harmonization layer applies **23 quality rules** across 6 entities. Each rule is implemented as an independent boolean `_fail_*` flag. Records with ANY failure are routed to the `rejected_records` quarantine table; clean records pass to `clean_*` tables.

**Enforcement approach:** DLT `@dlt.expect()` annotations track quality metrics in the pipeline UI, while `.filter(_has_any_failure == False)` enforces actual record routing. This gives both visibility and control.

---

## Rules by Entity

### Customers (4 Rules)

| # | Rule | Field | Trigger Condition | Issue Type | Business Justification |
|---|------|-------|--------------------|------------|----------------------|
| 1 | Customer ID required | `customer_id` | NULL after COALESCE of `customer_id`, `CustomerID`, `cust_id`, `customer_identifier` | `missing_value` | Cannot link orders, returns, or revenue without customer identity |
| 2 | Customer name required | `customer_name` | NULL after COALESCE of `customer_name`, `full_name` | `missing_value` | Unnamed customers cannot appear in fraud reports or executive summaries |
| 3 | Valid segment | `segment` | Not in (Consumer, Corporate, Home Office) after mapping CONS→Consumer, CORP→Corporate, HO→Home Office, Cosumer→Consumer | `invalid_value` | Segment profitability analysis requires standardized segment values |
| 4 | Valid region | `region` | Not in (East, West, South, Central, North) after mapping W→West, S→South | `invalid_value` | Regional revenue audit requires known region assignments |

### Orders (5 Rules)

| # | Rule | Field | Trigger Condition | Issue Type | Business Justification |
|---|------|-------|--------------------|------------|----------------------|
| 5 | Order ID required | `order_id` | NULL | `missing_value` | Order ID is the join key to transactions and returns |
| 6 | Customer ID required | `customer_id` | NULL | `missing_value` | Orphan orders cannot be attributed to customers for revenue reporting |
| 7 | Valid ship mode | `ship_mode` | Not in (First Class, Second Class, Standard Class, Same Day) after mapping 1st Class→First Class, 2nd Class→Second Class, Std Class→Standard Class | `invalid_value` | Ship mode analysis requires standardized values |
| 8 | Valid order status | `order_status` | Not in (canceled, created, delivered, invoiced, processing, shipped, unavailable) | `invalid_value` | Order lifecycle reporting needs known statuses |
| 9 | Parseable order date | `order_purchase_date` | Cannot parse as timestamp (tried: MM/dd/yyyy HH:mm, yyyy-MM-dd HH:mm, MM/dd/yyyy, yyyy-MM-dd) | `parse_failure` | Time-series revenue analysis requires valid dates |

### Transactions (5 Rules)

| # | Rule | Field | Trigger Condition | Issue Type | Business Justification |
|---|------|-------|--------------------|------------|----------------------|
| 10 | Order ID required | `order_id` | NULL after COALESCE of `order_id`, `Order_id`, `Order_ID` | `missing_value` | Transaction must link to an order for revenue calculation |
| 11 | Product ID required | `product_id` | NULL after COALESCE of `product_id`, `Product_id`, `Product_ID` | `missing_value` | Product-level analysis requires product identification |
| 12 | Non-negative sales | `sales` | NULL or < 0 after stripping currency symbols | `out_of_range` | Negative sales would understate revenue |
| 13 | Positive quantity | `quantity` | NULL or <= 0 | `out_of_range` | Zero/negative quantities are invalid for unit economics |
| 14 | Discount in range 0-1 | `discount` | NULL or < 0 or > 1 after converting percentage strings (strip %, divide by 100) | `out_of_range` | Discounts outside 0-100% distort margin calculations |

### Returns (5 Rules)

| # | Rule | Field | Trigger Condition | Issue Type | Business Justification |
|---|------|-------|--------------------|------------|----------------------|
| 15 | Order ID required | `order_id` | NULL after COALESCE of `order_id`, `OrderId` | `missing_value` | Returns must link to orders for cross-region fraud detection |
| 16 | Non-negative refund | `refund_amount` | NULL or < 0 (handles '?' values as NULL) | `out_of_range` | Negative refunds are invalid; unknown amounts cannot be aggregated |
| 17 | Valid return status | `return_status` | Not in (Approved, Pending, Rejected) after mapping APPRVD→Approved, PENDG→Pending, RJCTD→Rejected | `invalid_value` | Return status tracking requires standardized values |
| 18 | Valid return reason | `return_reason` | NULL or 'Unknown' (maps '?' to 'Unknown' first) | `invalid_value` | Return reason analysis needs specific reasons, not unknowns |
| 19 | Parseable return date | `return_date` | Cannot parse as timestamp (tried: yyyy-MM-dd, MM-dd-yyyy, MM/dd/yyyy) | `parse_failure` | Time-based fraud pattern detection requires valid dates |

### Products (2 Rules)

| # | Rule | Field | Trigger Condition | Issue Type | Business Justification |
|---|------|-------|--------------------|------------|----------------------|
| 20 | Product ID required | `product_id` | NULL | `missing_value` | Product ID is the primary key for dimensional model |
| 21 | Product name required | `product_name` | NULL | `missing_value` | Unnamed products cannot appear in inventory reports |

### Vendors (2 Rules)

| # | Rule | Field | Trigger Condition | Issue Type | Business Justification |
|---|------|-------|--------------------|------------|----------------------|
| 22 | Vendor ID required | `vendor_id` | NULL after TRIM | `missing_value` | Vendor ID is the primary key for vendor analysis |
| 23 | Vendor name required | `vendor_name` | NULL after TRIM | `missing_value` | Unnamed vendors cannot appear in procurement reports |

---

## Issue Type Summary

| Issue Type | Count | Description |
|------------|-------|-------------|
| `missing_value` | 12 | Required field is NULL after all COALESCE attempts |
| `invalid_value` | 6 | Value not in the allowed set after all mappings |
| `out_of_range` | 3 | Numeric value outside acceptable bounds |
| `parse_failure` | 2 | Date/timestamp cannot be parsed with any known format |
| **Total** | **23** | |

---

## Rejected Records Schema

Each quality failure generates one row in `globalmart.silver.rejected_records`:

| Column | Type | Description |
|--------|------|-------------|
| source_table | string | Target clean table (e.g., `clean_customers`) |
| record_key | string | Primary key value of the failed record |
| affected_field | string | Column that failed validation |
| issue_type | string | One of: missing_value, invalid_value, out_of_range, parse_failure |
| record_data | string | JSON of the full record's raw values |
| _source_file | string | Original source file path |
| rejected_at | timestamp | Timestamp of rejection |

A record failing 3 rules produces 3 rows in `rejected_records`.

---

## Pipeline Audit Log

The `globalmart.silver.pipeline_audit_log` materialized view reconciles counts:

```
bronze_count = silver_count + rejected_count
```

This ensures no records are lost during harmonization.

---

## How Databricks AI/BI Genie Helped

Databricks AI/BI Genie was used to run natural language queries against the rejected_records table to identify patterns in data quality failures. Queries like "Which entity has the most rejections?" and "What are the most common issue types?" helped prioritize data steward investigations and validate that quality rules were firing correctly across all source files.
