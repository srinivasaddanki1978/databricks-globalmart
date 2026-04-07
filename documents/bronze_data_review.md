# GlobalMart Bronze Data Review

## Overview

This document catalogs all data quality issues discovered during Bronze layer inspection of the 16 source files across 6 regional systems. Each issue is documented with the affected entity, specific problem, source file(s), and the cleaning approach applied in Silver harmonization.

---

## Issues by Entity

### Customers (8 Issues)

| # | Issue | Source File(s) | Cleaning Approach |
|---|-------|----------------|-------------------|
| 1 | **Four different ID column names**: `customer_id`, `CustomerID`, `cust_id`, `customer_identifier` across regions | customers_1.csv through customers_6.csv | COALESCE all 4 variants into unified `customer_id` |
| 2 | **Two different name columns**: `customer_name` vs `full_name` | Various region files | COALESCE `customer_name`, `full_name` |
| 3 | **Two different email columns**: `customer_email` vs `email_address` | Various region files | COALESCE `customer_email`, `email_address` |
| 4 | **Two different segment columns**: `segment` vs `customer_segment` | Various region files | COALESCE `segment`, `customer_segment` |
| 5 | **City and state values swapped** in Region 4 | customers_4.csv | Detect via `_source_file` containing `"Region 4"`, `"Region4"`, or `"Region%204"` and swap city↔state |
| 6 | **Segment abbreviations**: CONS, CORP, HO used instead of full names | Various region files | Map CONS→Consumer, CORP→Corporate, HO→Home Office |
| 7 | **Segment typo**: "Cosumer" instead of "Consumer" | Various region files | Map Cosumer→Consumer |
| 8 | **Region abbreviations**: W, S used instead of West, South | Various region files | Map W→West, S→South |

### Orders (4 Issues)

| # | Issue | Source File(s) | Cleaning Approach |
|---|-------|----------------|-------------------|
| 9 | **Date format mismatch**: MM/dd/yyyy HH:mm in some files, yyyy-MM-dd HH:mm in others | orders_1.csv, orders_2.csv, orders_3.csv | COALESCE across multiple `to_timestamp` format attempts |
| 10 | **Ship mode abbreviations**: "1st Class", "2nd Class", "Std Class" instead of standard names | Various order files | Map to First Class, Second Class, Standard Class |
| 11 | **Inconsistent order status casing**: Mixed case in status values | Various order files | Apply `lower(trim())` to normalize |
| 12 | **Some order_purchase_date values unparseable** with any known format | Scattered records | Route to rejected_records with `parse_failure` |

### Transactions (5 Issues)

| # | Issue | Source File(s) | Cleaning Approach |
|---|-------|----------------|-------------------|
| 13 | **Column casing mismatch**: `Order_id` vs `Order_ID` across files | transactions_1.csv, transactions_2.csv, transactions_3.csv | COALESCE `order_id`, `Order_id`, `Order_ID` |
| 14 | **Product ID casing mismatch**: `Product_id` vs `Product_ID` | Various transaction files | COALESCE `product_id`, `Product_id`, `Product_ID` |
| 15 | **Discount as percentage strings**: Values like "20%" instead of 0.20 | Various transaction files | Strip '%', divide by 100 |
| 16 | **Currency symbols in sales**: Values like "$150.00" instead of numeric | Various transaction files | `regexp_replace` to strip non-numeric characters |
| 17 | **Files split across Region folders and root**: 2 files in Region folders, 1 at data root | Region*/transactions_*.csv + transactions_3.csv | Two-stream union with `unionByName(allowMissingColumns=True)` |

### Returns (6 Issues)

| # | Issue | Source File(s) | Cleaning Approach |
|---|-------|----------------|-------------------|
| 18 | **Every column name differs between files**: `order_id`/`OrderId`, `return_reason`/`reason`, `return_status`/`status`, `refund_amount`/`RefundAmount`, `return_date`/`ReturnDate` | returns_1.json, returns_2.json | COALESCE all 5 column pairs |
| 19 | **Return status abbreviations**: APPRVD, PENDG, RJCTD instead of full names | Various return files | Map APPRVD→Approved, PENDG→Pending, RJCTD→Rejected |
| 20 | **Question mark values**: '?' used for unknown refund amounts and return reasons | Various return files | Map '?' refund_amount to NULL, '?' return_reason to 'Unknown' (then reject) |
| 21 | **Date format mismatch**: yyyy-MM-dd in one file, MM-dd-yyyy in another | returns_1.json, returns_2.json | COALESCE across multiple `to_timestamp` format attempts |
| 22 | **Files split across Region6 and root**: 1 JSON in Region6, 1 at data root | Region6/returns_1.json + returns_2.json | Two-stream union with `unionByName(allowMissingColumns=True)` |
| 23 | **JSON format** (not CSV like most other files) | Both return files | Auto Loader configured with `cloudFiles.format = json` |

### Products (2 Issues)

| # | Issue | Source File(s) | Cleaning Approach |
|---|-------|----------------|-------------------|
| 24 | **UPC in scientific notation**: Values like 1.23456E+11 instead of full barcode | products.json | `cast(double).cast(long).cast(string)` conversion chain |
| 25 | **High NULL rates** in optional columns | products.json | Preserve NULLs; only reject if product_id or product_name is NULL |

### Vendors (2 Issues)

| # | Issue | Source File(s) | Cleaning Approach |
|---|-------|----------------|-------------------|
| 26 | **Leading/trailing whitespace** in vendor_id and vendor_name | vendors.csv | `trim()` on all string fields |
| 27 | **Relatively clean data** — minimal issues compared to other entities | vendors.csv | Standard trim and NULL checks sufficient |

---

## Summary Statistics

| Entity | Source Files | Format | Issue Count | Key Challenge |
|--------|-------------|--------|-------------|---------------|
| Customers | 6 CSV | CSV | 8 | Column name chaos (4 ID variants), city/state swap |
| Orders | 3 CSV | CSV | 4 | Date format mismatch, ship mode abbreviations |
| Transactions | 3 CSV | CSV | 5 | Column casing, percentage discount strings |
| Returns | 2 JSON | JSON | 6 | Every column name differs, question mark values |
| Products | 1 JSON | JSON | 2 | UPC scientific notation |
| Vendors | 1 CSV | CSV | 2 | Whitespace (minimal issues) |
| **Total** | **16 files** | | **27 issues** | |

---

## Auto Loader Configuration

All Bronze tables use Auto Loader (`cloudFiles`) with:
- `cloudFiles.schemaEvolutionMode = addNewColumns` — accommodates different column names across regional files
- `readerCaseSensitive = false` — handles casing differences
- `header = true` — all CSV files have headers
- `inferSchema = true` — automatic type inference
- Schema checkpoint locations under `{SOURCE_BASE}/_checkpoints/{entity}`

Transactions and Returns each require **two separate streams** (regional + root) unioned via `unionByName(allowMissingColumns=True)` because their source files are split across subdirectories and the root `data/` folder.
