# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02 — Silver Harmonization (DLT)
# MAGIC
# MAGIC Applies 23 quality rules across 6 entities. Each rule is an independent boolean
# MAGIC `_fail_*` flag. Records with ANY failure are routed to `rejected_records`;
# MAGIC clean records pass through to `clean_*` tables.
# MAGIC
# MAGIC - **No deduplication** — dedup is handled in Gold (`dim_customers`).
# MAGIC - `rejected_records` uses `@dlt.append_flow` so each entity can append independently.
# MAGIC - `pipeline_audit_log` is a materialized view comparing bronze vs silver counts.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, coalesce, when, lit, trim, lower, upper, regexp_replace,
    to_timestamp, current_timestamp, to_json, struct,
    concat_ws, expr, length, array,
)
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

# COMMAND ----------

# ============================================================================
# CUSTOMERS — 4 quality rules
# ============================================================================

# COMMAND ----------

@dlt.table(
    name="silver.clean_customers",
    comment="Validated and harmonized customer records (duplicates preserved for Gold dedup)",
    table_properties={"quality": "silver"},

)
@dlt.expect("customer_id_present", "NOT _fail_customer_id_null")
@dlt.expect("customer_name_present", "NOT _fail_customer_name_null")
@dlt.expect("valid_segment", "NOT _fail_invalid_segment")
@dlt.expect("valid_region", "NOT _fail_invalid_region")
def clean_customers():
    raw = dlt.readStream("bronze.raw_customers")

    harmonized = raw.select(
        # COALESCE 4 ID variants
        coalesce(
            col("customer_id"), col("CustomerID"),
            col("cust_id"), col("customer_identifier")
        ).alias("customer_id"),
        # COALESCE name
        coalesce(col("customer_name"), col("full_name")).alias("customer_name"),
        # COALESCE email
        coalesce(col("customer_email"), col("email_address")).alias("customer_email"),
        # COALESCE segment
        coalesce(col("segment"), col("customer_segment")).alias("_raw_segment"),
        col("city").alias("_raw_city"),
        col("state").alias("_raw_state"),
        col("region").alias("_raw_region"),
        col("_source_file"),
        col("_load_timestamp"),
    )

    # Swap city/state for Region 4
    swapped = harmonized.withColumn(
        "city",
        when(col("_source_file").contains("Region 4"), col("_raw_state"))
        .when(col("_source_file").contains("Region%204"), col("_raw_state"))
        .when(col("_source_file").contains("Region4"), col("_raw_state"))
        .otherwise(col("_raw_city"))
    ).withColumn(
        "state",
        when(col("_source_file").contains("Region 4"), col("_raw_city"))
        .when(col("_source_file").contains("Region%204"), col("_raw_city"))
        .when(col("_source_file").contains("Region4"), col("_raw_city"))
        .otherwise(col("_raw_state"))
    )

    # Map segment abbreviations
    mapped = swapped.withColumn(
        "segment",
        when(upper(trim(col("_raw_segment"))) == "CONS", "Consumer")
        .when(upper(trim(col("_raw_segment"))) == "CORP", "Corporate")
        .when(upper(trim(col("_raw_segment"))) == "HO", "Home Office")
        .when(lower(trim(col("_raw_segment"))) == "cosumer", "Consumer")
        .when(trim(col("_raw_segment")).isin("Consumer", "Corporate", "Home Office"), trim(col("_raw_segment")))
        .otherwise(trim(col("_raw_segment")))
    )

    # Map region abbreviations
    mapped2 = mapped.withColumn(
        "region",
        when(upper(trim(col("_raw_region"))) == "W", "West")
        .when(upper(trim(col("_raw_region"))) == "S", "South")
        .when(trim(col("_raw_region")).isin("East", "West", "South", "Central", "North"), trim(col("_raw_region")))
        .otherwise(trim(col("_raw_region")))
    )

    # Apply quality rules as boolean flags
    flagged = mapped2.withColumn(
        "_fail_customer_id_null", col("customer_id").isNull()
    ).withColumn(
        "_fail_customer_name_null", col("customer_name").isNull()
    ).withColumn(
        "_fail_invalid_segment",
        col("segment").isNull() | ~col("segment").isin("Consumer", "Corporate", "Home Office")
    ).withColumn(
        "_fail_invalid_region",
        col("region").isNull() | ~col("region").isin("East", "West", "South", "Central", "North")
    ).withColumn(
        "_has_any_failure",
        col("_fail_customer_id_null") | col("_fail_customer_name_null") |
        col("_fail_invalid_segment") | col("_fail_invalid_region")
    )

    return flagged.filter(col("_has_any_failure") == False).select(
        "customer_id", "customer_name", "customer_email", "segment",
        "city", "state", "region", "_source_file", "_load_timestamp",
        "_fail_customer_id_null", "_fail_customer_name_null",
        "_fail_invalid_segment", "_fail_invalid_region",
    )

# COMMAND ----------

# ============================================================================
# ORDERS — 5 quality rules
# ============================================================================

# COMMAND ----------

@dlt.table(
    name="silver.clean_orders",
    comment="Validated and harmonized order records",
    table_properties={"quality": "silver"},

)
@dlt.expect("order_id_present", "NOT _fail_order_id_null")
@dlt.expect("customer_id_present", "NOT _fail_customer_id_null")
@dlt.expect("valid_ship_mode", "NOT _fail_invalid_ship_mode")
@dlt.expect("valid_order_status", "NOT _fail_invalid_order_status")
@dlt.expect("parseable_order_date", "NOT _fail_unparseable_date")
def clean_orders():
    raw = dlt.readStream("bronze.raw_orders")

    harmonized = raw.select(
        col("order_id"),
        col("customer_id"),
        col("vendor_id"),
        trim(col("ship_mode")).alias("_raw_ship_mode"),
        lower(trim(col("order_status"))).alias("_raw_order_status"),
        col("order_purchase_date").cast("string").alias("_raw_order_date"),
        col("_source_file"),
        col("_load_timestamp"),
    )

    # Map ship mode abbreviations
    mapped = harmonized.withColumn(
        "ship_mode",
        when(col("_raw_ship_mode").isin("1st Class", "1st"), "First Class")
        .when(col("_raw_ship_mode").isin("2nd Class", "2nd"), "Second Class")
        .when(col("_raw_ship_mode").isin("Std Class", "Std"), "Standard Class")
        .when(col("_raw_ship_mode").isin("First Class", "Second Class", "Standard Class", "Same Day"),
              col("_raw_ship_mode"))
        .otherwise(col("_raw_ship_mode"))
    )

    # Map order status
    mapped2 = mapped.withColumn(
        "order_status",
        col("_raw_order_status")
    )

    # Parse dates: try multiple formats
    parsed = mapped2.withColumn(
        "order_purchase_date",
        coalesce(
            to_timestamp(col("_raw_order_date"), "MM/dd/yyyy HH:mm"),
            to_timestamp(col("_raw_order_date"), "yyyy-MM-dd HH:mm"),
            to_timestamp(col("_raw_order_date"), "MM/dd/yyyy"),
            to_timestamp(col("_raw_order_date"), "yyyy-MM-dd"),
            to_timestamp(col("_raw_order_date"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
    )

    valid_statuses = [
        "canceled", "created", "delivered", "invoiced",
        "processing", "shipped", "unavailable"
    ]

    flagged = parsed.withColumn(
        "_fail_order_id_null", col("order_id").isNull()
    ).withColumn(
        "_fail_customer_id_null", col("customer_id").isNull()
    ).withColumn(
        "_fail_invalid_ship_mode",
        col("ship_mode").isNull() | ~col("ship_mode").isin(
            "First Class", "Second Class", "Standard Class", "Same Day"
        )
    ).withColumn(
        "_fail_invalid_order_status",
        col("order_status").isNull() | ~col("order_status").isin(*valid_statuses)
    ).withColumn(
        "_fail_unparseable_date",
        col("order_purchase_date").isNull()
    ).withColumn(
        "_has_any_failure",
        col("_fail_order_id_null") | col("_fail_customer_id_null") |
        col("_fail_invalid_ship_mode") | col("_fail_invalid_order_status") |
        col("_fail_unparseable_date")
    )

    return flagged.filter(col("_has_any_failure") == False).select(
        "order_id", "customer_id", "vendor_id", "ship_mode", "order_status",
        "order_purchase_date",
        "_raw_order_date", "_source_file", "_load_timestamp",
        "_fail_order_id_null", "_fail_customer_id_null",
        "_fail_invalid_ship_mode", "_fail_invalid_order_status", "_fail_unparseable_date",
    )

# COMMAND ----------

# ============================================================================
# TRANSACTIONS — 5 quality rules
# ============================================================================

# COMMAND ----------

@dlt.table(
    name="silver.clean_transactions",
    comment="Validated and harmonized transaction line items",
    table_properties={"quality": "silver"},

)
@dlt.expect("order_id_present", "NOT _fail_order_id_null")
@dlt.expect("product_id_present", "NOT _fail_product_id_null")
@dlt.expect("non_negative_sales", "NOT _fail_negative_sales")
@dlt.expect("positive_quantity", "NOT _fail_non_positive_quantity")
@dlt.expect("valid_discount_range", "NOT _fail_discount_out_of_range")
def clean_transactions():
    raw = dlt.readStream("bronze.raw_transactions")

    harmonized = raw.select(
        # COALESCE order ID variants
        coalesce(col("order_id"), col("Order_id"), col("Order_ID")).alias("order_id"),
        # COALESCE product ID variants
        coalesce(col("product_id"), col("Product_id"), col("Product_ID")).alias("product_id"),
        # Strip currency symbols and cast sales
        regexp_replace(
            coalesce(col("sales"), col("Sales")).cast("string"),
            "[^0-9.\\-]", ""
        ).alias("_raw_sales"),
        coalesce(col("quantity"), col("Quantity")).cast("string").alias("_raw_quantity"),
        coalesce(col("discount"), col("Discount")).cast("string").alias("_raw_discount"),
        col("profit"),
        col("payment_type"),
        col("_source_file"),
        col("_load_timestamp"),
    )

    # Convert discount: strip '%' and divide by 100 if needed
    converted = harmonized.withColumn(
        "_discount_is_pct", col("_raw_discount").contains("%")
    ).withColumn(
        "_discount_cleaned",
        regexp_replace(col("_raw_discount"), "[%]", "").cast("double")
    ).withColumn(
        "discount",
        when(col("_discount_is_pct"), col("_discount_cleaned") / 100.0)
        .otherwise(col("_discount_cleaned"))
    ).withColumn(
        "sales", col("_raw_sales").cast("double")
    ).withColumn(
        "quantity", col("_raw_quantity").cast("int")
    )

    flagged = converted.withColumn(
        "_fail_order_id_null", col("order_id").isNull()
    ).withColumn(
        "_fail_product_id_null", col("product_id").isNull()
    ).withColumn(
        "_fail_negative_sales",
        col("sales").isNull() | (col("sales") < 0)
    ).withColumn(
        "_fail_non_positive_quantity",
        col("quantity").isNull() | (col("quantity") <= 0)
    ).withColumn(
        "_fail_discount_out_of_range",
        col("discount").isNull() | (col("discount") < 0) | (col("discount") > 1)
    ).withColumn(
        "_has_any_failure",
        col("_fail_order_id_null") | col("_fail_product_id_null") |
        col("_fail_negative_sales") | col("_fail_non_positive_quantity") |
        col("_fail_discount_out_of_range")
    )

    return flagged.filter(col("_has_any_failure") == False).select(
        "order_id", "product_id", "sales", "quantity",
        "discount", "profit", "payment_type",
        "_raw_sales", "_raw_quantity", "_raw_discount",
        "_source_file", "_load_timestamp",
        "_fail_order_id_null", "_fail_product_id_null",
        "_fail_negative_sales", "_fail_non_positive_quantity",
        "_fail_discount_out_of_range",
    )

# COMMAND ----------

# ============================================================================
# RETURNS — 5 quality rules
# ============================================================================

# COMMAND ----------

@dlt.table(
    name="silver.clean_returns",
    comment="Validated and harmonized return records",
    table_properties={"quality": "silver"},

)
@dlt.expect("order_id_present", "NOT _fail_order_id_null")
@dlt.expect("non_negative_refund", "NOT _fail_negative_refund")
@dlt.expect("valid_return_status", "NOT _fail_invalid_return_status")
@dlt.expect("valid_return_reason", "NOT _fail_invalid_return_reason")
@dlt.expect("parseable_return_date", "NOT _fail_unparseable_return_date")
def clean_returns():
    raw = dlt.readStream("bronze.raw_returns")

    harmonized = raw.select(
        # COALESCE order ID variants
        coalesce(col("order_id"), col("OrderId")).alias("order_id"),
        # COALESCE return reason variants
        coalesce(col("return_reason"), col("reason")).alias("_raw_return_reason"),
        # COALESCE return status variants
        coalesce(col("return_status"), col("status")).alias("_raw_return_status"),
        # COALESCE refund amount variants
        coalesce(col("refund_amount"), col("amount")).cast("string").alias("_raw_refund_amount"),
        # COALESCE return date variants
        coalesce(col("return_date"), col("date_of_return")).cast("string").alias("_raw_return_date"),
        col("_source_file"),
        col("_load_timestamp"),
    )

    # Map status abbreviations
    mapped = harmonized.withColumn(
        "return_status",
        when(upper(trim(col("_raw_return_status"))) == "APPRVD", "Approved")
        .when(upper(trim(col("_raw_return_status"))) == "PENDG", "Pending")
        .when(upper(trim(col("_raw_return_status"))) == "RJCTD", "Rejected")
        .when(trim(col("_raw_return_status")).isin("Approved", "Pending", "Rejected"),
              trim(col("_raw_return_status")))
        .otherwise(trim(col("_raw_return_status")))
    )

    # Map '?' return_reason to 'Unknown'
    mapped2 = mapped.withColumn(
        "return_reason",
        when(trim(col("_raw_return_reason")) == "?", "Unknown")
        .otherwise(trim(col("_raw_return_reason")))
    )

    # Parse refund amount — handle '?' values
    converted = mapped2.withColumn(
        "refund_amount",
        when(col("_raw_refund_amount") == "?", None)
        .otherwise(regexp_replace(col("_raw_refund_amount"), "[^0-9.\\-]", "").cast("double"))
    )

    # Parse return date: try multiple formats
    parsed = converted.withColumn(
        "return_date",
        coalesce(
            to_timestamp(col("_raw_return_date"), "yyyy-MM-dd"),
            to_timestamp(col("_raw_return_date"), "MM-dd-yyyy"),
            to_timestamp(col("_raw_return_date"), "MM/dd/yyyy"),
            to_timestamp(col("_raw_return_date"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
    )

    flagged = parsed.withColumn(
        "_fail_order_id_null", col("order_id").isNull()
    ).withColumn(
        "_fail_negative_refund",
        col("refund_amount").isNull() | (col("refund_amount") < 0)
    ).withColumn(
        "_fail_invalid_return_status",
        col("return_status").isNull() | ~col("return_status").isin("Approved", "Pending", "Rejected")
    ).withColumn(
        "_fail_invalid_return_reason",
        col("return_reason").isNull() | (col("return_reason") == "Unknown")
    ).withColumn(
        "_fail_unparseable_return_date",
        col("return_date").isNull()
    ).withColumn(
        "_has_any_failure",
        col("_fail_order_id_null") | col("_fail_negative_refund") |
        col("_fail_invalid_return_status") | col("_fail_invalid_return_reason") |
        col("_fail_unparseable_return_date")
    )

    return flagged.filter(col("_has_any_failure") == False).select(
        "order_id", "return_reason", "return_status", "refund_amount", "return_date",
        "_raw_return_reason", "_raw_return_status", "_raw_refund_amount", "_raw_return_date",
        "_source_file", "_load_timestamp",
        "_fail_order_id_null", "_fail_negative_refund",
        "_fail_invalid_return_status", "_fail_invalid_return_reason",
        "_fail_unparseable_return_date",
    )

# COMMAND ----------

# ============================================================================
# PRODUCTS — 2 quality rules
# ============================================================================

# COMMAND ----------

@dlt.table(
    name="silver.clean_products",
    comment="Validated product catalog with UPC corrected from scientific notation",
    table_properties={"quality": "silver"},

)
@dlt.expect("product_id_present", "NOT _fail_product_id_null")
@dlt.expect("product_name_present", "NOT _fail_product_name_null")
def clean_products():
    raw = dlt.readStream("bronze.raw_products")

    harmonized = raw.select(
        col("product_id"),
        trim(col("product_name")).alias("product_name"),
        trim(col("brand")).alias("brand"),
        trim(col("categories")).alias("category"),
        # Fix UPC from scientific notation: cast to double → long → string
        col("upc").cast("double").cast("long").cast("string").alias("upc"),
        col("_source_file"),
        col("_load_timestamp"),
    )

    flagged = harmonized.withColumn(
        "_fail_product_id_null", col("product_id").isNull()
    ).withColumn(
        "_fail_product_name_null", col("product_name").isNull()
    ).withColumn(
        "_has_any_failure",
        col("_fail_product_id_null") | col("_fail_product_name_null")
    )

    return flagged.filter(col("_has_any_failure") == False).select(
        "product_id", "product_name", "brand", "category", "upc",
        "_source_file", "_load_timestamp",
        "_fail_product_id_null", "_fail_product_name_null",
    )

# COMMAND ----------

# ============================================================================
# VENDORS — 2 quality rules
# ============================================================================

# COMMAND ----------

@dlt.table(
    name="silver.clean_vendors",
    comment="Validated vendor reference data",
    table_properties={"quality": "silver"},

)
@dlt.expect("vendor_id_present", "NOT _fail_vendor_id_null")
@dlt.expect("vendor_name_present", "NOT _fail_vendor_name_null")
def clean_vendors():
    raw = dlt.readStream("bronze.raw_vendors")

    harmonized = raw.select(
        trim(col("vendor_id")).alias("vendor_id"),
        trim(col("vendor_name")).alias("vendor_name"),
        col("_source_file"),
        col("_load_timestamp"),
    )

    flagged = harmonized.withColumn(
        "_fail_vendor_id_null", col("vendor_id").isNull()
    ).withColumn(
        "_fail_vendor_name_null", col("vendor_name").isNull()
    ).withColumn(
        "_has_any_failure",
        col("_fail_vendor_id_null") | col("_fail_vendor_name_null")
    )

    return flagged.filter(col("_has_any_failure") == False).select(
        "vendor_id", "vendor_name",
        "_source_file", "_load_timestamp",
        "_fail_vendor_id_null", "_fail_vendor_name_null",
    )

# COMMAND ----------

# ============================================================================
# REJECTED RECORDS — append_flow from each entity
# ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rejected Records (Quarantine Table)
# MAGIC One row per quality rule failure. A record failing 3 rules produces 3 rows.
# MAGIC Uses `@dlt.append_flow` so each entity appends independently.

# COMMAND ----------

dlt.create_streaming_table(
    name="silver.rejected_records",
    comment="Quarantine table — one row per quality rule failure across all entities",
    table_properties={"quality": "silver"},

)

# COMMAND ----------

# --- Rejected: Customers ---
@dlt.append_flow(target="silver.rejected_records", name="rej_customers")
def rej_customers():
    raw = dlt.readStream("bronze.raw_customers")

    harmonized = raw.select(
        coalesce(col("customer_id"), col("CustomerID"),
                 col("cust_id"), col("customer_identifier")).alias("customer_id"),
        coalesce(col("customer_name"), col("full_name")).alias("customer_name"),
        coalesce(col("customer_email"), col("email_address")).alias("customer_email"),
        coalesce(col("segment"), col("customer_segment")).alias("_raw_segment"),
        col("region").alias("_raw_region"),
        col("_source_file"),
        col("_load_timestamp"),
    )

    mapped = harmonized.withColumn(
        "segment",
        when(upper(trim(col("_raw_segment"))) == "CONS", "Consumer")
        .when(upper(trim(col("_raw_segment"))) == "CORP", "Corporate")
        .when(upper(trim(col("_raw_segment"))) == "HO", "Home Office")
        .when(lower(trim(col("_raw_segment"))) == "cosumer", "Consumer")
        .when(trim(col("_raw_segment")).isin("Consumer", "Corporate", "Home Office"), trim(col("_raw_segment")))
        .otherwise(trim(col("_raw_segment")))
    ).withColumn(
        "region",
        when(upper(trim(col("_raw_region"))) == "W", "West")
        .when(upper(trim(col("_raw_region"))) == "S", "South")
        .when(trim(col("_raw_region")).isin("East", "West", "South", "Central", "North"), trim(col("_raw_region")))
        .otherwise(trim(col("_raw_region")))
    )

    flagged = mapped.withColumn(
        "_fail_customer_id_null", col("customer_id").isNull()
    ).withColumn(
        "_fail_customer_name_null", col("customer_name").isNull()
    ).withColumn(
        "_fail_invalid_segment",
        col("segment").isNull() | ~col("segment").isin("Consumer", "Corporate", "Home Office")
    ).withColumn(
        "_fail_invalid_region",
        col("region").isNull() | ~col("region").isin("East", "West", "South", "Central", "North")
    ).withColumn(
        "_has_any_failure",
        col("_fail_customer_id_null") | col("_fail_customer_name_null") |
        col("_fail_invalid_segment") | col("_fail_invalid_region")
    ).withColumn(
        "record_data", to_json(struct(
            coalesce(col("customer_id").cast("string"), lit("NULL")).alias("customer_id"),
            coalesce(col("customer_name").cast("string"), lit("NULL")).alias("customer_name"),
            coalesce(col("customer_email").cast("string"), lit("NULL")).alias("customer_email"),
            coalesce(col("segment").cast("string"), lit("NULL")).alias("segment"),
            coalesce(col("region").cast("string"), lit("NULL")).alias("region"),
            coalesce(col("_raw_segment").cast("string"), lit("NULL")).alias("_raw_segment"),
            coalesce(col("_raw_region").cast("string"), lit("NULL")).alias("_raw_region"),
        ))
    )

    failures = flagged.filter(col("_has_any_failure") == True)

    # Explode each failure into its own row
    r1 = failures.filter(col("_fail_customer_id_null")).select(
        lit("clean_customers").alias("source_table"),
        col("customer_id").cast("string").alias("record_key"),
        lit("customer_id").alias("affected_field"),
        lit("missing_value").alias("issue_type"),
        col("record_data"),
        col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r2 = failures.filter(col("_fail_customer_name_null")).select(
        lit("clean_customers").alias("source_table"),
        col("customer_id").cast("string").alias("record_key"),
        lit("customer_name").alias("affected_field"),
        lit("missing_value").alias("issue_type"),
        col("record_data"),
        col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r3 = failures.filter(col("_fail_invalid_segment")).select(
        lit("clean_customers").alias("source_table"),
        col("customer_id").cast("string").alias("record_key"),
        lit("segment").alias("affected_field"),
        lit("invalid_value").alias("issue_type"),
        col("record_data"),
        col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r4 = failures.filter(col("_fail_invalid_region")).select(
        lit("clean_customers").alias("source_table"),
        col("customer_id").cast("string").alias("record_key"),
        lit("region").alias("affected_field"),
        lit("invalid_value").alias("issue_type"),
        col("record_data"),
        col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )

    return r1.unionByName(r2).unionByName(r3).unionByName(r4)

# COMMAND ----------

# --- Rejected: Orders ---
@dlt.append_flow(target="silver.rejected_records", name="rej_orders")
def rej_orders():
    raw = dlt.readStream("bronze.raw_orders")

    harmonized = raw.select(
        col("order_id"),
        col("customer_id"),
        col("vendor_id"),
        trim(col("ship_mode")).alias("_raw_ship_mode"),
        lower(trim(col("order_status"))).alias("_raw_order_status"),
        col("order_purchase_date").cast("string").alias("_raw_order_date"),
        col("_source_file"),
        col("_load_timestamp"),
    )

    mapped = harmonized.withColumn(
        "ship_mode",
        when(col("_raw_ship_mode").isin("1st Class", "1st"), "First Class")
        .when(col("_raw_ship_mode").isin("2nd Class", "2nd"), "Second Class")
        .when(col("_raw_ship_mode").isin("Std Class", "Std"), "Standard Class")
        .when(col("_raw_ship_mode").isin("First Class", "Second Class", "Standard Class", "Same Day"),
              col("_raw_ship_mode"))
        .otherwise(col("_raw_ship_mode"))
    ).withColumn(
        "order_status", col("_raw_order_status")
    ).withColumn(
        "order_purchase_date",
        coalesce(
            to_timestamp(col("_raw_order_date"), "MM/dd/yyyy HH:mm"),
            to_timestamp(col("_raw_order_date"), "yyyy-MM-dd HH:mm"),
            to_timestamp(col("_raw_order_date"), "MM/dd/yyyy"),
            to_timestamp(col("_raw_order_date"), "yyyy-MM-dd"),
            to_timestamp(col("_raw_order_date"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
    )

    valid_statuses = [
        "canceled", "created", "delivered", "invoiced",
        "processing", "shipped", "unavailable"
    ]

    flagged = mapped.withColumn(
        "_fail_order_id_null", col("order_id").isNull()
    ).withColumn(
        "_fail_customer_id_null", col("customer_id").isNull()
    ).withColumn(
        "_fail_invalid_ship_mode",
        col("ship_mode").isNull() | ~col("ship_mode").isin(
            "First Class", "Second Class", "Standard Class", "Same Day")
    ).withColumn(
        "_fail_invalid_order_status",
        col("order_status").isNull() | ~col("order_status").isin(*valid_statuses)
    ).withColumn(
        "_fail_unparseable_date",
        col("order_purchase_date").isNull()
    ).withColumn(
        "_has_any_failure",
        col("_fail_order_id_null") | col("_fail_customer_id_null") |
        col("_fail_invalid_ship_mode") | col("_fail_invalid_order_status") |
        col("_fail_unparseable_date")
    ).withColumn(
        "record_data", to_json(struct(
            coalesce(col("order_id").cast("string"), lit("NULL")).alias("order_id"),
            coalesce(col("customer_id").cast("string"), lit("NULL")).alias("customer_id"),
            coalesce(col("vendor_id").cast("string"), lit("NULL")).alias("vendor_id"),
            coalesce(col("ship_mode").cast("string"), lit("NULL")).alias("ship_mode"),
            coalesce(col("order_status").cast("string"), lit("NULL")).alias("order_status"),
            coalesce(col("_raw_ship_mode").cast("string"), lit("NULL")).alias("_raw_ship_mode"),
            coalesce(col("_raw_order_status").cast("string"), lit("NULL")).alias("_raw_order_status"),
            coalesce(col("_raw_order_date").cast("string"), lit("NULL")).alias("_raw_order_date"),
        ))
    )

    failures = flagged.filter(col("_has_any_failure") == True)

    r1 = failures.filter(col("_fail_order_id_null")).select(
        lit("clean_orders").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("order_id").alias("affected_field"),
        lit("missing_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r2 = failures.filter(col("_fail_customer_id_null")).select(
        lit("clean_orders").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("customer_id").alias("affected_field"),
        lit("missing_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r3 = failures.filter(col("_fail_invalid_ship_mode")).select(
        lit("clean_orders").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("ship_mode").alias("affected_field"),
        lit("invalid_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r4 = failures.filter(col("_fail_invalid_order_status")).select(
        lit("clean_orders").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("order_status").alias("affected_field"),
        lit("invalid_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r5 = failures.filter(col("_fail_unparseable_date")).select(
        lit("clean_orders").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("order_purchase_date").alias("affected_field"),
        lit("parse_failure").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )

    return r1.unionByName(r2).unionByName(r3).unionByName(r4).unionByName(r5)

# COMMAND ----------

# --- Rejected: Transactions ---
@dlt.append_flow(target="silver.rejected_records", name="rej_transactions")
def rej_transactions():
    raw = dlt.readStream("bronze.raw_transactions")

    harmonized = raw.select(
        coalesce(col("order_id"), col("Order_id"), col("Order_ID")).alias("order_id"),
        coalesce(col("product_id"), col("Product_id"), col("Product_ID")).alias("product_id"),
        regexp_replace(
            coalesce(col("sales"), col("Sales")).cast("string"),
            "[^0-9.\\-]", ""
        ).alias("_raw_sales"),
        coalesce(col("quantity"), col("Quantity")).cast("string").alias("_raw_quantity"),
        coalesce(col("discount"), col("Discount")).cast("string").alias("_raw_discount"),
        col("_source_file"),
        col("_load_timestamp"),
    )

    converted = harmonized.withColumn(
        "_discount_is_pct", col("_raw_discount").contains("%")
    ).withColumn(
        "_discount_cleaned",
        regexp_replace(col("_raw_discount"), "[%]", "").cast("double")
    ).withColumn(
        "discount",
        when(col("_discount_is_pct"), col("_discount_cleaned") / 100.0)
        .otherwise(col("_discount_cleaned"))
    ).withColumn(
        "sales", col("_raw_sales").cast("double")
    ).withColumn(
        "quantity", col("_raw_quantity").cast("int")
    )

    flagged = converted.withColumn(
        "_fail_order_id_null", col("order_id").isNull()
    ).withColumn(
        "_fail_product_id_null", col("product_id").isNull()
    ).withColumn(
        "_fail_negative_sales",
        col("sales").isNull() | (col("sales") < 0)
    ).withColumn(
        "_fail_non_positive_quantity",
        col("quantity").isNull() | (col("quantity") <= 0)
    ).withColumn(
        "_fail_discount_out_of_range",
        col("discount").isNull() | (col("discount") < 0) | (col("discount") > 1)
    ).withColumn(
        "_has_any_failure",
        col("_fail_order_id_null") | col("_fail_product_id_null") |
        col("_fail_negative_sales") | col("_fail_non_positive_quantity") |
        col("_fail_discount_out_of_range")
    ).withColumn(
        "record_data", to_json(struct(
            coalesce(col("order_id").cast("string"), lit("NULL")).alias("order_id"),
            coalesce(col("product_id").cast("string"), lit("NULL")).alias("product_id"),
            coalesce(col("sales").cast("string"), lit("NULL")).alias("sales"),
            coalesce(col("quantity").cast("string"), lit("NULL")).alias("quantity"),
            coalesce(col("discount").cast("string"), lit("NULL")).alias("discount"),
            coalesce(col("_raw_sales").cast("string"), lit("NULL")).alias("_raw_sales"),
            coalesce(col("_raw_quantity").cast("string"), lit("NULL")).alias("_raw_quantity"),
            coalesce(col("_raw_discount").cast("string"), lit("NULL")).alias("_raw_discount"),
        ))
    )

    failures = flagged.filter(col("_has_any_failure") == True)

    r1 = failures.filter(col("_fail_order_id_null")).select(
        lit("clean_transactions").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("order_id").alias("affected_field"),
        lit("missing_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r2 = failures.filter(col("_fail_product_id_null")).select(
        lit("clean_transactions").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("product_id").alias("affected_field"),
        lit("missing_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r3 = failures.filter(col("_fail_negative_sales")).select(
        lit("clean_transactions").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("sales").alias("affected_field"),
        lit("out_of_range").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r4 = failures.filter(col("_fail_non_positive_quantity")).select(
        lit("clean_transactions").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("quantity").alias("affected_field"),
        lit("out_of_range").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r5 = failures.filter(col("_fail_discount_out_of_range")).select(
        lit("clean_transactions").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("discount").alias("affected_field"),
        lit("out_of_range").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )

    return r1.unionByName(r2).unionByName(r3).unionByName(r4).unionByName(r5)

# COMMAND ----------

# --- Rejected: Returns ---
@dlt.append_flow(target="silver.rejected_records", name="rej_returns")
def rej_returns():
    raw = dlt.readStream("bronze.raw_returns")

    harmonized = raw.select(
        coalesce(col("order_id"), col("OrderId")).alias("order_id"),
        coalesce(col("return_reason"), col("reason")).alias("_raw_return_reason"),
        coalesce(col("return_status"), col("status")).alias("_raw_return_status"),
        coalesce(col("refund_amount"), col("amount")).cast("string").alias("_raw_refund_amount"),
        coalesce(col("return_date"), col("date_of_return")).cast("string").alias("_raw_return_date"),
        col("_source_file"),
        col("_load_timestamp"),
    )

    mapped = harmonized.withColumn(
        "return_status",
        when(upper(trim(col("_raw_return_status"))) == "APPRVD", "Approved")
        .when(upper(trim(col("_raw_return_status"))) == "PENDG", "Pending")
        .when(upper(trim(col("_raw_return_status"))) == "RJCTD", "Rejected")
        .when(trim(col("_raw_return_status")).isin("Approved", "Pending", "Rejected"),
              trim(col("_raw_return_status")))
        .otherwise(trim(col("_raw_return_status")))
    ).withColumn(
        "return_reason",
        when(trim(col("_raw_return_reason")) == "?", "Unknown")
        .otherwise(trim(col("_raw_return_reason")))
    ).withColumn(
        "refund_amount",
        when(col("_raw_refund_amount") == "?", None)
        .otherwise(regexp_replace(col("_raw_refund_amount"), "[^0-9.\\-]", "").cast("double"))
    ).withColumn(
        "return_date",
        coalesce(
            to_timestamp(col("_raw_return_date"), "yyyy-MM-dd"),
            to_timestamp(col("_raw_return_date"), "MM-dd-yyyy"),
            to_timestamp(col("_raw_return_date"), "MM/dd/yyyy"),
            to_timestamp(col("_raw_return_date"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
    )

    flagged = mapped.withColumn(
        "_fail_order_id_null", col("order_id").isNull()
    ).withColumn(
        "_fail_negative_refund",
        col("refund_amount").isNull() | (col("refund_amount") < 0)
    ).withColumn(
        "_fail_invalid_return_status",
        col("return_status").isNull() | ~col("return_status").isin("Approved", "Pending", "Rejected")
    ).withColumn(
        "_fail_invalid_return_reason",
        col("return_reason").isNull() | (col("return_reason") == "Unknown")
    ).withColumn(
        "_fail_unparseable_return_date",
        col("return_date").isNull()
    ).withColumn(
        "_has_any_failure",
        col("_fail_order_id_null") | col("_fail_negative_refund") |
        col("_fail_invalid_return_status") | col("_fail_invalid_return_reason") |
        col("_fail_unparseable_return_date")
    ).withColumn(
        "record_data", to_json(struct(
            coalesce(col("order_id").cast("string"), lit("NULL")).alias("order_id"),
            coalesce(col("return_reason").cast("string"), lit("NULL")).alias("return_reason"),
            coalesce(col("return_status").cast("string"), lit("NULL")).alias("return_status"),
            coalesce(col("refund_amount").cast("string"), lit("NULL")).alias("refund_amount"),
            coalesce(col("return_date").cast("string"), lit("NULL")).alias("return_date"),
            coalesce(col("_raw_return_reason").cast("string"), lit("NULL")).alias("_raw_return_reason"),
            coalesce(col("_raw_return_status").cast("string"), lit("NULL")).alias("_raw_return_status"),
            coalesce(col("_raw_refund_amount").cast("string"), lit("NULL")).alias("_raw_refund_amount"),
            coalesce(col("_raw_return_date").cast("string"), lit("NULL")).alias("_raw_return_date"),
        ))
    )

    failures = flagged.filter(col("_has_any_failure") == True)

    r1 = failures.filter(col("_fail_order_id_null")).select(
        lit("clean_returns").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("order_id").alias("affected_field"),
        lit("missing_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r2 = failures.filter(col("_fail_negative_refund")).select(
        lit("clean_returns").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("refund_amount").alias("affected_field"),
        lit("out_of_range").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r3 = failures.filter(col("_fail_invalid_return_status")).select(
        lit("clean_returns").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("return_status").alias("affected_field"),
        lit("invalid_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r4 = failures.filter(col("_fail_invalid_return_reason")).select(
        lit("clean_returns").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("return_reason").alias("affected_field"),
        lit("invalid_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r5 = failures.filter(col("_fail_unparseable_return_date")).select(
        lit("clean_returns").alias("source_table"),
        col("order_id").cast("string").alias("record_key"),
        lit("return_date").alias("affected_field"),
        lit("parse_failure").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )

    return r1.unionByName(r2).unionByName(r3).unionByName(r4).unionByName(r5)

# COMMAND ----------

# --- Rejected: Products ---
@dlt.append_flow(target="silver.rejected_records", name="rej_products")
def rej_products():
    raw = dlt.readStream("bronze.raw_products")

    harmonized = raw.select(
        col("product_id"),
        trim(col("product_name")).alias("product_name"),
        col("_source_file"),
        col("_load_timestamp"),
    )

    flagged = harmonized.withColumn(
        "_fail_product_id_null", col("product_id").isNull()
    ).withColumn(
        "_fail_product_name_null", col("product_name").isNull()
    ).withColumn(
        "_has_any_failure",
        col("_fail_product_id_null") | col("_fail_product_name_null")
    ).withColumn(
        "record_data", to_json(struct(
            coalesce(col("product_id").cast("string"), lit("NULL")).alias("product_id"),
            coalesce(col("product_name").cast("string"), lit("NULL")).alias("product_name"),
        ))
    )

    failures = flagged.filter(col("_has_any_failure") == True)

    r1 = failures.filter(col("_fail_product_id_null")).select(
        lit("clean_products").alias("source_table"),
        col("product_id").cast("string").alias("record_key"),
        lit("product_id").alias("affected_field"),
        lit("missing_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r2 = failures.filter(col("_fail_product_name_null")).select(
        lit("clean_products").alias("source_table"),
        col("product_id").cast("string").alias("record_key"),
        lit("product_name").alias("affected_field"),
        lit("missing_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )

    return r1.unionByName(r2)

# COMMAND ----------

# --- Rejected: Vendors ---
@dlt.append_flow(target="silver.rejected_records", name="rej_vendors")
def rej_vendors():
    raw = dlt.readStream("bronze.raw_vendors")

    harmonized = raw.select(
        trim(col("vendor_id")).alias("vendor_id"),
        trim(col("vendor_name")).alias("vendor_name"),
        col("_source_file"),
        col("_load_timestamp"),
    )

    flagged = harmonized.withColumn(
        "_fail_vendor_id_null", col("vendor_id").isNull()
    ).withColumn(
        "_fail_vendor_name_null", col("vendor_name").isNull()
    ).withColumn(
        "_has_any_failure",
        col("_fail_vendor_id_null") | col("_fail_vendor_name_null")
    ).withColumn(
        "record_data", to_json(struct(
            coalesce(col("vendor_id").cast("string"), lit("NULL")).alias("vendor_id"),
            coalesce(col("vendor_name").cast("string"), lit("NULL")).alias("vendor_name"),
        ))
    )

    failures = flagged.filter(col("_has_any_failure") == True)

    r1 = failures.filter(col("_fail_vendor_id_null")).select(
        lit("clean_vendors").alias("source_table"),
        col("vendor_id").cast("string").alias("record_key"),
        lit("vendor_id").alias("affected_field"),
        lit("missing_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )
    r2 = failures.filter(col("_fail_vendor_name_null")).select(
        lit("clean_vendors").alias("source_table"),
        col("vendor_id").cast("string").alias("record_key"),
        lit("vendor_name").alias("affected_field"),
        lit("missing_value").alias("issue_type"),
        col("record_data"), col("_source_file"),
        current_timestamp().alias("rejected_at"),
    )

    return r1.unionByName(r2)

# COMMAND ----------

# ============================================================================
# PIPELINE AUDIT LOG — Materialized view
# ============================================================================

# COMMAND ----------

@dlt.table(
    name="silver.pipeline_audit_log",
    comment="Row count reconciliation: bronze_count = silver_count + rejected_count",
    table_properties={"quality": "silver"},

)
def pipeline_audit_log():
    from pyspark.sql.functions import count, countDistinct

    entities = [
        ("customers", "raw_customers", "clean_customers"),
        ("orders", "raw_orders", "clean_orders"),
        ("transactions", "raw_transactions", "clean_transactions"),
        ("returns", "raw_returns", "clean_returns"),
        ("products", "raw_products", "clean_products"),
        ("vendors", "raw_vendors", "clean_vendors"),
    ]

    frames = []
    for entity_name, bronze_table, silver_table in entities:
        bronze_df = dlt.read(f"bronze.{bronze_table}")
        silver_df = dlt.read(f"silver.{silver_table}")
        rejected_df = dlt.read("silver.rejected_records").filter(
            col("source_table") == silver_table
        )

        # Pure DataFrame aggregations — no .count() actions or spark.createDataFrame()
        # DLT can schedule these AFTER upstream tables are populated
        bronze_cnt = bronze_df.select(count("*").alias("bronze_count"))
        silver_cnt = silver_df.select(count("*").alias("silver_count"))
        rejected_cnt = rejected_df.select(
            countDistinct(concat_ws("||", col("record_key"), col("_source_file"))).alias("rejected_count")
        )

        combined = (
            bronze_cnt
            .crossJoin(silver_cnt)
            .crossJoin(rejected_cnt)
            .withColumn("entity", lit(entity_name))
            .withColumn("reconciliation_diff",
                col("bronze_count") - col("silver_count") - col("rejected_count"))
        )
        frames.append(combined)

    result = frames[0]
    for f in frames[1:]:
        result = result.unionByName(f)

    return result.select(
        "entity", "bronze_count", "silver_count", "rejected_count",
        "reconciliation_diff",
    ).withColumn("audit_timestamp", current_timestamp())
