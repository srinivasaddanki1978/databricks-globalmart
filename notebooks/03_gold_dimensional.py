# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03 — Gold Dimensional Model (DLT)
# MAGIC
# MAGIC Builds the dimensional model from Silver tables:
# MAGIC - 4 dimension tables (customers, products, vendors, dates)
# MAGIC - 2 fact tables (sales, returns)
# MAGIC - 1 bridge table (return_products — proportional allocation)
# MAGIC - 3 materialized views (revenue by region, return rate by vendor, slow-moving products)
# MAGIC
# MAGIC All tables use `dlt.read()` (batch) because Gold requires joins and window functions.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, when, lit, row_number, count, sum as _sum, min as _min, max as _max,
    countDistinct, round as _round, coalesce, current_timestamp,
    year, month, quarter, dayofmonth, dayofweek, date_format,
    explode, sequence, to_date, expr, concat_ws,
    monotonically_increasing_id, md5, concat,
)
from pyspark.sql.window import Window

# COMMAND ----------

# ============================================================================
# DIMENSION TABLES
# ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_customers
# MAGIC ROW_NUMBER deduplication: 748 raw records → 374 unique customers.
# MAGIC Keeps the most recent record, preferring rows with email.

# COMMAND ----------

@dlt.table(
    name="gold.dim_customers",
    comment="Deduplicated customer dimension — one row per customer_id",
    table_properties={"quality": "gold"},

)
def dim_customers():
    customers = dlt.read("silver.clean_customers")

    window = Window.partitionBy("customer_id").orderBy(
        col("_load_timestamp").desc(),
        when(col("customer_email").isNotNull(), 0).otherwise(1),
    )

    deduped = (
        customers
        .withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    return deduped.select(
        monotonically_increasing_id().alias("customer_sk"),
        "customer_id", "customer_name", "customer_email",
        "segment", "city", "state", "region",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_products

# COMMAND ----------

@dlt.table(
    name="gold.dim_products",
    comment="Product dimension from cleaned product catalog",
    table_properties={"quality": "gold"},

)
def dim_products():
    return dlt.read("silver.clean_products").select(
        monotonically_increasing_id().alias("product_sk"),
        "product_id", "product_name", "brand", "category", "upc",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_vendors

# COMMAND ----------

@dlt.table(
    name="gold.dim_vendors",
    comment="Vendor dimension from cleaned vendor reference data",
    table_properties={"quality": "gold"},

)
def dim_vendors():
    return dlt.read("silver.clean_vendors").select(
        monotonically_increasing_id().alias("vendor_sk"),
        "vendor_id", "vendor_name",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_dates
# MAGIC Generated calendar dimension spanning the full date range of orders and returns.

# COMMAND ----------

@dlt.table(
    name="gold.dim_dates",
    comment="Calendar dimension — one row per day spanning the full data range",
    table_properties={"quality": "gold"},

)
def dim_dates():
    orders = dlt.read("silver.clean_orders")
    returns = dlt.read("silver.clean_returns")

    # Find min/max dates across orders and returns
    order_range = orders.select(
        _min(to_date("order_purchase_date")).alias("min_date"),
        _max(to_date("order_purchase_date")).alias("max_date"),
    )
    return_range = returns.select(
        _min(to_date("return_date")).alias("min_date"),
        _max(to_date("return_date")).alias("max_date"),
    )

    combined = order_range.unionByName(return_range)
    date_bounds = combined.select(
        _min("min_date").alias("min_date"),
        _max("max_date").alias("max_date"),
    )

    # Pure DataFrame approach — avoids .first()/.collect() which fail during DLT analysis
    # Generate full calendar years: Jan 1 of earliest year to Dec 31 of latest year
    calendar = date_bounds.selectExpr(
        "explode(sequence(make_date(year(min_date), 1, 1), "
        "make_date(year(max_date), 12, 31), interval 1 day)) as date_key"
    )

    return calendar.select(
        monotonically_increasing_id().alias("date_sk"),
        col("date_key"),
        year("date_key").alias("year"),
        quarter("date_key").alias("quarter"),
        month("date_key").alias("month"),
        date_format("date_key", "MMMM").alias("month_name"),
        dayofmonth("date_key").alias("day"),
        dayofweek("date_key").alias("day_of_week"),
        when(dayofweek("date_key").isin(1, 7), True).otherwise(False).alias("is_weekend"),
    )

# COMMAND ----------

# ============================================================================
# FACT TABLES
# ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_sales
# MAGIC Grain: one row per order line item.
# MAGIC Source: clean_orders INNER JOIN clean_transactions ON order_id.

# COMMAND ----------

@dlt.table(
    name="gold.fact_sales",
    comment="Sales fact — one row per order line item (orders × transactions)",
    table_properties={"quality": "gold"},

)
def fact_sales():
    orders = dlt.read("silver.clean_orders")
    transactions = dlt.read("silver.clean_transactions")
    dim_cust = dlt.read("gold.dim_customers").select("customer_id", "customer_sk")
    dim_prod = dlt.read("gold.dim_products").select("product_id", "product_sk")
    dim_vend = dlt.read("gold.dim_vendors").select("vendor_id", "vendor_sk")
    dim_dt = dlt.read("gold.dim_dates").select("date_key", "date_sk")

    # Base fact from Silver
    base = orders.join(transactions, on="order_id", how="inner").select(
        col("order_id"),
        orders["customer_id"],
        col("product_id"),
        col("vendor_id"),
        to_date(col("order_purchase_date")).alias("order_date_key"),
        col("sales"),
        col("quantity"),
        col("discount"),
        col("profit"),
        col("ship_mode"),
        col("order_status"),
        col("order_purchase_date"),
        col("payment_type"),
    )

    # Look up surrogate keys from dimension tables
    with_sk = (
        base
        .join(dim_cust, on="customer_id", how="left")
        .join(dim_prod, on="product_id", how="left")
        .join(dim_vend, on="vendor_id", how="left")
        .join(dim_dt, col("order_date_key") == dim_dt["date_key"], how="left")
    )

    return with_sk.select(
        monotonically_increasing_id().alias("sales_sk"),
        col("order_id"),
        col("customer_sk"),
        col("customer_id"),
        col("product_sk"),
        col("product_id"),
        col("vendor_sk"),
        col("vendor_id"),
        col("date_sk").alias("order_date_sk"),
        col("order_date_key"),
        col("sales"),
        col("quantity"),
        col("discount"),
        col("profit"),
        col("ship_mode"),
        col("order_status"),
        col("order_purchase_date"),
        col("payment_type"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_returns
# MAGIC Grain: one row per return event.
# MAGIC Source: clean_returns LEFT JOIN clean_orders ON order_id.
# MAGIC LEFT JOIN preserves returns without matching orders.

# COMMAND ----------

@dlt.table(
    name="gold.fact_returns",
    comment="Returns fact — one row per return event (LEFT JOIN preserves unmatched returns)",
    table_properties={"quality": "gold"},

)
def fact_returns():
    returns = dlt.read("silver.clean_returns")
    orders = dlt.read("silver.clean_orders")
    dim_cust = dlt.read("gold.dim_customers").select("customer_id", "customer_sk")
    dim_vend = dlt.read("gold.dim_vendors").select("vendor_id", "vendor_sk")
    dim_dt = dlt.read("gold.dim_dates").select("date_key", "date_sk")

    # Base fact from Silver (LEFT JOIN preserves returns without matching orders)
    base = returns.join(orders, on="order_id", how="left").select(
        returns["order_id"],
        orders["customer_id"],
        col("vendor_id"),
        to_date(col("return_date")).alias("return_date_key"),
        col("refund_amount"),
        col("return_reason"),
        col("return_status"),
        col("return_date"),
    )

    # Look up surrogate keys from dimension tables
    with_sk = (
        base
        .join(dim_cust, on="customer_id", how="left")
        .join(dim_vend, on="vendor_id", how="left")
        .join(dim_dt, col("return_date_key") == dim_dt["date_key"], how="left")
    )

    return with_sk.select(
        monotonically_increasing_id().alias("return_sk"),
        col("order_id"),
        col("customer_sk"),
        col("customer_id"),
        col("vendor_sk"),
        col("vendor_id"),
        col("date_sk").alias("return_date_sk"),
        col("return_date_key"),
        col("refund_amount"),
        col("return_reason"),
        col("return_status"),
        col("return_date"),
    )

# COMMAND ----------

# ============================================================================
# BRIDGE TABLE
# ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## bridge_return_products
# MAGIC Connects returns (order-level) to products (line-item level) using proportional allocation.
# MAGIC Reads from Gold facts for correct DLT lineage.

# COMMAND ----------

@dlt.table(
    name="gold.bridge_return_products",
    comment="Bridge: allocates return refunds proportionally across order line items",
    table_properties={"quality": "gold"},

)
def bridge_return_products():
    fact_ret = dlt.read("gold.fact_returns")
    fact_sal = dlt.read("gold.fact_sales")

    # Total sales per order for allocation weight
    order_totals = fact_sal.groupBy("order_id").agg(
        _sum("sales").alias("total_order_sales")
    )

    # Join returns with sales line items
    joined = (
        fact_ret
        .join(fact_sal, on="order_id", how="inner")
        .join(order_totals, on="order_id", how="inner")
    )

    return joined.select(
        fact_ret["order_id"],
        fact_sal["product_sk"],
        col("product_id"),
        fact_ret["return_date_sk"],
        col("return_date_key"),
        col("return_reason"),
        col("return_status"),
        col("refund_amount"),
        col("sales").alias("line_sales"),
        col("total_order_sales"),
        (_round(col("sales") / col("total_order_sales"), 4)).alias("allocation_weight"),
        (_round(col("refund_amount") * col("sales") / col("total_order_sales"), 2)).alias("allocated_refund"),
    )

# COMMAND ----------

# ============================================================================
# MATERIALIZED VIEWS
# ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_revenue_by_region
# MAGIC Pre-computed revenue metrics by year, month, and region.

# COMMAND ----------

@dlt.table(
    name="gold.mv_revenue_by_region",
    comment="Monthly revenue aggregation by region — addresses Revenue Audit failure",
    table_properties={"quality": "gold"},

)
def mv_revenue_by_region():
    sales = dlt.read("gold.fact_sales")
    customers = dlt.read("gold.dim_customers")
    dates = dlt.read("gold.dim_dates")

    joined = (
        sales
        .join(customers, on="customer_id", how="inner")
        .join(dates, sales["order_date_key"] == dates["date_key"], how="inner")
    )

    return joined.groupBy(
        dates["year"], dates["month"], dates["month_name"], customers["region"]
    ).agg(
        countDistinct("order_id").alias("order_count"),
        _round(_sum("sales"), 2).alias("total_sales"),
        _round(_sum("profit"), 2).alias("total_profit"),
        _sum("quantity").alias("total_quantity"),
        countDistinct("customer_id").alias("unique_customers"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_return_rate_by_vendor
# MAGIC Vendor-level return rates — addresses Returns Fraud failure.

# COMMAND ----------

@dlt.table(
    name="gold.mv_return_rate_by_vendor",
    comment="Vendor-level return rate metrics — addresses Returns Fraud failure",
    table_properties={"quality": "gold"},

)
def mv_return_rate_by_vendor():
    sales = dlt.read("gold.fact_sales")
    returns = dlt.read("gold.fact_returns")
    vendors = dlt.read("gold.dim_vendors")

    # Sales aggregation per vendor
    vendor_sales = sales.groupBy("vendor_id").agg(
        countDistinct("order_id").alias("total_orders"),
        _round(_sum("sales"), 2).alias("total_sales"),
    )

    # Returns aggregation per vendor — vendor_id is directly on fact_returns
    vendor_returns = returns.filter(col("vendor_id").isNotNull()).groupBy("vendor_id").agg(
        countDistinct("order_id").alias("return_order_count"),
        _round(_sum("refund_amount"), 2).alias("total_refunded"),
    )

    # Combine
    combined = (
        vendor_sales
        .join(vendors, on="vendor_id", how="inner")
        .join(vendor_returns, on="vendor_id", how="left")
    )

    return combined.select(
        col("vendor_id"),
        col("vendor_name"),
        col("total_orders"),
        col("total_sales"),
        coalesce(col("return_order_count"), lit(0)).alias("return_order_count"),
        coalesce(col("total_refunded"), lit(0.0)).alias("total_refunded"),
        _round(
            coalesce(col("return_order_count"), lit(0)) / col("total_orders") * 100, 2
        ).alias("return_rate_pct"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_slow_moving_products
# MAGIC Product × region performance — addresses Inventory Blindspot failure.

# COMMAND ----------

@dlt.table(
    name="gold.mv_slow_moving_products",
    comment="Product-region performance with slow-moving flag — addresses Inventory Blindspot",
    table_properties={"quality": "gold"},

)
def mv_slow_moving_products():
    sales = dlt.read("gold.fact_sales")
    products = dlt.read("gold.dim_products")
    customers = dlt.read("gold.dim_customers")

    joined = (
        sales
        .join(products, on="product_id", how="inner")
        .join(customers, on="customer_id", how="inner")
    )

    agg = joined.groupBy(
        "product_id", "product_name", "brand", customers["region"]
    ).agg(
        _round(_sum("sales"), 2).alias("total_sales"),
        _sum("quantity").alias("total_quantity"),
        countDistinct("order_id").alias("order_count"),
        _round(_sum("profit"), 2).alias("total_profit"),
    )

    # Flag slow-moving: bottom 25% by total_quantity within each region
    w = Window.partitionBy("region")
    with_pct = agg.withColumn(
        "_p25_qty", expr("percentile_approx(total_quantity, 0.25)").over(w)
    )

    return with_pct.withColumn(
        "is_slow_moving",
        when(col("total_quantity") <= col("_p25_qty"), True).otherwise(False)
    ).drop("_p25_qty")
