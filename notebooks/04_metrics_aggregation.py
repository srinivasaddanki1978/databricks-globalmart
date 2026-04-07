# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04 — Metrics Aggregation (DLT)
# MAGIC
# MAGIC Pre-computed aggregation tables that serve the 3 business failures:
# MAGIC 1. **Revenue Audit** — monthly revenue by region + segment profitability
# MAGIC 2. **Returns Fraud** — customer return history + return reason analysis
# MAGIC 3. **Inventory Blindspot** — vendor-product performance + product return impact
# MAGIC
# MAGIC All 6 tables are materialized views reading from Gold, recomputed every pipeline run.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, count, countDistinct, sum as _sum, avg as _avg,
    min as _min, max as _max, round as _round, coalesce, to_date,
    concat_ws, collect_set, when,
)

# COMMAND ----------

# ============================================================================
# REVENUE AUDIT — Business Failure #1
# ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_monthly_revenue_by_region
# MAGIC Grain: year × month × region

# COMMAND ----------

@dlt.table(
    name="metrics.mv_monthly_revenue_by_region",
    comment="Monthly revenue by region — Revenue Audit business failure",
    table_properties={"quality": "metrics"},

)
def mv_monthly_revenue_by_region():
    sales = dlt.read("gold.fact_sales")
    # Slim reads: exclude natural keys already present on fact side to avoid ambiguity
    customers = dlt.read("gold.dim_customers").select("customer_sk", "region")
    dates = dlt.read("gold.dim_dates").select("date_sk", "year", "month", "month_name")

    joined = (
        sales
        .join(customers, on="customer_sk", how="inner")
        .join(dates, sales["order_date_sk"] == dates["date_sk"], how="inner")
    )

    return joined.groupBy(
        "year", "month", "month_name", "region"
    ).agg(
        countDistinct("order_id").alias("order_count"),
        _round(_sum("sales"), 2).alias("total_revenue"),
        _round(_sum("profit"), 2).alias("total_profit"),
        _sum("quantity").alias("total_quantity"),
        countDistinct("customer_id").alias("unique_customers"),
        _round(_avg("sales"), 2).alias("avg_order_value"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_segment_profitability
# MAGIC Grain: segment × region

# COMMAND ----------

@dlt.table(
    name="metrics.mv_segment_profitability",
    comment="Segment profitability by region — Revenue Audit business failure",
    table_properties={"quality": "metrics"},

)
def mv_segment_profitability():
    sales = dlt.read("gold.fact_sales")
    customers = dlt.read("gold.dim_customers").select("customer_sk", "segment", "region")

    joined = sales.join(customers, on="customer_sk", how="inner")

    return joined.groupBy("segment", "region").agg(
        countDistinct("order_id").alias("order_count"),
        countDistinct("customer_id").alias("customer_count"),
        _round(_sum("sales"), 2).alias("total_revenue"),
        _round(_sum("profit"), 2).alias("total_profit"),
        _round(_sum("profit") / _sum("sales") * 100, 2).alias("profit_margin_pct"),
        _round(_avg("discount"), 4).alias("avg_discount"),
    )

# COMMAND ----------

# ============================================================================
# RETURNS FRAUD — Business Failure #2
# ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_customer_return_history
# MAGIC Grain: one row per customer

# COMMAND ----------

@dlt.table(
    name="metrics.mv_customer_return_history",
    comment="Customer return profiles — Returns Fraud business failure",
    table_properties={"quality": "metrics"},

)
def mv_customer_return_history():
    returns = dlt.read("gold.fact_returns")
    customers = dlt.read("gold.dim_customers").select("customer_sk", "customer_name", "segment", "region")

    joined = returns.filter(col("customer_sk").isNotNull()).join(
        customers, on="customer_sk", how="inner"
    )

    return joined.groupBy(
        "customer_id", "customer_name", "segment", "region"
    ).agg(
        count("*").alias("total_returns"),
        _round(_sum("refund_amount"), 2).alias("total_refund_value"),
        _round(_avg("refund_amount"), 2).alias("avg_refund_per_return"),
        countDistinct("return_reason").alias("distinct_reason_count"),
        concat_ws(", ", collect_set("return_reason")).alias("return_reasons"),
        _sum(when(col("return_status") == "Approved", 1).otherwise(0)).alias("approved_count"),
        _round(
            _sum(when(col("return_status") == "Approved", 1).otherwise(0)) / count("*") * 100, 2
        ).alias("approval_rate_pct"),
        _min("return_date").alias("first_return_date"),
        _max("return_date").alias("last_return_date"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_return_reason_analysis
# MAGIC Grain: return_reason × region

# COMMAND ----------

@dlt.table(
    name="metrics.mv_return_reason_analysis",
    comment="Return reasons by region — Returns Fraud business failure",
    table_properties={"quality": "metrics"},

)
def mv_return_reason_analysis():
    returns = dlt.read("gold.fact_returns")
    customers = dlt.read("gold.dim_customers").select("customer_sk", "region")

    joined = returns.filter(col("customer_sk").isNotNull()).join(
        customers, on="customer_sk", how="inner"
    )

    return joined.groupBy("return_reason", "region").agg(
        count("*").alias("return_count"),
        _round(_sum("refund_amount"), 2).alias("total_refunded"),
        _round(_avg("refund_amount"), 2).alias("avg_refund"),
        countDistinct("customer_id").alias("unique_customers"),
        _sum(when(col("return_status") == "Approved", 1).otherwise(0)).alias("approved_count"),
        _sum(when(col("return_status") == "Rejected", 1).otherwise(0)).alias("rejected_count"),
    )

# COMMAND ----------

# ============================================================================
# INVENTORY BLINDSPOT — Business Failure #3
# ============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_vendor_product_performance
# MAGIC Grain: vendor × product × region (5-table join)

# COMMAND ----------

@dlt.table(
    name="metrics.mv_vendor_product_performance",
    comment="Vendor-product performance by region — Inventory Blindspot business failure",
    table_properties={"quality": "metrics"},

)
def mv_vendor_product_performance():
    sales = dlt.read("gold.fact_sales")
    returns = dlt.read("gold.fact_returns")
    # Slim reads: each dim supplies only _sk + descriptive cols, never the _id already on sales
    customers = dlt.read("gold.dim_customers").select("customer_sk", "region")
    products = dlt.read("gold.dim_products").select("product_sk", "product_name", "brand")
    vendors = dlt.read("gold.dim_vendors").select("vendor_sk", "vendor_name")

    # Sales metrics per vendor × product × region
    sales_agg = (
        sales
        .join(customers, on="customer_sk", how="inner")
        .join(products, on="product_sk", how="inner")
        .join(vendors, on="vendor_sk", how="inner")
        .groupBy(
            col("vendor_id"), col("vendor_name"),
            col("product_id"), col("product_name"), col("brand"),
            col("region"),
        )
        .agg(
            countDistinct("order_id").alias("order_count"),
            _round(_sum("sales"), 2).alias("total_sales"),
            _sum("quantity").alias("total_quantity"),
            _round(_sum("profit"), 2).alias("total_profit"),
        )
    )

    # Return metrics per vendor × product (via bridge)
    bridge = dlt.read("gold.bridge_return_products")
    return_agg = (
        bridge
        .join(sales.select("order_id", "vendor_id").distinct(), on="order_id", how="inner")
        .groupBy("vendor_id", "product_id")
        .agg(
            count("*").alias("return_count"),
            _round(_sum("allocated_refund"), 2).alias("total_allocated_refund"),
        )
    )

    combined = sales_agg.join(
        return_agg,
        on=["vendor_id", "product_id"],
        how="left"
    )

    return combined.select(
        "vendor_id", "vendor_name", "product_id", "product_name", "brand",
        "region", "order_count", "total_sales", "total_quantity", "total_profit",
        coalesce(col("return_count"), lit(0)).alias("return_count"),
        coalesce(col("total_allocated_refund"), lit(0.0)).alias("total_allocated_refund"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_product_return_impact
# MAGIC Grain: product × region

# COMMAND ----------

@dlt.table(
    name="metrics.mv_product_return_impact",
    comment="Product return impact by region — Inventory Blindspot business failure",
    table_properties={"quality": "metrics"},

)
def mv_product_return_impact():
    bridge = dlt.read("gold.bridge_return_products")
    sales = dlt.read("gold.fact_sales")
    products = dlt.read("gold.dim_products").select("product_sk", "product_name", "brand")
    customers = dlt.read("gold.dim_customers").select("customer_sk", "region")

    # Get region from sales → customers via customer_sk
    sales_with_region = sales.join(customers, on="customer_sk", how="inner")

    # Bridge + sales: join on order_id + product_sk (bridge carries product_sk)
    joined = (
        bridge
        .join(
            sales_with_region.select("order_id", "product_sk", "region").distinct(),
            on=["order_id", "product_sk"],
            how="inner"
        )
        .join(products, on="product_sk", how="inner")
    )

    return joined.groupBy(
        "product_id", "product_name", "brand", "region"
    ).agg(
        count("*").alias("return_line_count"),
        _round(_sum("allocated_refund"), 2).alias("total_return_cost"),
        _round(_avg("allocated_refund"), 2).alias("avg_return_cost"),
        _round(_sum("allocation_weight"), 4).alias("total_allocation_weight"),
    )
