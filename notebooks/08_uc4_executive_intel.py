# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 08 — UC4: Executive Business Intelligence
# MAGIC
# MAGIC Reads pre-computed metrics tables, computes summary KPIs, generates
# MAGIC executive narratives via LLM, and demonstrates `ai_query()` for SQL-native
# MAGIC LLM calls.
# MAGIC
# MAGIC **Model:** `databricks-gpt-oss-20b`

# COMMAND ----------

# MAGIC %pip install openai --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from pyspark.sql.functions import (
    col, sum as _sum, avg as _avg, count, countDistinct,
    round as _round, current_timestamp, lit, max as _max, min as _min,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM Connection

# COMMAND ----------

from openai import OpenAI

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
_ws_raw = spark.conf.get("spark.databricks.workspaceUrl")
_ws_url = _ws_raw.replace("https://", "").replace("http://", "").strip("/")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{_ws_url}/serving-endpoints"
)
MODEL_NAME = "databricks-gpt-oss-20b"

# COMMAND ----------

def parse_llm_response(response_text):
    """Extract the 'text' block from the model's structured response."""
    try:
        response_text = list(response_text)
        for block in response_text:
            dict_block = dict(block)
            if dict_block.get("type") == "text":
                return dict_block.get("text", "")
        return str(response_text)
    except (json.JSONDecodeError, TypeError):
        return str(response_text)

def call_llm(prompt: str) -> str:
    """Call the LLM and parse the response."""
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=1024,
        )
        raw = response.choices[0].message.content
        return parse_llm_response(raw)
    except Exception as e:
        return f"LLM call failed: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Domain 1: Revenue Performance

# COMMAND ----------

revenue_df = spark.table("globalmart.metrics.mv_monthly_revenue_by_region")
segment_df = spark.table("globalmart.metrics.mv_segment_profitability")

# Compute summary KPIs
revenue_kpis = revenue_df.agg(
    _round(_sum("total_revenue"), 2).alias("grand_total_revenue"),
    _round(_sum("total_profit"), 2).alias("grand_total_profit"),
    _sum("total_quantity").alias("grand_total_units"),
    _sum("order_count").alias("grand_total_orders"),
).first()

region_breakdown = revenue_df.groupBy("region").agg(
    _round(_sum("total_revenue"), 2).alias("total_revenue"),
    _round(_sum("total_profit"), 2).alias("total_profit"),
).collect()

segment_breakdown = segment_df.groupBy("segment").agg(
    _round(_sum("total_revenue"), 2).alias("total_revenue"),
    _round(_avg("profit_margin_pct"), 2).alias("avg_margin_pct"),
).collect()

revenue_kpi_data = {
    "grand_total_revenue": float(revenue_kpis["grand_total_revenue"]),
    "grand_total_profit": float(revenue_kpis["grand_total_profit"]),
    "grand_total_units": int(revenue_kpis["grand_total_units"]),
    "grand_total_orders": int(revenue_kpis["grand_total_orders"]),
    "by_region": [{
        "region": r["region"],
        "revenue": float(r["total_revenue"]),
        "profit": float(r["total_profit"]),
    } for r in region_breakdown],
    "by_segment": [{
        "segment": s["segment"],
        "revenue": float(s["total_revenue"]),
        "margin": float(s["avg_margin_pct"]),
    } for s in segment_breakdown],
}

kpi_json = json.dumps(revenue_kpi_data, indent=2)

revenue_prompt = f"""You are a senior business analyst writing an executive summary for GlobalMart's leadership team.

Here are the revenue performance KPIs:
{kpi_json}

Write a 4-6 sentence executive summary that:
1. States the total revenue and profit figures
2. Identifies the strongest and weakest performing regions
3. Compares segment profitability (Consumer vs Corporate vs Home Office)
4. Highlights one actionable insight for the leadership team
5. Uses dollar amounts and percentages from the data

Be concise and executive-friendly. No bullet points — flowing prose only."""

revenue_summary = call_llm(revenue_prompt)
print("Revenue Performance Summary:")
print(revenue_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Domain 2: Vendor Return Rates

# COMMAND ----------

vendor_perf = spark.table("globalmart.metrics.mv_vendor_product_performance")

vendor_kpis = vendor_perf.groupBy("vendor_id", "vendor_name").agg(
    _sum("total_sales").alias("total_sales"),
    _sum("return_count").alias("total_returns"),
    _sum("total_allocated_refund").alias("total_refunded"),
).collect()

vendor_kpi_data = {
    "vendors": [{
        "vendor": v["vendor_name"],
        "total_sales": float(v["total_sales"] or 0),
        "total_returns": int(v["total_returns"] or 0),
        "total_refunded": float(v["total_refunded"] or 0),
        "return_rate": round(
            (v["total_returns"] or 0) / max((v["total_sales"] or 1), 1) * 100, 2
        ),
    } for v in vendor_kpis],
}

vendor_json = json.dumps(vendor_kpi_data, indent=2)

vendor_prompt = f"""You are a senior business analyst writing an executive summary about vendor performance for GlobalMart's procurement team.

Here are the vendor return rate KPIs:
{vendor_json}

Write a 4-6 sentence executive summary that:
1. Identifies the vendor with the highest return-related costs
2. Compares return rates across vendors
3. Quantifies the total financial impact of vendor returns
4. Recommends which vendor relationships to renegotiate
5. Uses actual dollar amounts from the data

Be concise and executive-friendly. No bullet points — flowing prose only."""

vendor_summary = call_llm(vendor_prompt)
print("Vendor Return Rate Summary:")
print(vendor_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Domain 3: Slow-Moving Inventory

# COMMAND ----------

product_impact = spark.table("globalmart.metrics.mv_product_return_impact")

impact_kpis = product_impact.agg(
    count("*").alias("total_product_regions"),
    _round(_sum("total_return_cost"), 2).alias("total_return_cost"),
    _round(_avg("avg_return_cost"), 2).alias("overall_avg_return_cost"),
).first()

top_return_products = product_impact.orderBy(col("total_return_cost").desc()).limit(5).collect()

inventory_kpi_data = {
    "total_product_region_combos": int(impact_kpis["total_product_regions"]),
    "total_return_cost": float(impact_kpis["total_return_cost"]),
    "avg_return_cost": float(impact_kpis["overall_avg_return_cost"]),
    "top_5_return_cost_products": [{
        "product": p["product_name"],
        "region": p["region"],
        "return_cost": float(p["total_return_cost"]),
        "return_lines": int(p["return_line_count"]),
    } for p in top_return_products],
}

inventory_json = json.dumps(inventory_kpi_data, indent=2)

inventory_prompt = f"""You are a senior business analyst writing an executive summary about slow-moving inventory and product return costs for GlobalMart's operations team.

Here are the product return impact KPIs:
{inventory_json}

Write a 4-6 sentence executive summary that:
1. States the total return cost across all products
2. Identifies the products with the highest return costs by region
3. Highlights the connection between slow-moving inventory and return rates
4. Recommends inventory reallocation actions
5. Uses actual dollar amounts from the data

Be concise and executive-friendly. No bullet points — flowing prose only."""

inventory_summary = call_llm(inventory_prompt)
print("Slow-Moving Inventory Summary:")
print(inventory_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Executive Summaries to Gold

# COMMAND ----------

results = [
    {
        "insight_type": "revenue_performance",
        "executive_summary": revenue_summary,
        "kpi_data": json.dumps(revenue_kpi_data),
    },
    {
        "insight_type": "vendor_return_rate",
        "executive_summary": vendor_summary,
        "kpi_data": json.dumps(vendor_kpi_data),
    },
    {
        "insight_type": "slow_moving_inventory",
        "executive_summary": inventory_summary,
        "kpi_data": json.dumps(inventory_kpi_data),
    },
]

schema = StructType([
    StructField("insight_type", StringType()),
    StructField("executive_summary", StringType()),
    StructField("kpi_data", StringType()),
])

result_df = spark.createDataFrame(results, schema=schema)
result_df = result_df.withColumn("generated_at", current_timestamp())

result_df.write.mode("overwrite").saveAsTable("globalmart.gold.ai_business_insights")

print("Wrote 3 executive summaries to globalmart.gold.ai_business_insights")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demonstrate ai_query() — SQL-Native LLM Calls
# MAGIC
# MAGIC The rubric requires `ai_query()` with `get_json_object()` for clean text extraction.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 1: Revenue Assessment per Region

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     region,
# MAGIC     total_revenue,
# MAGIC     total_profit,
# MAGIC     get_json_object(
# MAGIC         get_json_object(
# MAGIC             ai_query(
# MAGIC                 'databricks-gpt-oss-20b',
# MAGIC                 CONCAT(
# MAGIC                     'In one sentence, assess the revenue health of the ',
# MAGIC                     region,
# MAGIC                     ' region with total revenue of $',
# MAGIC                     CAST(total_revenue AS STRING),
# MAGIC                     ' and profit of $',
# MAGIC                     CAST(total_profit AS STRING),
# MAGIC                     '. Is this region performing well or underperforming?'
# MAGIC                 )
# MAGIC             ),
# MAGIC             '$[1]'
# MAGIC         ),
# MAGIC         '$.text'
# MAGIC     ) AS ai_assessment
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         region,
# MAGIC         ROUND(SUM(total_revenue), 2) AS total_revenue,
# MAGIC         ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC     FROM globalmart.metrics.mv_monthly_revenue_by_region
# MAGIC     GROUP BY region
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo 2: Vendor Risk Assessment

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     vendor_name,
# MAGIC     total_sales,
# MAGIC     return_rate_pct,
# MAGIC     get_json_object(
# MAGIC         get_json_object(
# MAGIC             ai_query(
# MAGIC                 'databricks-gpt-oss-20b',
# MAGIC                 CONCAT(
# MAGIC                     'In one sentence, should GlobalMart continue, renegotiate, or drop vendor "',
# MAGIC                     vendor_name,
# MAGIC                     '" who has $',
# MAGIC                     CAST(total_sales AS STRING),
# MAGIC                     ' in sales and a ',
# MAGIC                     CAST(return_rate_pct AS STRING),
# MAGIC                     '% return rate? Give a clear recommendation.'
# MAGIC                 )
# MAGIC             ),
# MAGIC             '$[1]'
# MAGIC         ),
# MAGIC         '$.text'
# MAGIC     ) AS ai_vendor_recommendation
# MAGIC FROM globalmart.gold.mv_return_rate_by_vendor

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Executive Insights

# COMMAND ----------

display(spark.table("globalmart.gold.ai_business_insights"))
