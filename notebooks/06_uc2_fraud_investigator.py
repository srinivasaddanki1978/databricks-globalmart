# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 06 — UC2: Returns Fraud Investigator
# MAGIC
# MAGIC Builds customer return profiles, applies 5 weighted anomaly rules (rule-based,
# MAGIC NOT LLM), calculates a composite score (0-100), flags customers scoring >= 40,
# MAGIC and generates LLM investigation briefs for flagged customers.
# MAGIC
# MAGIC **Model:** `databricks-gpt-oss-20b`

# COMMAND ----------

# MAGIC %pip install openai --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, avg as _avg, round as _round,
    collect_set, concat_ws, when, lit, current_timestamp, udf,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
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
            max_tokens=512,
        )
        raw = response.choices[0].message.content
        return parse_llm_response(raw)
    except Exception as e:
        return f"LLM call failed: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Customer Return Profiles

# COMMAND ----------

returns = spark.table("globalmart.gold.fact_returns")
# Slim read: exclude customer_id (already on fact_returns) to avoid ambiguity after _sk join
customers = spark.table("globalmart.gold.dim_customers").select("customer_sk", "customer_name", "segment", "region")

# Only consider returns that have a customer
return_profiles = (
    returns
    .filter(col("customer_sk").isNotNull())
    .join(customers, on="customer_sk", how="inner")
    .groupBy("customer_id", "customer_name", "segment", "region")
    .agg(
        count("*").alias("total_returns"),
        _round(_sum("refund_amount"), 2).alias("total_refund_value"),
        _round(_avg("refund_amount"), 2).alias("avg_refund_per_return"),
        _sum(when(col("return_status") == "Approved", 1).otherwise(0)).alias("approved_count"),
        countDistinct("return_reason").alias("distinct_reason_count"),
        concat_ws(", ", collect_set("return_reason")).alias("return_reasons"),
    )
    .withColumn(
        "approval_rate",
        _round(col("approved_count") / col("total_returns") * 100, 2)
    )
)

# Note: .cache() not supported on serverless compute, use temp view instead
return_profiles.createOrReplaceTempView("_return_profiles")
return_profiles = spark.table("_return_profiles")
print(f"Total customer return profiles: {return_profiles.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Calculate Data-Driven Thresholds (P75)

# COMMAND ----------

thresholds = return_profiles.selectExpr(
    "percentile_approx(total_returns, 0.75) as p75_returns",
    "percentile_approx(total_refund_value, 0.75) as p75_refund",
    "percentile_approx(avg_refund_per_return, 0.75) as p75_avg_refund",
).first()

P75_RETURNS = max(thresholds["p75_returns"], 3)  # at least 3
P75_REFUND = thresholds["p75_refund"]
P75_AVG_REFUND = thresholds["p75_avg_refund"]

print(f"Thresholds — returns: {P75_RETURNS}, total refund: {P75_REFUND}, avg refund: {P75_AVG_REFUND}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply 5 Weighted Anomaly Rules

# COMMAND ----------

# Rule weights
W_HIGH_RETURN_COUNT = 20
W_HIGH_TOTAL_REFUND = 25
W_HIGH_AVG_REFUND = 20
W_HIGH_APPROVAL_RATE = 15
W_MULTIPLE_REASONS = 20

scored = return_profiles.withColumn(
    "score_high_return_count",
    when(col("total_returns") > P75_RETURNS, W_HIGH_RETURN_COUNT).otherwise(0)
).withColumn(
    "score_high_total_refund",
    when(col("total_refund_value") > P75_REFUND, W_HIGH_TOTAL_REFUND).otherwise(0)
).withColumn(
    "score_high_avg_refund",
    when(col("avg_refund_per_return") > P75_AVG_REFUND, W_HIGH_AVG_REFUND).otherwise(0)
).withColumn(
    "score_high_approval_rate",
    when((col("approval_rate") > 90) & (col("total_returns") >= 2), W_HIGH_APPROVAL_RATE).otherwise(0)
).withColumn(
    "score_multiple_reasons",
    when(col("distinct_reason_count") >= 3, W_MULTIPLE_REASONS).otherwise(0)
)

# Composite score
scored = scored.withColumn(
    "anomaly_score",
    col("score_high_return_count") + col("score_high_total_refund") +
    col("score_high_avg_refund") + col("score_high_approval_rate") +
    col("score_multiple_reasons")
)

# Build rules_violated string
scored = scored.withColumn(
    "rules_violated",
    concat_ws(", ",
        when(col("score_high_return_count") > 0,
             lit(f"high_return_count ({W_HIGH_RETURN_COUNT})")),
        when(col("score_high_total_refund") > 0,
             lit(f"high_total_refund ({W_HIGH_TOTAL_REFUND})")),
        when(col("score_high_avg_refund") > 0,
             lit(f"high_avg_refund ({W_HIGH_AVG_REFUND})")),
        when(col("score_high_approval_rate") > 0,
             lit(f"high_approval_rate ({W_HIGH_APPROVAL_RATE})")),
        when(col("score_multiple_reasons") > 0,
             lit(f"multiple_reasons ({W_MULTIPLE_REASONS})")),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Flag Customers Scoring >= 40

# COMMAND ----------

flagged = scored.filter(col("anomaly_score") >= 40)
flagged_list = flagged.collect()
print(f"Flagged customers (score >= 40): {len(flagged_list)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generate Investigation Briefs (LLM)

# COMMAND ----------

results = []

for row in flagged_list:
    prompt = f"""You are a fraud analyst writing an investigation brief for a returns fraud review board.

Customer: {row["customer_name"]} (ID: {row["customer_id"]})
Segment: {row["segment"]}, Region: {row["region"]}
Total Returns: {row["total_returns"]}
Total Refund Value: ${row["total_refund_value"]:.2f}
Average Refund per Return: ${row["avg_refund_per_return"]:.2f}
Approval Rate: {row["approval_rate"]:.1f}%
Distinct Return Reasons: {row["distinct_reason_count"]} ({row["return_reasons"]})
Anomaly Score: {row["anomaly_score"]}/100
Rules Violated: {row["rules_violated"]}

Write a 4-5 sentence investigation brief that:
1. Cites the actual numbers above (not generic statements)
2. Acknowledges at least one innocent explanation for the pattern
3. Identifies the single strongest fraud signal from the data
4. Recommends exactly 2 specific next actions for the investigator

Be factual and balanced."""

    brief = call_llm(prompt)

    results.append({
        "customer_id": row["customer_id"],
        "customer_name": row["customer_name"],
        "segment": row["segment"],
        "region": row["region"],
        "total_returns": row["total_returns"],
        "total_refund_value": float(row["total_refund_value"]),
        "avg_refund_per_return": float(row["avg_refund_per_return"]),
        "approval_rate": float(row["approval_rate"]),
        "distinct_reason_count": row["distinct_reason_count"],
        "return_reasons": row["return_reasons"],
        "anomaly_score": row["anomaly_score"],
        "rules_violated": row["rules_violated"],
        "investigation_brief": brief,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Write to Gold

# COMMAND ----------

schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("segment", StringType()),
    StructField("region", StringType()),
    StructField("total_returns", IntegerType()),
    StructField("total_refund_value", DoubleType()),
    StructField("avg_refund_per_return", DoubleType()),
    StructField("approval_rate", DoubleType()),
    StructField("distinct_reason_count", IntegerType()),
    StructField("return_reasons", StringType()),
    StructField("anomaly_score", IntegerType()),
    StructField("rules_violated", StringType()),
    StructField("investigation_brief", StringType()),
])

result_df = spark.createDataFrame(results, schema=schema)
result_df = result_df.withColumn("generated_at", current_timestamp())

result_df.write.mode("overwrite").saveAsTable("globalmart.gold.flagged_return_customers")

print(f"Wrote {len(results)} flagged customer records to globalmart.gold.flagged_return_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Results

# COMMAND ----------

display(spark.table("globalmart.gold.flagged_return_customers").orderBy(col("anomaly_score").desc()))
