# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05 — UC1: AI Data Quality Reporter
# MAGIC
# MAGIC Reads `rejected_records` from Silver, groups by (entity, field, issue_type),
# MAGIC generates LLM explanations of each data quality issue, and writes the
# MAGIC audit report to `globalmart.gold.dq_audit_report`.
# MAGIC
# MAGIC **Model:** `databricks-gpt-oss-20b`

# COMMAND ----------

# MAGIC %pip install openai --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql.functions import col, count, collect_list, lit, current_timestamp
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

# MAGIC %md
# MAGIC ## Response Parser
# MAGIC `databricks-gpt-oss-20b` returns a Python object (list of dicts), not a JSON string.

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

# COMMAND ----------

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
# MAGIC ## Read Rejected Records and Audit Log

# COMMAND ----------

rejected_df = spark.table("globalmart.silver.rejected_records")
audit_df = spark.table("globalmart.silver.pipeline_audit_log")

# COMMAND ----------

# Group by (source_table, affected_field, issue_type)
grouped = (
    rejected_df
    .groupBy("source_table", "affected_field", "issue_type")
    .agg(
        count("*").alias("rejected_count"),
        collect_list("record_data").alias("_sample_records_all"),
    )
    .collect()
)

# Build audit lookup: entity → bronze_count
audit_rows = audit_df.collect()
audit_lookup = {}
for row in audit_rows:
    audit_lookup[row["entity"]] = row["bronze_count"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate LLM Explanations

# COMMAND ----------

# Map source_table to entity name
TABLE_TO_ENTITY = {
    "clean_customers": "customers",
    "clean_orders": "orders",
    "clean_transactions": "transactions",
    "clean_returns": "returns",
    "clean_products": "products",
    "clean_vendors": "vendors",
}

results = []

for row in grouped:
    source_table = row["source_table"]
    affected_field = row["affected_field"]
    issue_type = row["issue_type"]
    rejected_count = row["rejected_count"]
    all_samples = row["_sample_records_all"]

    entity = TABLE_TO_ENTITY.get(source_table, source_table)
    total_records = audit_lookup.get(entity, 0)
    rejection_rate = (rejected_count / total_records * 100) if total_records > 0 else 0.0

    # Take up to 3 sample records
    samples = all_samples[:3] if len(all_samples) >= 3 else all_samples
    samples_json = json.dumps(samples)

    prompt = f"""You are a data quality analyst writing for a business audience.

A data quality check found {rejected_count} records out of {total_records} total {entity} records
({rejection_rate:.1f}%) were rejected because the "{affected_field}" field had a "{issue_type}" problem.

Here are sample rejected records:
{samples_json}

Write a 3-4 sentence explanation that:
1. Describes the problem in plain business language (do NOT use words like NULL, parse failure, schema, boolean, or any technical database terms)
2. Names the specific field and gives examples of the bad values found in the samples
3. Identifies which business report or audit figure could be wrong because of this issue
4. Recommends what the data steward should investigate to fix it

Keep it concise and actionable."""

    explanation = call_llm(prompt)

    results.append({
        "entity": entity,
        "affected_field": affected_field,
        "issue_type": issue_type,
        "rejected_count": rejected_count,
        "total_entity_records": total_records,
        "rejection_rate_pct": round(rejection_rate, 2),
        "sample_records": samples_json,
        "ai_explanation": explanation,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

schema = StructType([
    StructField("entity", StringType()),
    StructField("affected_field", StringType()),
    StructField("issue_type", StringType()),
    StructField("rejected_count", IntegerType()),
    StructField("total_entity_records", IntegerType()),
    StructField("rejection_rate_pct", DoubleType()),
    StructField("sample_records", StringType()),
    StructField("ai_explanation", StringType()),
])

result_df = spark.createDataFrame(results, schema=schema)
result_df = result_df.withColumn("generated_at", current_timestamp())

result_df.write.mode("overwrite").saveAsTable("globalmart.gold.dq_audit_report")

print(f"Wrote {len(results)} data quality audit rows to globalmart.gold.dq_audit_report")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Results

# COMMAND ----------

display(spark.table("globalmart.gold.dq_audit_report"))
