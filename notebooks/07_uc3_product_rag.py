# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 07 — UC3: Product Intelligence Assistant (RAG)
# MAGIC
# MAGIC Builds a RAG pipeline over Gold/Metrics tables:
# MAGIC 1. Convert structured rows into natural language documents
# MAGIC 2. Embed with `all-MiniLM-L6-v2` (local, no API call)
# MAGIC 3. Build FAISS vector index
# MAGIC 4. Answer questions by retrieving top-k docs + LLM
# MAGIC 5. Log all queries to `globalmart.gold.rag_query_history`
# MAGIC
# MAGIC **Models:** `databricks-gpt-oss-20b` + `sentence-transformers/all-MiniLM-L6-v2`

# COMMAND ----------

# MAGIC %pip install sentence-transformers faiss-cpu openai --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import numpy as np
import faiss
from datetime import datetime
from sentence_transformers import SentenceTransformer
from pyspark.sql.functions import col, current_timestamp
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
# MAGIC ## Step 1: Load Gold + Metrics Data

# COMMAND ----------

products = spark.table("globalmart.gold.dim_products").collect()
vendors = spark.table("globalmart.gold.dim_vendors").collect()
slow_moving = spark.table("globalmart.gold.mv_slow_moving_products").collect()
vendor_returns = spark.table("globalmart.gold.mv_return_rate_by_vendor").collect()
revenue_by_region = spark.table("globalmart.gold.mv_revenue_by_region").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Convert Structured Rows to Documents

# COMMAND ----------

documents = []

# Product summaries — one per product
product_lookup = {p["product_id"]: p for p in products}
vendor_return_lookup = {v["vendor_id"]: v for v in vendor_returns}

# Aggregate slow-moving data by product
product_metrics = {}
for row in slow_moving:
    pid = row["product_id"]
    if pid not in product_metrics:
        product_metrics[pid] = {
            "total_sales": 0, "total_quantity": 0, "order_count": 0,
            "total_profit": 0, "regions": [], "is_slow_any": False,
        }
    m = product_metrics[pid]
    m["total_sales"] += (row["total_sales"] or 0)
    m["total_quantity"] += (row["total_quantity"] or 0)
    m["order_count"] += (row["order_count"] or 0)
    m["total_profit"] += (row["total_profit"] or 0)
    m["regions"].append(row["region"])
    if row["is_slow_moving"]:
        m["is_slow_any"] = True

for pid, p in product_lookup.items():
    metrics = product_metrics.get(pid, {})
    slow_flag = "YES (slow-moving)" if metrics.get("is_slow_any", False) else "No"
    regions_str = ", ".join(metrics.get("regions", []))
    doc = (
        f"Product: {p['product_name']} (ID: {pid}). "
        f"Brand: {p['brand']}. Category: {p['category']}. "
        f"Total sales: ${metrics.get('total_sales', 0):.2f}. "
        f"Total quantity sold: {metrics.get('total_quantity', 0)}. "
        f"Total profit: ${metrics.get('total_profit', 0):.2f}. "
        f"Order count: {metrics.get('order_count', 0)}. "
        f"Slow-moving: {slow_flag}. "
        f"Active in regions: {regions_str}."
    )
    documents.append(doc)

# Product-region summaries — one per product × region
for row in slow_moving:
    p = product_lookup.get(row["product_id"], {})
    slow_flag = "YES" if row["is_slow_moving"] else "No"
    doc = (
        f"Product-Region: {row['product_name']} in {row['region']} region. "
        f"Brand: {row['brand']}. "
        f"Sales: ${(row['total_sales'] or 0):.2f}. Quantity: {row['total_quantity'] or 0}. "
        f"Orders: {row['order_count'] or 0}. Profit: ${(row['total_profit'] or 0):.2f}. "
        f"Slow-moving in this region: {slow_flag}."
    )
    documents.append(doc)

# Vendor summaries — one per vendor
for v in vendor_returns:
    doc = (
        f"Vendor: {v['vendor_name']} (ID: {v['vendor_id']}). "
        f"Total orders: {v['total_orders'] or 0}. Total sales: ${(v['total_sales'] or 0):.2f}. "
        f"Return orders: {v['return_order_count'] or 0}. Total refunded: ${(v['total_refunded'] or 0):.2f}. "
        f"Return rate: {(v['return_rate_pct'] or 0):.1f}%."
    )
    documents.append(doc)

print(f"Total documents created: {len(documents)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Embed Documents with all-MiniLM-L6-v2

# COMMAND ----------

embed_model = SentenceTransformer("all-MiniLM-L6-v2")
embeddings = embed_model.encode(documents, show_progress_bar=True)
embeddings = np.array(embeddings).astype("float32")

print(f"Embeddings shape: {embeddings.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Build FAISS Index

# COMMAND ----------

dimension = embeddings.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(embeddings)
print(f"FAISS index built with {index.ntotal} vectors of dimension {dimension}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: RAG Query Pipeline

# COMMAND ----------

def rag_query(question: str, top_k: int = 5) -> dict:
    """Embed question, retrieve top-k docs from FAISS, generate LLM answer."""
    # Embed the question
    q_embedding = embed_model.encode([question]).astype("float32")

    # Search FAISS
    distances, indices = index.search(q_embedding, top_k)

    retrieved_docs = [documents[i] for i in indices[0] if i < len(documents)]
    top_distance = float(distances[0][0]) if len(distances[0]) > 0 else -1.0

    # Build context for LLM
    context = "\n\n".join([f"Document {i+1}: {doc}" for i, doc in enumerate(retrieved_docs)])

    prompt = f"""You are a product intelligence assistant for GlobalMart, a US retail chain.

Answer the following question using ONLY the information from the retrieved documents below.
If the answer is not contained in the documents, say "This information is not available in the current data."

Retrieved Documents:
{context}

Question: {question}

Provide a clear, specific answer citing actual numbers from the documents."""

    answer = call_llm(prompt)

    return {
        "question": question,
        "answer": answer,
        "retrieved_documents": json.dumps(retrieved_docs),
        "retrieved_count": str(len(retrieved_docs)),
        "top_distance": str(round(top_distance, 4)),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Run 5 Test Questions

# COMMAND ----------

test_questions = [
    "Which products are slow-moving and have low sales?",
    "Which vendor has the highest return rate?",
    "What are the top-selling products in the East region?",
    "Which products have the highest return cost?",
    "Which vendors are active in the West region and what are their sales?",
]

results = []
for q in test_questions:
    print(f"\nQ: {q}")
    result = rag_query(q)
    print(f"A: {result['answer'][:200]}...")
    results.append(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write Query History to Gold

# COMMAND ----------

schema = StructType([
    StructField("question", StringType()),
    StructField("answer", StringType()),
    StructField("retrieved_documents", StringType()),
    StructField("retrieved_count", StringType()),
    StructField("top_distance", StringType()),
])

result_df = spark.createDataFrame(results, schema=schema)
result_df = result_df.withColumn("generated_at", current_timestamp())

result_df.write.mode("overwrite").saveAsTable("globalmart.gold.rag_query_history")

print(f"\nWrote {len(results)} query results to globalmart.gold.rag_query_history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Results

# COMMAND ----------

display(spark.table("globalmart.gold.rag_query_history"))
