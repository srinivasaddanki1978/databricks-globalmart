# GlobalMart — Cross-Use-Case Reflection Answers

## 1. Prompt Design Differences: UC1 (DQ Reporter) vs UC2 (Fraud Investigator)

**UC1 prompt structure** is *template-driven and data-descriptive*. Each prompt receives a statistical summary (rejected count, total records, rejection rate, sample records) and asks the LLM to translate technical findings into business language. The prompt explicitly forbids technical jargon ("do NOT use words like NULL, parse failure, schema, boolean") because the audience is data stewards who need actionable recommendations, not database administrators. The prompt is constrained: it asks for exactly 3-4 sentences covering problem description, affected field/values, business report at risk, and investigation recommendation.

**UC2 prompt structure** is *investigative and evidence-based*. Each prompt receives a full customer profile (return count, refund totals, approval rate, distinct reasons, anomaly score, rules violated) and asks the LLM to write a fraud investigation brief. Unlike UC1, the UC2 prompt requires the LLM to acknowledge innocent explanations — a critical safeguard against false accusations. It also requires citing specific numbers, identifying the strongest fraud signal, and recommending exactly 2 next actions.

**Key difference in validation:** UC1 prompts are validated by checking that the explanation avoids forbidden terms and references the specific field. UC2 prompts are validated by checking that the brief cites actual numbers from the customer profile — generic statements like "this customer has many returns" would fail review because they don't cite the exact count and dollar amounts provided.

---

## 2. Response Parser: Why It's Needed

The `databricks-gpt-oss-20b` model returns a **Python object** (a list of dictionaries), not a JSON string. The response structure is:

```python
[
    {"type": "reasoning", "summary": [...]},
    {"type": "text", "text": "the actual answer"}
]
```

**Why the parser is needed:** Without it, every AI-generated column would contain the raw structured response including the reasoning chain — hundreds of tokens of internal model reasoning that the end user should never see. The `dq_audit_report.ai_explanation` column would show `[{'type': 'reasoning', 'summary': [...], ...}, {'type': 'text', 'text': '...'}]` instead of a clean business sentence.

**What happens without it in each UC:**
- **UC1:** The `ai_explanation` column would contain the full reasoning chain, making the DQ audit report unreadable for data stewards.
- **UC2:** The `investigation_brief` column would be a blob of structured data instead of a readable brief for the fraud review board.
- **UC3 (RAG):** The `answer` column in `rag_query_history` would contain reasoning tokens, making the product intelligence responses unusable.
- **UC4:** The `executive_summary` column would show raw model internals to executives — the opposite of the intended purpose.
- **UC4 ai_query():** SQL-native calls require `get_json_object()` with `'$[1]'` then `'$.text'` to extract the text block, which is the SQL equivalent of the Python parser.

**Implementation detail:** The parser uses `list()` and `dict()` iteration (not `json.loads()`) because the response is already a Python object. Calling `json.loads()` on it would raise a `TypeError` since it's not a string.

---

## 3. Rules vs LLM: Why Anomaly Scoring Is Rule-Based

The UC2 Fraud Investigator uses **5 deterministic, weighted rules** for anomaly scoring, with the LLM used only for generating investigation briefs after scoring is complete.

**Why rules, not LLM, for scoring:**

1. **Determinism:** The same customer profile must always produce the same anomaly score. An LLM might score the same customer differently on successive runs due to temperature and sampling variation. Rules guarantee that a customer with 5 returns and $2,000 in refunds will always score exactly the same points.

2. **Speed:** Scoring 374 customers with 5 arithmetic rules takes milliseconds. Sending each profile to an LLM for scoring would require 374 API calls at ~2 seconds each = ~12 minutes. Rules make the scoring step instant and scale to millions of customers.

3. **Threshold control:** Business stakeholders can adjust rule weights and thresholds without rewriting prompts. If the fraud team decides "high approval rate" should be weighted 25 instead of 15, they change one number. With LLM scoring, adjusting sensitivity requires prompt engineering — an imprecise and unpredictable process.

4. **Auditability:** The `rules_violated` column shows exactly which rules fired and their weights (e.g., "high_return_count (20), high_total_refund (25)"). An LLM-generated score would be a black box — the fraud review board couldn't explain to a customer why they were flagged.

5. **Cost:** LLM calls are reserved for the high-value task of generating investigation briefs for flagged customers only. If 20 out of 374 customers are flagged, we make 20 LLM calls instead of 374. This is a 95% reduction in LLM usage.

---

## 4. Production Redesign: UC3 (RAG) Needs the Most Work

**UC3 (Product Intelligence Assistant)** requires the most significant redesign for production deployment, for three reasons:

### Problem 1: Ephemeral FAISS Index
The current implementation builds a FAISS index in notebook memory on every run. When the notebook terminates, the index is gone. In production, every query would require rebuilding the index from scratch — loading all Gold/Metrics tables, converting to documents, embedding, and indexing. This takes minutes, making real-time queries impossible.

**Production solution:** Replace FAISS with **Databricks Vector Search** or a managed vector database. The index would be persistent, backed by Delta tables, and automatically updated when source data changes. This provides millisecond query latency without rebuilding.

### Problem 2: No Change Data Capture (CDC)
When Gold tables are updated (new products, updated sales figures), the current pipeline has no mechanism to detect which documents changed and re-embed only those. It re-embeds everything. With thousands of products across 5 regions, this becomes expensive.

**Production solution:** Implement **incremental document updates** using Delta table change data feed. When `mv_slow_moving_products` is updated, detect the changed product-region combinations, re-generate only those documents, compute new embeddings, and upsert into the vector store.

### Problem 3: No Serving Layer
The current RAG pipeline runs as a batch notebook. There's no API endpoint for ad-hoc questions. Users must open the notebook, modify the test questions, and re-run.

**Production solution:** Deploy a **Mosaic AI Model Serving endpoint** with a chain that encapsulates the embedding → retrieval → LLM flow. Users would query through a REST API or a Databricks app with a chat interface. The chain would use Databricks Vector Search for retrieval and a Foundation Model for generation.

### Why Not UC1, UC2, or UC4?
- **UC1 and UC4** are batch report generators — they read metrics, call the LLM once per group, and write results. This batch pattern works fine in production with scheduled jobs.
- **UC2** is also batch, and its rule-based scoring doesn't need infrastructure changes — only the LLM brief generation runs, and only for flagged customers.
- **UC3** is the only use case that needs to serve real-time, ad-hoc queries from users, which fundamentally changes the architecture from batch to serving.
