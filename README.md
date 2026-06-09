# cx-qa-platform

Automated QA pipeline for customer support operations — processes 100% of tickets but calls the LLM on only ~2–5% of them, cutting API costs by 95%.

> **Status:** 🚧 In Progress — Phase 1 (Webhook Ingestion)

---

## The Problem

Businesses running customer support at scale face a triangle they can't escape:

| Option | Problem |
|---|---|
| Manual QA team | Doesn't scale — one analyst reads ~80 tickets/day |
| LLM reviews every ticket | API costs exceed the business case |
| Skip QA entirely | Quality degrades, no data to act on |

**This project solves it with a 3-tier triage funnel** — fully automated, but only the genuinely hard cases (~2–5%) ever reach an LLM.

---

## Architecture

```
[Zendesk / Intercom]
        │  Webhook (HTTP POST)
        ▼
[FastAPI]  →  [Kafka]
                │
                ▼
        [Spark Streaming]
                │
        ┌───────┴──────────────────────┐
        │                              │
        ▼                              │
[Tier 1: Rule-based Filter]            │  ~35% flagged
  Regex · blacklist · AHT outliers     │
        │ pass                         │
        ▼                              │
[Tier 2: Delta Sentiment]              │  ~10% more flagged
  DistilBERT · agent tone classifier   │
        │ red flag only                │
        ▼                              │
[Tier 3: LLM Judge] ◄──────────────────┘
  n8n · GPT-4o / Claude API   ~2–5% of total tickets
        │
        ▼
[Apache Iceberg]  ◄──  [Debezium CDC  ◄──  Postgres]
        │
      [dbt]
        │
     [Trino]  →  CX Dashboard
```

### How the three tiers work

**Tier 1 — Rule-based ($0 cost)**
Spark Streaming consumes directly from Kafka and runs regex rules across every ticket. Flags immediately if the agent used prohibited language, skipped a required phrase ("sorry", "thank you") when the customer complained, or if response time is an outlier.

**Tier 2 — Delta Sentiment (internal compute only)**
DistilBERT runs on-premise. Rather than only checking sentiment at the end of a conversation, it measures **sentiment delta** at every segment — catching cases where the customer started angry and the agent made things worse, or where the customer gave up rather than escalated. A separate **Agent Tone Classifier** analyzes the agent's messages specifically, labeling tone as `empathetic`, `neutral`, `defensive`, or `dismissive`.

**Tier 3 — LLM Judge (API cost, ~2–5% of tickets)**
Only tickets flagged by Tier 1 or Tier 2 reach the LLM. Every call logs `prompt_version`, `tokens_used`, and `cost_usd`. Prompts are version-controlled in a `prompt_registry` table — changing a prompt creates a new version rather than overwriting, enabling A/B comparison across the same ticket set.

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| API Gateway | FastAPI | Async, Pydantic validation at the door, auto OpenAPI docs |
| Message Queue | Apache Kafka | Replay, multi-consumer groups, production-scale buffer |
| Stream Processing | Apache Spark Streaming | Same codebase for both stream and batch workloads |
| Local ML | DistilBERT (Hugging Face) | Lightweight, runs on CPU, no per-inference API cost |
| Orchestration | n8n | Visual workflow, configurable retry, easy to audit |
| LLM | OpenAI GPT-4o / Anthropic Claude | Pluggable — swap models without touching business logic |
| Storage | Apache Iceberg + MinIO | Time travel, schema evolution, no vendor lock-in |
| Transform | dbt + Trino | SQL-first, testable models, federated queries |
| CDC | Debezium | Captures DB change events without modifying application code |

> For the reasoning behind each major choice, see [`docs/adr/`](docs/adr/).

---

## Data Lineage

```
simulate_zendesk.py
      │
      │  POST /webhook/v1/conversation
      ▼
FastAPI  (schema validation)
      │
      │  publish → conversations.raw
      ▼
Kafka
      │
      ├──► Spark Tier 1 ──────────────► conversations.flagged
      ├──► Spark Tier 2 ──────────────► conversations.flagged
      └──► (pass-through) ──────────► Iceberg: conversations/  [qa_result: auto_pass]
                                               │
Postgres (customer DB)                         │
      │                                        │
   Debezium                                    │
      │                                        │
      │  publish → customer.profile.changes    │
      ▼                                        │
Kafka ──────────────────────────────────► Iceberg: customer_profiles/
                                               │
n8n ──► LLM API ──► JSON scores ──────► Iceberg: llm_scores/
                                               │
                                             dbt
                                    ┌──────────┴──────────┐
                               stg_conversations     stg_llm_scores
                                    └──────────┬──────────┘
                                         fct_qa_results
                                               │
                               ┌───────────────┼───────────────┐
                        agg_daily_csat   agg_cost_per_ticket   agg_funnel_efficiency
                                               │
                                             Trino
                                               │
                                           Dashboard
```

---

## Repository Structure

```
cx-qa-platform/
├── services/
│   ├── webhook-api/            # FastAPI — receives webhooks from Zendesk/Intercom
│   │   └── app/
│   │       ├── routers/        # POST /webhook/v1/conversation
│   │       ├── models/         # Pydantic schemas
│   │       └── core/           # Kafka producer, config
│   ├── triage-engine/          # Spark + DistilBERT — 3-tier filter
│   │   └── tiers/
│   │       ├── tier1_rules/    # Regex rule engine + rule_config.yaml
│   │       ├── tier2_sentiment/# Delta sentiment + agent tone classifier
│   │       └── tier3_llm/      # Prompt manager + LLM client
│   ├── cdc-connector/          # Debezium connector config
│   └── simulation/             # Simulates Zendesk sending webhook payloads
├── data/
│   ├── dbt/
│   │   └── models/
│   │       ├── staging/        # stg_conversations, stg_llm_scores, stg_customer_profiles
│   │       ├── marts/          # fct_qa_results, dim_agents, dim_customers
│   │       └── metrics/        # agg_daily_csat, agg_cost_per_ticket, agg_funnel_efficiency
│   └── iceberg/
│       └── schemas/            # Table schema definitions
├── infrastructure/
│   ├── kafka/topics/           # Topic creation script
│   ├── n8n/workflows/          # Exported n8n workflow JSONs
│   ├── trino/catalog/          # iceberg.properties
│   └── minio/                  # Bucket init
├── scripts/
│   ├── init_db.sql             # Creates prompt_registry + qa_feedback tables
│   └── seed_prompt_registry.sql# Inserts prompt v1.0.0
└── docs/
    └── adr/                    # Architecture Decision Records
        ├── ADR-001-kafka.md
        ├── ADR-002-iceberg-vs-delta.md
        └── ADR-003-distilbert-local.md
```

---

## Running Locally

### Prerequisites

- Docker + Docker Compose v2
- Python 3.11+
- 8 GB RAM minimum (DistilBERT + Spark)

### Start the platform

```bash
# 1. Clone and configure
git clone https://github.com/<your-username>/cx-qa-platform
cd cx-qa-platform
cp .env.example .env
# Fill in OPENAI_API_KEY or ANTHROPIC_API_KEY

# 2. Bring up all infrastructure
docker-compose up -d

# 3. Wait ~60s for services to be healthy, then initialize
bash infrastructure/kafka/topics/create_topics.sh
psql $DATABASE_URL -f scripts/init_db.sql
psql $DATABASE_URL -f scripts/seed_prompt_registry.sql

# 4. Run the data simulator
cd services/simulation
pip install -r requirements.txt
python simulate_zendesk.py --rate 10   # 10 requests/sec
```

### Verify the pipeline

```bash
# Watch raw tickets arriving in Kafka
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic conversations.raw \
  --from-beginning

# Watch flagged tickets
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic conversations.flagged
```

---

## Progress

- [ ] **Phase 1** — Webhook Ingestion (FastAPI + Kafka) ← *in progress*
- [ ] **Phase 2** — Tier 1: Rule-based Filter (Spark)
- [ ] **Phase 3** — Tier 2: Delta Sentiment (DistilBERT)
- [ ] **Phase 4** — Tier 3: LLM Judge (n8n + API)
- [ ] **Phase 5** — Data Lakehouse (Iceberg + Debezium CDC)
- [ ] **Phase 6** — dbt Models + Trino
- [ ] **Phase 7** — Cost Dashboard + Feedback Loop

---

## Architecture Decision Records

| ADR | Decision | Summary |
|---|---|---|
| [ADR-001](docs/adr/ADR-001-kafka.md) | Use Kafka over Redis Streams | Designed for production scale; enables replay and independent consumer groups |
| [ADR-002](docs/adr/ADR-002-iceberg-vs-delta.md) | Iceberg over Delta Lake | No vendor lock-in, native time travel, works with any query engine |
| [ADR-003](docs/adr/ADR-003-distilbert-local.md) | DistilBERT on-premise | Eliminates per-inference API cost for high-volume Tier 2 filtering |

---

## Dataset

[Twitter Customer Support Dataset](https://www.kaggle.com/datasets/thoughtvector/customer-support-on-twitter) — used to simulate realistic webhook payloads during local development.