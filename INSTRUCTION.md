# Omnichannel CX Intelligence & Automated QA — Tài Liệu Thiết Kế Kỹ Thuật

> **Phiên bản:** 2.0  
> **Trạng thái:** Internal Draft  
> **Mục đích:** Tài liệu nội bộ để tự build — high-level architecture + lý do chọn công nghệ

---

## 1. Bài Toán Nghiệp Vụ

### Vấn đề cốt lõi

Doanh nghiệp B2B và sàn thương mại điện tử lớn đang đối mặt với mâu thuẫn không giải quyết được:

- **QA thủ công** tốn kém, không thể scale: Mỗi nhân viên QA chỉ đọc được ~50–80 ticket/ngày, trong khi một hệ thống tầm trung có thể nhận 10.000+ ticket/ngày.
- **QA bằng LLM 100%** giải quyết được vấn đề scale nhưng chi phí API gọi LLM cho toàn bộ ticket sẽ vượt ngưỡng khả thi về kinh doanh.
- **Tích hợp plugin phức tạp** vào Zendesk/Intercom/Freshdesk hiện có — không phải doanh nghiệp nào cũng sẵn sàng, và IT của họ thường từ chối.

### Mục tiêu thiết kế

| Mục tiêu | Chỉ số đo |
|---|---|
| Tự động hóa 100% quy trình QA | 0 ticket cần đọc thủ công ở tầng phân loại |
| Tiết kiệm ≥ 95% chi phí API LLM | Chỉ ~2–5% ticket cần gọi LLM |
| Tích hợp không xâm lấn | Khách hàng chỉ cần cài webhook, không cần cài plugin |
| Có thể chạy trên một máy dev | Toàn bộ infrastructure docker-compose |

---

## 2. Kiến Trúc Tổng Quan

```
[Zendesk / Intercom]
        │  HTTP POST (webhook)
        ▼
[FastAPI — Webhook Receiver]
        │  publish
        ▼
[Apache Kafka — Message Queue]
        │  consume (streaming)
        ▼
[Apache Spark Streaming]
        │
        ├─► TẦNG 1: Rule-based Filter (Regex)  ──► ~35% ticket bị lỗi cơ bản
        │              │ pass
        ▼
[DistilBERT Sentiment Analyzer]
        │
        ├─► TẦNG 2: Delta Sentiment Flag  ──────► ~10% ticket thêm bị flag
        │              │ red flag only
        ▼
[n8n Workflow Orchestrator]
        │  gọi API (chỉ ~2-5% ticket)
        ▼
[LLM — GPT-4 / Claude API]
        │  JSON output (chi tiết lỗi)
        ▼
[Apache Iceberg — Data Lakehouse]
        │
        ├─► dbt (transform, tính CSAT, AHT, cost per ticket)
        │
        └─► Trino (query engine) ──► Dashboard (Giám đốc CX)

[PostgreSQL] ──► [Debezium CDC] ──► [Kafka] ──► [Iceberg]
(Customer DB)                                   (customer profile events)
```

---

## 3. Chi Tiết Từng Thành Phần

### 3.1 Giai đoạn 1 — Ingestion & Integration

#### FastAPI — Webhook Receiver

**Vai trò:** Endpoint duy nhất tiếp nhận dữ liệu từ hệ thống khách hàng.

**Lý do chọn FastAPI thay vì Flask/Express:**
- Async native — xử lý đồng thời nhiều request mà không block.
- Auto-generate OpenAPI docs — tiện khi demo cho khách hàng.
- Pydantic validation tích hợp — từ chối dữ liệu sai format ngay ở cổng vào.

**Nguyên tắc thiết kế:** FastAPI **không xử lý logic**. Nhận JSON → validate schema → ném vào Kafka → trả về `202 Accepted`. Toàn bộ processing xảy ra downstream.

```
POST /webhook/v1/conversation
Body: { tenant_id, conversation_id, agent_id, messages: [...], closed_at }
Response: 202 Accepted
```

**Multi-tenant từ đầu:** Mỗi payload bắt buộc có `tenant_id`. Đây là quyết định schema không thể thay đổi sau, cần làm đúng ngay từ đầu.

---

#### Apache Kafka — Message Queue

**Vai trò:** Bộ đệm bất đồng bộ giữa ingestion và processing.

**Lý do chọn Kafka:**

> ⚠️ **Architecture Decision Record (ADR-001)**  
> **Bối cảnh:** Với project cá nhân và data giả lập, Kafka là overengineering về mặt kỹ thuật thuần túy. Redis Streams hay thậm chí một queue đơn giản là đủ cho traffic hiện tại.  
> **Quyết định:** Vẫn dùng Kafka vì:  
> 1. Thiết kế này nhắm đến production scale (siêu sale, hàng nghìn ticket/giây).  
> 2. Kafka cho phép nhiều consumer group đọc cùng một topic (Spark + Debezium đọc song song mà không ảnh hưởng nhau).  
> 3. Retention policy — có thể replay lại toàn bộ data nếu bug xảy ra ở downstream.  
> **Đánh đổi:** Tăng độ phức tạp vận hành, cần ZooKeeper/KRaft, tốn RAM hơn khi chạy local.

**Topic design:**
```
conversations.raw          ← FastAPI push vào đây
conversations.flagged      ← Spark push ticket bị flag
customer.profile.changes   ← Debezium CDC push vào đây
```

---

### 3.2 Giai đoạn 2 — Triage Funnel (Phễu Lọc Chi Phí)

Đây là phần cốt lõi tạo ra giá trị kinh tế của hệ thống.

#### Tầng 1 — Rule-based Filter (Chi phí: $0)

**Công nghệ:** Apache Spark Streaming consumer từ Kafka topic `conversations.raw`.

**Logic:**

```
Lỗi cờ đỏ ngay nếu bất kỳ điều nào sau đây đúng:
  - Agent dùng từ ngữ thô tục (Regex blacklist)
  - Agent KHÔNG nói "xin lỗi" / "sorry" khi conversation có từ "lỗi", "sai", "vỡ", "hỏng"
  - Agent KHÔNG nói "cảm ơn" / "thank" ở bất kỳ điểm nào trong conversation
  - Response time trung bình của agent > threshold (ví dụ > 5 phút/tin nhắn)
  - Conversation bị đóng trong < 30 giây (có thể là spam close)
```

**Output:** Ticket bị flag → push sang topic `conversations.flagged` kèm `flag_reason: "rule_violation"`. Ticket pass → vào Tầng 2.

**Lý do dùng Spark Streaming thay vì Kafka Streams:**
- Spark dễ mở rộng sang batch processing sau này (dùng lại cùng codebase).
- Quen thuộc hơn với hệ sinh thái data engineering.

---

#### Tầng 2 — Delta Sentiment Analysis (Chi phí: Compute nội bộ)

**Công nghệ:** DistilBERT từ Hugging Face, deploy local trên server nội bộ.

**Cải tiến so với bản gốc — Delta-based flagging:**

> ⚠️ **Vấn đề với bản gốc:** Chỉ đo sentiment ở cuối conversation tạo ra hai lỗ hổng:  
> 1. **False negative:** Khách hàng ban đầu đã tức giận, nhân viên xử lý dở nhưng khách chán không than nữa → sentiment cuối "neutral" → không bị flag → lọt lỗi.  
> 2. **Invisible agent error:** Nhân viên tư vấn sai thông tin nhưng khách không biết → sentiment positive → không bị flag.

**Logic mới:**

```python
# Chia conversation thành segments (đầu, giữa, cuối)
sentiment_scores = [analyze(segment) for segment in segments]

# Flag nếu bất kỳ điều nào sau đây xảy ra:
flag_conditions = [
    # 1. Sentiment giảm mạnh ở bất kỳ điểm nào (delta-based)
    any(sentiment_scores[i] - sentiment_scores[i+1] > DELTA_THRESHOLD 
        for i in range(len(sentiment_scores)-1)),
    
    # 2. Sentiment cuối conversation âm (vẫn giữ điều kiện gốc)
    sentiment_scores[-1] < NEGATIVE_THRESHOLD,
    
    # 3. Agent tone classifier: giọng điệu lạnh lùng / phòng thủ / thiếu empathy
    agent_tone_score < EMPATHY_THRESHOLD,
]
```

**Agent Tone Classifier** (bổ sung mới): Train một classifier riêng chỉ chạy trên phần text của agent, không phải khách hàng. Label gồm: `empathetic`, `neutral`, `defensive`, `dismissive`. Đây là điểm phân biệt quan trọng với các hệ thống QA thông thường.

**Output:** Ticket bị flag → push sang `conversations.flagged` kèm `flag_reason: "sentiment_drop" | "negative_end" | "poor_agent_tone"`. Ticket pass → lưu thẳng vào Iceberg với `qa_result: "auto_pass"`.

---

#### Tầng 3 — LLM Judge (Tối ưu API)

**Công nghệ:** n8n workflow orchestrator gọi LLM API (GPT-4 / Claude).

**Input:** Chỉ những ticket có trong topic `conversations.flagged` (từ Tầng 1 + Tầng 2). Ước tính ~2–5% tổng ticket.

**Thiết kế Prompt — có version control:**

> ⚠️ **Vấn đề với n8n thuần:** Không track được token usage, không version prompt, không A/B test được.

**Giải pháp:** Lưu prompt trong bảng `prompt_registry` ở Postgres:

```sql
CREATE TABLE prompt_registry (
    id          SERIAL PRIMARY KEY,
    version     VARCHAR(20) NOT NULL,  -- e.g. "v1.2.0"
    content     TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    is_active   BOOLEAN DEFAULT FALSE
);
```

Mỗi lần gọi LLM, n8n đọc prompt version `is_active = TRUE`. Khi muốn thay đổi prompt → tạo row mới, set active → toàn bộ kết quả từ đây trở đi dùng version mới. Kết quả cũ và mới có thể so sánh trực tiếp trên cùng tập ticket.

**LLM Output Schema (JSON bắt buộc):**

```json
{
  "conversation_id": "abc123",
  "tenant_id": "tenant_001",
  "prompt_version": "v1.2.0",
  "model": "gpt-4o",
  "tokens_input": 1240,
  "tokens_output": 380,
  "cost_usd": 0.0048,
  "qa_score": 3.5,
  "violations": [
    {
      "type": "wrong_information",
      "severity": "high",
      "evidence": "Agent nói thời gian hoàn tiền là 3 ngày, thực tế là 7 ngày",
      "turn": 6
    }
  ],
  "recommendation": "Cần training lại nhân viên về chính sách hoàn tiền"
}
```

**Retry logic trong n8n:** Cấu hình `maxRetries: 3`, `retryDelay: exponential`. Log mọi lần retry vào Iceberg để audit.

---

### 3.3 Giai đoạn 3 — Modern Data Lakehouse

#### Debezium CDC

**Vai trò:** Bắt các sự kiện thay đổi trạng thái khách hàng từ Postgres (ví dụ: nâng hạng lên VIP, thay đổi gói dịch vụ) mà không cần viết trigger hay modify application code.

**Lý do quan trọng:** Khi một khách hàng VIP complain, điểm QA cần được weighted khác so với khách thường. CDC cho phép join real-time giữa conversation data và customer tier data.

---

#### Apache Iceberg — Storage Layer

**Lý do chọn Iceberg thay vì Delta Lake / Hudi:**
- Hỗ trợ nhiều query engine (Trino, Spark, Flink) không vendor lock-in.
- Time travel native — có thể query lại data tại bất kỳ thời điểm nào (quan trọng cho audit).
- Schema evolution không cần rewrite toàn bộ table.

**Partition strategy:**
```
conversations/
  tenant_id=tenant_001/
    date=2025-01-15/
      *.parquet

llm_scores/
  tenant_id=tenant_001/
    date=2025-01-15/
      *.parquet
```

Partition by `(tenant_id, date)` — tối ưu cho query pattern "xem report của tenant X trong tháng Y".

---

#### dbt — Transformation Layer

**Các model cần build:**

```
staging/
  stg_conversations.sql       ← clean raw conversation data
  stg_llm_scores.sql          ← parse LLM JSON output
  stg_customer_profiles.sql   ← từ CDC events

marts/
  fct_qa_results.sql          ← fact table: 1 row = 1 conversation + QA result
  dim_agents.sql              ← agent dimension
  dim_customers.sql           ← customer dimension với tier

metrics/
  agg_daily_csat.sql          ← CSAT theo ngày / agent / tenant
  agg_cost_per_ticket.sql     ← chi phí LLM thực tế vs chi phí QA thủ công
  agg_funnel_efficiency.sql   ← % ticket lọt qua từng tầng
```

**Cost tracking model — đây là điểm quan trọng:**

```sql
-- agg_cost_per_ticket.sql
SELECT
    date,
    tenant_id,
    COUNT(*) AS total_tickets,
    SUM(CASE WHEN qa_method = 'llm' THEN 1 ELSE 0 END) AS llm_reviewed,
    SUM(cost_usd) AS total_llm_cost_usd,
    
    -- Chi phí QA thủ công ước tính (giả định $0.5/ticket để review thủ công)
    COUNT(*) * 0.5 AS manual_qa_cost_usd,
    
    -- ROI
    (COUNT(*) * 0.5 - SUM(cost_usd)) / (COUNT(*) * 0.5) * 100 AS cost_saving_pct
FROM fct_qa_results
GROUP BY date, tenant_id
```

---

#### Trino — Query Engine

**Vai trò:** Query engine đứng trên Iceberg, expose SQL interface cho dashboard.

**Lý do chọn Trino:** Hỗ trợ federated query — có thể join data từ Iceberg với Postgres trong một câu SQL mà không cần ETL thêm.

---

### 3.4 Feedback Loop — Active Learning

Đây là thành phần nâng cao hệ thống từ "chạy được" lên "tự cải thiện được".

**Luồng:**

```
LLM chấm điểm ticket
        │
        ▼
QA Manager review kết quả trên Dashboard
        │  label: "đúng" / "sai" / "một phần đúng"
        ▼
Feedback lưu vào bảng qa_feedback
        │
        ▼
Weekly batch job: lấy các case LLM sai
        │
        ├─► Fine-tune lại Agent Tone Classifier (Tầng 2)
        └─► Tạo PR để update prompt_registry (Tầng 3)
```

**Bảng feedback:**

```sql
CREATE TABLE qa_feedback (
    conversation_id  VARCHAR PRIMARY KEY,
    llm_score        FLOAT,
    reviewer_id      VARCHAR,
    human_score      FLOAT,
    is_correct       BOOLEAN,
    correction_note  TEXT,
    reviewed_at      TIMESTAMP
);
```

---

## 4. Chiến Lược Triển Khai

### 4.1 Data Simulation

Không cần data thật để build. Dùng **Twitter Customer Support Dataset** từ Kaggle.

**Script giả lập Zendesk webhook:**

```python
# simulate_zendesk.py
import httpx, time, random, json
from datasets import load_dataset

dataset = load_dataset("...")  # Twitter CS dataset

def format_as_conversation(row) -> dict:
    return {
        "tenant_id": f"tenant_{random.randint(1,3):03d}",
        "conversation_id": row["tweet_id"],
        "agent_id": row["author_id"],
        "messages": [...],
        "closed_at": row["created_at"]
    }

# Giả lập burst traffic (siêu sale scenario)
for row in dataset:
    payload = format_as_conversation(row)
    httpx.post("http://localhost:8000/webhook/v1/conversation", json=payload)
    time.sleep(random.uniform(0.01, 0.1))  # 10–100 req/s
```

---

### 4.2 Docker Compose Infrastructure

Toàn bộ hạ tầng chạy bằng một lệnh: `docker-compose up -d`

**Services:**

| Service | Image | Port |
|---|---|---|
| FastAPI | `python:3.11-slim` | 8000 |
| Kafka | `confluentinc/cp-kafka` | 9092 |
| Spark | `bitnami/spark:3.5` | 8080 |
| DistilBERT | custom Dockerfile | 8001 |
| n8n | `n8nio/n8n` | 5678 |
| Postgres | `postgres:16` | 5432 |
| Iceberg REST Catalog | `tabulario/iceberg-rest` | 8181 |
| Trino | `trinodb/trino` | 8082 |
| MinIO (S3-compatible) | `minio/minio` | 9000 |
| Debezium | `debezium/connect` | 8083 |

**Lưu ý:** Iceberg cần object storage. Dùng MinIO để giả lập S3 local — không phát sinh chi phí cloud.

---

### 4.3 Git Flow

```
main
  └── develop
        ├── feature/webhook-ingestion
        ├── feature/spark-triage-funnel
        ├── feature/distilbert-sentiment
        ├── feature/n8n-llm-workflow
        ├── feature/dbt-models
        └── feature/trino-dashboard
```

**README phải có:**
- Data Lineage diagram (dùng Mermaid hoặc draw.io)
- ADR section (lý do chọn Kafka, Iceberg, DistilBERT)
- Cost analysis: "Trước khi có hệ thống" vs "Sau khi có hệ thống"
- Hướng dẫn chạy local trong 5 phút

---

## 5. Thứ Tự Build Được Khuyến Nghị

```
Tuần 1  →  FastAPI webhook + Kafka setup + simulation script
Tuần 2  →  Spark Triage Tầng 1 (Rule-based)
Tuần 3  →  DistilBERT Tầng 2 (Delta sentiment + Agent tone)
Tuần 4  →  n8n + LLM integration + prompt_registry
Tuần 5  →  Iceberg + Debezium CDC
Tuần 6  →  dbt models (staging → marts → metrics)
Tuần 7  →  Trino + Dashboard + cost tracking
Tuần 8  →  Feedback loop + README + polish
```

---

## 6. Rủi Ro Kỹ Thuật Cần Theo Dõi

| Rủi ro | Khả năng xảy ra | Giải pháp |
|---|---|---|
| RAM không đủ chạy toàn bộ docker-compose | Cao (DistilBERT + Spark tốn RAM) | Tắt Spark UI, giảm JVM heap, dùng Spark local mode |
| DistilBERT inference quá chậm | Trung bình | Batch inference thay vì per-message, dùng ONNX runtime |
| LLM API rate limit | Thấp (vì chỉ gọi ~2–5% ticket) | n8n retry + queue với delay |
| Iceberg schema không nhất quán giữa các tenant | Trung bình | Enforce schema registry từ đầu ở FastAPI layer |
| n8n mất trạng thái khi restart | Cao (nếu không configure persistence) | Mount volume cho n8n data directory |

---

## 7. Định Nghĩa "Done" Cho Từng Phase

- **Phase 1 (Ingestion):** Script giả lập bắn 1.000 request/phút vào FastAPI, Kafka consumer lag < 5 giây.
- **Phase 2 (Triage):** Tầng 1 lọc đúng ≥ 90% các vi phạm luật rõ ràng trên test set. Tầng 2 F1-score ≥ 0.75 cho sentiment drop detection.
- **Phase 3 (LLM):** 100% ticket flagged có JSON output hợp lệ. Prompt version được track đầy đủ.
- **Phase 4 (Lakehouse):** dbt test passes, cost saving dashboard hiển thị đúng số.
- **Phase 5 (Complete):** Toàn bộ pipeline chạy end-to-end từ simulation script đến dashboard bằng một lệnh.