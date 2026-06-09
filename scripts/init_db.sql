CREATE TABLE IF NOT EXISTS prompt_registry (
    id          SERIAL PRIMARY KEY,
    version     VARCHAR(20) NOT NULL,
    content     TEXT NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    is_active   BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS qa_feedback (
    conversation_id  VARCHAR PRIMARY KEY,
    tenant_id        VARCHAR NOT NULL,
    llm_score        FLOAT,
    reviewer_id      VARCHAR,
    human_score      FLOAT,
    is_correct       BOOLEAN,
    correction_note  TEXT,
    reviewed_at      TIMESTAMP
);

CREATE INDEX idx_prompt_registry_active ON prompt_registry (is_active);
CREATE INDEX idx_qa_feedback_tenant    ON qa_feedback (tenant_id);