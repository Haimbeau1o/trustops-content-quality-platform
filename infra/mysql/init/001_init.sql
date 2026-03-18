CREATE TABLE IF NOT EXISTS content_cases (
  case_id VARCHAR(64) PRIMARY KEY,
  status VARCHAR(32) NOT NULL,
  content_id VARCHAR(128) NOT NULL,
  content_type VARCHAR(64) NOT NULL,
  risk_signals JSON NOT NULL,
  evidence JSON NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS content_ingest_idempotency (
  idempotency_key VARCHAR(128) PRIMARY KEY,
  event_id VARCHAR(64) NOT NULL,
  case_id VARCHAR(64) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_content_ingest_event_id (event_id),
  KEY idx_content_ingest_case_id (case_id)
);

CREATE TABLE IF NOT EXISTS content_case_outbox (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_id VARCHAR(64) NOT NULL,
  case_id VARCHAR(64) NOT NULL,
  content_id VARCHAR(128) NOT NULL,
  content_type VARCHAR(64) NOT NULL,
  risk_signals JSON NOT NULL,
  payload JSON NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_error VARCHAR(1024) NOT NULL DEFAULT '',
  published_at TIMESTAMP NULL DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_content_case_outbox_status_next (status, next_attempt_at),
  KEY idx_content_case_outbox_case_id (case_id)
);

CREATE TABLE IF NOT EXISTS content_audit_logs (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_id VARCHAR(64) NOT NULL,
  case_id VARCHAR(64) NOT NULL,
  action VARCHAR(64) NOT NULL,
  detail VARCHAR(1024) NOT NULL DEFAULT '',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_content_audit_case (case_id, created_at),
  KEY idx_content_audit_action (action, created_at)
);

INSERT INTO content_cases (
  case_id,
  status,
  content_id,
  content_type,
  risk_signals,
  evidence
) VALUES (
  'case-001',
  'open',
  'content-seed-001',
  'video',
  JSON_ARRAY('low_quality'),
  JSON_ARRAY(JSON_OBJECT('type', 'rule_hit', 'detail', 'rule:seed_low_quality'))
)
ON DUPLICATE KEY UPDATE case_id = VALUES(case_id);
