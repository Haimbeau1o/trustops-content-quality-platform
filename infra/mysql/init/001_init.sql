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
