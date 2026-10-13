CREATE TABLE IF NOT EXISTS outbox(
  id INTEGER PRIMARY KEY,
  bot_id TEXT NOT NULL,
  update_id INTEGER NOT NULL,
  payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL,
  UNIQUE(bot_id, update_id)
);
CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(bot_id, status, next_attempt_at);
