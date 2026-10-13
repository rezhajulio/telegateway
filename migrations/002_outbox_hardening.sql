-- telegateway — Copyright (C) 2026 Rezha Julio
-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE outbox_new(
  id INTEGER PRIMARY KEY,
  bot_id TEXT NOT NULL,
  update_id INTEGER NOT NULL,
  payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'in_flight', 'delivered', 'dead')),
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL,
  UNIQUE(bot_id, update_id)
);

INSERT INTO outbox_new(id, bot_id, update_id, payload, status, attempts, next_attempt_at, last_error, created_at)
SELECT id, bot_id, update_id, payload, status, attempts, next_attempt_at, last_error, created_at
FROM outbox;

DROP TABLE outbox;

ALTER TABLE outbox_new RENAME TO outbox;

DROP INDEX IF EXISTS idx_outbox_due;

CREATE INDEX IF NOT EXISTS idx_outbox_claim ON outbox(bot_id, status, id);
