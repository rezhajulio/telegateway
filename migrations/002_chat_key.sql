-- Adds chat_key for chat-level ordered delivery.
ALTER TABLE outbox ADD COLUMN chat_key INTEGER;
-- Serves the per-chat ordering check in claim_due.
CREATE INDEX IF NOT EXISTS idx_outbox_chat ON outbox(bot_id, chat_key, id);
