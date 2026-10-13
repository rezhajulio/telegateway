// telegateway — Copyright (C) 2026 Rezha Julio
// SPDX-License-Identifier: AGPL-3.0-or-later
//! Telegram update envelopes: parsing the raw JSON into `update_id`,
//! kind, and chat key for routing, rate limiting, and deduplication.
use serde_json::Value;
use thiserror::Error;

/// The kind of a Telegram update, derived from which payload key is present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateKind {
    /// `message`
    Message,
    /// `edited_message`
    EditedMessage,
    /// `channel_post`
    ChannelPost,
    /// `edited_channel_post`
    EditedChannelPost,
    /// `business_message`
    BusinessMessage,
    /// `callback_query`
    CallbackQuery,
    /// `inline_query`
    InlineQuery,
    /// `chosen_inline_result`
    ChosenInlineResult,
    /// `shipping_query`
    ShippingQuery,
    /// `pre_checkout_query`
    PreCheckoutQuery,
    /// `poll`
    Poll,
    /// `poll_answer`
    PollAnswer,
    /// `my_chat_member`
    MyChatMember,
    /// `chat_member`
    ChatMember,
    /// `chat_join_request`
    ChatJoinRequest,
    /// `message_reaction`
    MessageReaction,
    /// `message_reaction_count`
    MessageReactionCount,
    /// `business_connection`
    BusinessConnection,
    /// `deleted_business_messages`
    DeletedBusinessMessages,
    /// `chat_boost`
    ChatBoost,
    /// `removed_chat_boost`
    RemovedChatBoost,
    /// `purchased_paid_media`
    PurchasedPaidMedia,
    /// Any other (or future) payload key.
    Other,
}

/// The routing envelope of an update: the fields the gateway needs to make
/// admission and routing decisions, without interpreting bot business logic.
#[derive(Debug, Clone)]
pub struct UpdateEnvelope {
    /// Telegram's update id; the dedupe key.
    pub update_id: i64,
    /// Which payload key the update carries.
    pub kind: UpdateKind,
    /// Chat (or user) the rate limiter keys on; `None` when the update
    /// has no chat context.
    pub chat_key: Option<i64>,
}

/// Rejection reason when the JSON is not a valid Telegram update.
#[derive(Debug, Error)]
#[error("invalid Telegram update: {0}")]
pub struct EnvelopeError(pub String);

/// Adapt Telegram's raw update JSON into the gateway's routing envelope.
///
/// Only `update_id`, the update kind, and a stable chat key are extracted;
/// everything else is forwarded opaquely to the downstream bot.
pub fn envelope(value: &Value) -> Result<UpdateEnvelope, EnvelopeError> {
    let update_id = value
        .get("update_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| EnvelopeError("missing or invalid `update_id`".to_owned()))?;

    // (payload key, kind, chat lookup)
    let (kind, chat_key) = if let Some(m) = value.get("message") {
        (UpdateKind::Message, chat_id(m))
    } else if let Some(m) = value.get("edited_message") {
        (UpdateKind::EditedMessage, chat_id(m))
    } else if let Some(m) = value.get("channel_post") {
        (UpdateKind::ChannelPost, chat_id(m))
    } else if let Some(m) = value.get("edited_channel_post") {
        (UpdateKind::EditedChannelPost, chat_id(m))
    } else if let Some(m) = value.get("business_message") {
        (UpdateKind::BusinessMessage, chat_id(m))
    } else if let Some(q) = value.get("callback_query") {
        (
            UpdateKind::CallbackQuery,
            q.get("message")
                .and_then(chat_id)
                .or_else(|| q.get("from").and_then(user_id)),
        )
    } else if let Some(q) = value.get("inline_query") {
        (UpdateKind::InlineQuery, q.get("from").and_then(user_id))
    } else if let Some(q) = value.get("chosen_inline_result") {
        (
            UpdateKind::ChosenInlineResult,
            q.get("from").and_then(user_id),
        )
    } else if let Some(q) = value.get("shipping_query") {
        (UpdateKind::ShippingQuery, q.get("from").and_then(user_id))
    } else if let Some(q) = value.get("pre_checkout_query") {
        (
            UpdateKind::PreCheckoutQuery,
            q.get("from").and_then(user_id),
        )
    } else if value.get("poll").is_some() {
        (UpdateKind::Poll, None)
    } else if let Some(a) = value.get("poll_answer") {
        (UpdateKind::PollAnswer, a.get("user").and_then(user_id))
    } else if let Some(m) = value.get("my_chat_member") {
        (UpdateKind::MyChatMember, chat_id(m))
    } else if let Some(m) = value.get("chat_member") {
        (UpdateKind::ChatMember, chat_id(m))
    } else if let Some(r) = value.get("chat_join_request") {
        (UpdateKind::ChatJoinRequest, chat_id(r))
    } else if let Some(r) = value.get("message_reaction") {
        (UpdateKind::MessageReaction, chat_id(r))
    } else if let Some(r) = value.get("message_reaction_count") {
        (UpdateKind::MessageReactionCount, chat_id(r))
    } else if let Some(b) = value.get("business_connection") {
        (
            UpdateKind::BusinessConnection,
            b.get("user").and_then(user_id),
        )
    } else if let Some(b) = value.get("deleted_business_messages") {
        (UpdateKind::DeletedBusinessMessages, chat_id(b))
    } else if let Some(b) = value.get("chat_boost") {
        (UpdateKind::ChatBoost, chat_id(b))
    } else if let Some(b) = value.get("removed_chat_boost") {
        (UpdateKind::RemovedChatBoost, chat_id(b))
    } else if let Some(m) = value.get("purchased_paid_media") {
        (
            UpdateKind::PurchasedPaidMedia,
            m.get("from").and_then(user_id),
        )
    } else {
        (UpdateKind::Other, None)
    };

    Ok(UpdateEnvelope {
        update_id,
        kind,
        chat_key,
    })
}

fn chat_id(message: &Value) -> Option<i64> {
    message.get("chat")?.get("id")?.as_i64()
}

fn user_id(user: &Value) -> Option<i64> {
    user.get("id")?.as_i64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_message_envelope() {
        let update = json!({
            "update_id": 42,
            "message": {
                "message_id": 1,
                "chat": { "id": -100123, "type": "supergroup" },
                "text": "hello"
            }
        });
        let env = envelope(&update).unwrap();
        assert_eq!(env.update_id, 42);
        assert_eq!(env.kind, UpdateKind::Message);
        assert_eq!(env.chat_key, Some(-100123));
    }

    #[test]
    fn extracts_callback_query_envelope() {
        let update = json!({
            "update_id": 43,
            "callback_query": {
                "id": "abc",
                "from": { "id": 7 },
                "message": { "chat": { "id": 7 } }
            }
        });
        let env = envelope(&update).unwrap();
        assert_eq!(env.kind, UpdateKind::CallbackQuery);
        assert_eq!(env.chat_key, Some(7));
    }

    #[test]
    fn unknown_update_kind_is_other() {
        let update = json!({ "update_id": 44, "some_future_field": {} });
        let env = envelope(&update).unwrap();
        assert_eq!(env.kind, UpdateKind::Other);
        assert_eq!(env.chat_key, None);
    }

    #[test]
    fn rejects_missing_update_id() {
        let update = json!({ "message": {} });
        assert!(envelope(&update).is_err());
    }
    #[test]
    fn extracts_callback_query_without_message_fallback() {
        let update = json!({
            "update_id": 44,
            "callback_query": {
                "id": "abc",
                "from": { "id": 99 }
            }
        });
        let env = envelope(&update).unwrap();
        assert_eq!(env.kind, UpdateKind::CallbackQuery);
        assert_eq!(env.chat_key, Some(99));
    }

    #[test]
    fn extracts_shipping_and_pre_checkout_query() {
        let ship = json!({
            "update_id": 45,
            "shipping_query": {
                "id": "s1",
                "from": { "id": 101 }
            }
        });
        let env = envelope(&ship).unwrap();
        assert_eq!(env.kind, UpdateKind::ShippingQuery);
        assert_eq!(env.chat_key, Some(101));

        let checkout = json!({
            "update_id": 46,
            "pre_checkout_query": {
                "id": "p1",
                "from": { "id": 102 }
            }
        });
        let env = envelope(&checkout).unwrap();
        assert_eq!(env.kind, UpdateKind::PreCheckoutQuery);
        assert_eq!(env.chat_key, Some(102));
    }

    #[test]
    fn extracts_poll_has_no_chat_key() {
        let update = json!({
            "update_id": 47,
            "poll": {
                "id": "poll1"
            }
        });
        let env = envelope(&update).unwrap();
        assert_eq!(env.kind, UpdateKind::Poll);
        assert_eq!(env.chat_key, None);
    }

    #[test]
    fn extracts_new_telegram_kinds() {
        let biz = json!({
            "update_id": 48,
            "business_connection": { "user": { "id": 201 } }
        });
        let env = envelope(&biz).unwrap();
        assert_eq!(env.kind, UpdateKind::BusinessConnection);
        assert_eq!(env.chat_key, Some(201));

        let del = json!({
            "update_id": 49,
            "deleted_business_messages": { "chat": { "id": -1005 } }
        });
        let env = envelope(&del).unwrap();
        assert_eq!(env.kind, UpdateKind::DeletedBusinessMessages);
        assert_eq!(env.chat_key, Some(-1005));

        let react = json!({
            "update_id": 50,
            "message_reaction_count": { "chat": { "id": -1006 } }
        });
        let env = envelope(&react).unwrap();
        assert_eq!(env.kind, UpdateKind::MessageReactionCount);
        assert_eq!(env.chat_key, Some(-1006));

        let boost = json!({
            "update_id": 51,
            "chat_boost": { "chat": { "id": -1007 } }
        });
        let env = envelope(&boost).unwrap();
        assert_eq!(env.kind, UpdateKind::ChatBoost);
        assert_eq!(env.chat_key, Some(-1007));

        let unboost = json!({
            "update_id": 52,
            "removed_chat_boost": { "chat": { "id": -1008 } }
        });
        let env = envelope(&unboost).unwrap();
        assert_eq!(env.kind, UpdateKind::RemovedChatBoost);
        assert_eq!(env.chat_key, Some(-1008));

        let media = json!({
            "update_id": 53,
            "purchased_paid_media": { "from": { "id": 301 } }
        });
        let env = envelope(&media).unwrap();
        assert_eq!(env.kind, UpdateKind::PurchasedPaidMedia);
        assert_eq!(env.chat_key, Some(301));
    }

    use proptest::prelude::*;

    fn arb_json() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(|n| Value::Number(n.into())),
            ".*".prop_map(Value::String),
        ];
        leaf.prop_recursive(3, 16, 4, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
                prop::collection::hash_map(".*", inner, 0..4)
                    .prop_map(|m| Value::Object(m.into_iter().collect())),
            ]
        })
    }

    proptest! {
        #[test]
        fn envelope_never_panics_on_arbitrary_json(val in arb_json()) {
            let _ = envelope(&val);
        }
    }
}
