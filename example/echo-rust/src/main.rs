// telegateway example bot (Rust).
// Listens on 127.0.0.1:8083 and echoes every message back via sendMessage.
// Without TELEGRAM_BOT_TOKEN it logs what it would send and answers 200.
use axum::{routing::post, Json, Router};
use serde::Deserialize;
use std::env;
use std::time::Duration;

#[derive(Deserialize)]
struct Update {
    update_id: i64,
    message: Option<Message>,
}

#[derive(Deserialize)]
struct Message {
    text: Option<String>,
    chat: Chat,
}

#[derive(Deserialize)]
struct Chat {
    id: i64,
}

async fn webhook(Json(update): Json<Update>) -> &'static str {
    if let Some(message) = update.message {
        if let Some(text) = message.text {
            println!(
                "echo-rust: update {} from chat {}",
                update.update_id, message.chat.id
            );
            if let Err(e) = send_message(message.chat.id, &text).await {
                eprintln!("sendMessage failed: {e}");
            }
        }
    }
    "ok"
}

async fn send_message(chat_id: i64, text: &str) -> Result<(), reqwest::Error> {
    let token = env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
    if token.is_empty() {
        println!("[dry-run] would send to {chat_id}: {text}");
        return Ok(());
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let resp = client
        .post(format!("https://api.telegram.org/bot{token}/sendMessage"))
        .json(&serde_json::json!({ "chat_id": chat_id, "text": text }))
        .send()
        .await?;
    resp.error_for_status()?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let router = Router::new().route("/webhook", post(webhook));
    println!("echo-rust listening on 127.0.0.1:8083");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8083")
        .await
        .unwrap();
    axum::serve(listener, router).await.unwrap();
}
