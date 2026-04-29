use crate::app_event::AppEvent;
use crate::app_event_sender::AppEventSender;
use axum::Router;
use axum::extract::Query;
use axum::extract::State;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::StatusCode;
use axum::response::Html;
use axum::response::IntoResponse;
use axum::routing::get;
use futures::SinkExt;
use futures::StreamExt;
use ratatui::text::Line;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::UdpSocket;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use uuid::Uuid;

const HISTORY_LIMIT: usize = 4000;

#[derive(Clone)]
pub(crate) struct MultiplayerSession {
    state: Arc<MultiplayerState>,
    url: String,
}

struct MultiplayerState {
    token: String,
    history: Mutex<Vec<String>>,
    tx: broadcast::Sender<ServerMessage>,
    app_event_tx: AppEventSender,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum ServerMessage {
    Snapshot { lines: Vec<String> },
    Transcript { lines: Vec<String> },
    System { text: String },
}

#[derive(Debug, Deserialize)]
struct InviteQuery {
    token: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum ClientMessage {
    Chat { name: String, text: String },
}

impl MultiplayerSession {
    pub(crate) async fn start(
        app_event_tx: AppEventSender,
        initial_history: Vec<String>,
    ) -> anyhow::Result<Self> {
        let token = Uuid::new_v4().to_string();
        let (tx, _) = broadcast::channel(512);
        let state = Arc::new(MultiplayerState {
            token: token.clone(),
            history: Mutex::new(trim_history(initial_history)),
            tx,
            app_event_tx,
        });

        let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        let addr = listener.local_addr()?;
        let display_host = lan_ip().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let url = format!("http://{display_host}:{}?token={token}", addr.port());
        let app = Router::new()
            .route("/", get(index))
            .route("/ws", get(ws_handler))
            .with_state(state.clone());

        tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, app).await {
                tracing::error!(error = %err, "multiplayer invite server stopped");
            }
        });

        Ok(Self { state, url })
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn publish_lines(&self, lines: Vec<String>) {
        if lines.is_empty() {
            return;
        }
        let state = self.state.clone();
        tokio::spawn(async move {
            {
                let mut history = state.history.lock().await;
                history.extend(lines.clone());
                if history.len() > HISTORY_LIMIT {
                    let overflow = history.len() - HISTORY_LIMIT;
                    history.drain(0..overflow);
                }
            }
            let _ = state.tx.send(ServerMessage::Transcript { lines });
        });
    }

    pub(crate) fn publish_system(&self, text: impl Into<String>) {
        let _ = self
            .state
            .tx
            .send(ServerMessage::System { text: text.into() });
    }
}

pub(crate) fn lines_to_plain_text(lines: Vec<Line<'static>>) -> Vec<String> {
    lines
        .iter()
        .map(|line| {
            line.spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect::<String>()
        })
        .collect()
}

fn trim_history(mut history: Vec<String>) -> Vec<String> {
    if history.len() > HISTORY_LIMIT {
        let overflow = history.len() - HISTORY_LIMIT;
        history.drain(0..overflow);
    }
    history
}

fn lan_ip() -> Option<IpAddr> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    socket.connect((Ipv4Addr::new(8, 8, 8, 8), 80)).ok()?;
    Some(socket.local_addr().ok()?.ip())
}

async fn index(
    State(state): State<Arc<MultiplayerState>>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    if query.get("token") != Some(&state.token) {
        return (StatusCode::UNAUTHORIZED, "Invalid invite token").into_response();
    }
    Html(INDEX_HTML).into_response()
}

async fn ws_handler(
    State(state): State<Arc<MultiplayerState>>,
    Query(query): Query<InviteQuery>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    if query.token != state.token {
        return (StatusCode::UNAUTHORIZED, "Invalid invite token").into_response();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<MultiplayerState>) {
    let (mut sender, mut receiver) = socket.split();
    let snapshot = {
        let history = state.history.lock().await;
        ServerMessage::Snapshot {
            lines: history.clone(),
        }
    };
    if let Ok(payload) = serde_json::to_string(&snapshot)
        && sender.send(Message::Text(payload.into())).await.is_err()
    {
        return;
    }

    let mut rx = state.tx.subscribe();
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Ok(message) => {
                        let Ok(payload) = serde_json::to_string(&message) else {
                            continue;
                        };
                        if sender.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(ClientMessage::Chat { name, text }) =
                            serde_json::from_str::<ClientMessage>(&text)
                        {
                            let name = clean_name(&name);
                            let text = text.trim();
                            if !text.is_empty() {
                                state.app_event_tx.send(AppEvent::MultiplayerChatMessage {
                                    author: name,
                                    text: text.to_string(),
                                });
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }
}

fn clean_name(name: &str) -> String {
    let name = name.trim();
    if name.is_empty() {
        return "Guest".to_string();
    }
    name.chars().take(40).collect()
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Codex multiplayer session</title>
  <style>
    :root { color-scheme: dark; }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: #111317;
      color: #f4f4f5;
      font: 14px/1.45 ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
    }
    main { min-height: 100vh; display: grid; grid-template-rows: auto 1fr auto; }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 12px 16px;
      border-bottom: 1px solid #2a2f3a;
      background: #181b21;
    }
    h1 { margin: 0; font-size: 14px; font-weight: 700; }
    #status { color: #cbd5e1; font-size: 12px; }
    #transcript {
      overflow: auto;
      padding: 16px;
      white-space: pre-wrap;
      word-break: break-word;
    }
    #transcript div { min-height: 1.45em; }
    form {
      display: grid;
      grid-template-columns: minmax(96px, 180px) 1fr auto;
      gap: 8px;
      padding: 12px;
      border-top: 1px solid #2a2f3a;
      background: #181b21;
    }
    input, button {
      border: 1px solid #3a4150;
      border-radius: 6px;
      background: #111317;
      color: #f4f4f5;
      font: inherit;
      min-height: 38px;
      padding: 8px 10px;
    }
    button {
      background: #2563eb;
      border-color: #2563eb;
      font-weight: 700;
      cursor: pointer;
    }
    button:disabled { opacity: 0.55; cursor: not-allowed; }
    @media (max-width: 720px) {
      form { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Codex multiplayer session</h1>
      <div id="status">Connecting</div>
    </header>
    <section id="transcript" aria-live="polite"></section>
    <form id="form">
      <input id="name" autocomplete="name" placeholder="Name" />
      <input id="message" autocomplete="off" placeholder="Message Codex" />
      <button id="send" type="submit">Send</button>
    </form>
  </main>
  <script>
    const params = new URLSearchParams(location.search);
    const token = params.get("token") || "";
    const statusEl = document.getElementById("status");
    const transcriptEl = document.getElementById("transcript");
    const form = document.getElementById("form");
    const nameInput = document.getElementById("name");
    const messageInput = document.getElementById("message");
    const sendButton = document.getElementById("send");

    nameInput.value = localStorage.getItem("codex.multiplayer.name") || "";

    function appendLines(lines) {
      const shouldStick = transcriptEl.scrollTop + transcriptEl.clientHeight >= transcriptEl.scrollHeight - 24;
      for (const line of lines) {
        const div = document.createElement("div");
        div.textContent = line || " ";
        transcriptEl.appendChild(div);
      }
      if (shouldStick) transcriptEl.scrollTop = transcriptEl.scrollHeight;
    }

    const scheme = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${scheme}://${location.host}/ws?token=${encodeURIComponent(token)}`);

    ws.addEventListener("open", () => {
      statusEl.textContent = "Connected";
      sendButton.disabled = false;
    });
    ws.addEventListener("close", () => {
      statusEl.textContent = "Disconnected";
      sendButton.disabled = true;
    });
    ws.addEventListener("message", (event) => {
      const message = JSON.parse(event.data);
      if (message.type === "snapshot") {
        transcriptEl.textContent = "";
        appendLines(message.lines || []);
      } else if (message.type === "transcript") {
        appendLines(message.lines || []);
      } else if (message.type === "system") {
        appendLines([`[system] ${message.text}`]);
      }
    });

    form.addEventListener("submit", (event) => {
      event.preventDefault();
      const text = messageInput.value.trim();
      if (!text || ws.readyState !== WebSocket.OPEN) return;
      const name = nameInput.value.trim() || "Guest";
      localStorage.setItem("codex.multiplayer.name", name);
      ws.send(JSON.stringify({ type: "chat", name, text }));
      messageInput.value = "";
      messageInput.focus();
    });
  </script>
</body>
</html>
"#;

#[cfg(test)]
mod tests {
    use super::HISTORY_LIMIT;
    use super::clean_name;
    use super::trim_history;

    #[test]
    fn clean_name_defaults_blank_names_to_guest() {
        assert_eq!(clean_name("   "), "Guest");
    }

    #[test]
    fn clean_name_limits_long_names() {
        let name = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        assert_eq!(clean_name(name).chars().count(), 40);
    }

    #[test]
    fn trim_history_keeps_latest_lines() {
        let history = (0..(HISTORY_LIMIT + 5))
            .map(|idx| idx.to_string())
            .collect::<Vec<_>>();
        let trimmed = trim_history(history);
        assert_eq!(trimmed.len(), HISTORY_LIMIT);
        assert_eq!(trimmed.first().map(String::as_str), Some("5"));
    }
}
