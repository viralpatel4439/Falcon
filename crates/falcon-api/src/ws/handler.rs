use crate::state::AppState;
use crate::ws::protocol::{ClientMsg, ServerMsg};
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};
use falcon_events::{ChangeEvent, ChangeValue};
use std::collections::HashMap;
use tokio::sync::broadcast;

pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

enum Filter {
    Key(String),
    Prefix(String),
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    // One outbound mpsc so multiple per-keyspace listener tasks can share a writer.
    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<ServerMsg>();
    let mut listener_tasks: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();

    let writer_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            let text = serde_json::to_string(&msg).unwrap_or_default();
            if sender.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    while let Some(Ok(msg)) = receiver.next().await {
        let Message::Text(text) = msg else { continue };
        let parsed: Result<ClientMsg, _> = serde_json::from_str(&text);
        match parsed {
            Ok(ClientMsg::Subscribe {
                id,
                keyspace,
                key,
                prefix,
            }) => {
                let filter = match (key, prefix) {
                    (Some(k), _) => Filter::Key(k),
                    (None, Some(p)) => Filter::Prefix(p),
                    (None, None) => {
                        let _ = out_tx.send(ServerMsg::Error {
                            id,
                            message: "subscribe requires 'key' or 'prefix'".to_string(),
                        });
                        continue;
                    }
                };

                let Some(ks) = state.node.keyspace(&keyspace) else {
                    let _ = out_tx.send(ServerMsg::Error {
                        id,
                        message: format!("unknown keyspace '{keyspace}'"),
                    });
                    continue;
                };
                let Some(bus) = ks.events() else {
                    let _ = out_tx.send(ServerMsg::Error {
                        id,
                        message: format!("subscriptions disabled for keyspace '{keyspace}'"),
                    });
                    continue;
                };

                let rx = bus.subscribe();
                let sub_id = id.clone();
                let out_tx2 = out_tx.clone();
                let task = tokio::spawn(listen_loop(rx, sub_id, filter, out_tx2));

                listener_tasks.insert(id.clone(), task);
                let _ = out_tx.send(ServerMsg::Subscribed { id });
            }
            Ok(ClientMsg::Unsubscribe { id }) => {
                if let Some(task) = listener_tasks.remove(&id) {
                    task.abort();
                }
                let _ = out_tx.send(ServerMsg::Unsubscribed { id });
            }
            Err(e) => {
                let _ = out_tx.send(ServerMsg::Error {
                    id: String::new(),
                    message: format!("invalid message: {e}"),
                });
            }
        }
    }

    for (_, task) in listener_tasks {
        task.abort();
    }
    writer_task.abort();
}

async fn listen_loop(
    mut rx: broadcast::Receiver<ChangeEvent>,
    sub_id: String,
    filter: Filter,
    out_tx: tokio::sync::mpsc::UnboundedSender<ServerMsg>,
) {
    loop {
        match rx.recv().await {
            Ok(event) => {
                if !matches_filter(&filter, &event.key) {
                    continue;
                }
                let (value, tombstone) = match &event.value {
                    ChangeValue::Put(v) => (Some(String::from_utf8_lossy(v).to_string()), false),
                    ChangeValue::Delete => (None, true),
                };
                let msg = ServerMsg::Update {
                    id: sub_id.clone(),
                    keyspace: event.keyspace.clone(),
                    key: String::from_utf8_lossy(&event.key).to_string(),
                    value,
                    sequence: event.sequence,
                    timestamp: event.timestamp,
                    tombstone,
                };
                if out_tx.send(msg).is_err() {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                if out_tx
                    .send(ServerMsg::ResyncRequired {
                        id: sub_id.clone(),
                    })
                    .is_err()
                {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

fn matches_filter(filter: &Filter, key: &[u8]) -> bool {
    match filter {
        Filter::Key(k) => key == k.as_bytes(),
        Filter::Prefix(p) => key.starts_with(p.as_bytes()),
    }
}
