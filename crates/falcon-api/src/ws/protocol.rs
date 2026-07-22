use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    Subscribe {
        id: String,
        #[serde(default = "default_keyspace")]
        keyspace: String,
        key: Option<String>,
        prefix: Option<String>,
    },
    Unsubscribe {
        id: String,
    },
}

fn default_keyspace() -> String {
    "default".to_string()
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    Subscribed {
        id: String,
    },
    Unsubscribed {
        id: String,
    },
    Error {
        id: String,
        message: String,
    },
    Update {
        id: String,
        keyspace: String,
        key: String,
        value: Option<String>,
        sequence: u64,
        timestamp: u128,
        tombstone: bool,
    },
    ResyncRequired {
        id: String,
    },
}
