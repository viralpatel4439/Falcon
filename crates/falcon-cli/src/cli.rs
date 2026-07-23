use clap::{Args, Parser, Subcommand};

/// Falcon — a fast, safe, multi-core data platform: FalconDB (key-value),
/// Falcon Queue, Falcon Pub/Sub, Falcon Event Streaming, and Falcon Realtime
/// DB, behind one binary.
///
/// Run a node with `falcon serve`; talk to a running node with the client
/// subcommands (`falcon get`, `falcon put`, `falcon topic publish`, …).
#[derive(Parser, Debug)]
#[command(name = "falcon", version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Server flags accepted directly (no subcommand) for backward compat:
    /// `falcon --http-bind ...` behaves like `falcon serve --http-bind ...`.
    #[command(flatten)]
    pub serve: ServeArgs,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run a Falcon node (KV, pub/sub, queues, streams, realtime, replication).
    Serve(ServeArgs),

    /// Get a key's value from a running node.
    Get(KeyArgs),
    /// Put (set) a key's value. Value is read from the arg or stdin.
    Put(PutArgs),
    /// Delete a key.
    Del(KeyArgs),
    /// Scan keys by prefix.
    Scan(ScanArgs),

    /// Pub/Sub topic operations.
    #[command(subcommand)]
    Topic(TopicCmd),
    /// Work queue operations.
    #[command(subcommand)]
    Queue(QueueCmd),
    /// Event stream operations.
    #[command(subcommand)]
    Stream(StreamCmd),

    /// Print a node's health/feature JSON.
    Health(ClientArgs),
    /// Print a node's Prometheus metrics.
    Metrics(ClientArgs),
}

/// Everything needed to run a node. Every field maps to a config value and
/// overrides the config file (defaults < file < env < flags).
#[derive(Args, Debug, Default)]
pub struct ServeArgs {
    /// Path to a TOML config file. If omitted, built-in defaults are used.
    #[arg(long, env = "FALCON_CONFIG")]
    pub config: Option<String>,

    /// HTTP/WebSocket/UI bind address.
    #[arg(long, env = "FALCON_HTTP_BIND")]
    pub http_bind: Option<String>,

    /// Binary wire-protocol bind address.
    #[arg(long, env = "FALCON_WIRE_BIND")]
    pub wire_bind: Option<String>,

    /// Disable the fast binary protocol server (enabled by default).
    #[arg(long, env = "FALCON_WIRE_DISABLED")]
    pub wire_disabled: bool,

    #[arg(long, env = "FALCON_NODE_ID")]
    pub node_id: Option<String>,

    #[arg(long, env = "FALCON_REGION")]
    pub region: Option<String>,

    #[arg(long, env = "FALCON_DATA_DIR")]
    pub data_dir: Option<String>,

    /// Default storage tier for keyspaces: hot | warm | cold | tiered | sharded.
    #[arg(long, env = "FALCON_DEFAULT_TIER")]
    pub default_tier: Option<String>,

    /// Shared-secret API key required on all client + inter-node connections.
    /// When unset (default), auth is OFF. Accepts `--api-key` / `--auth-token`.
    #[arg(long = "api-key", visible_alias = "auth-token", env = "FALCON_API_KEY")]
    pub api_key: Option<String>,

    /// Tokio worker threads (multi-core). 0 or unset = one per logical CPU.
    #[arg(long, env = "FALCON_WORKER_THREADS")]
    pub worker_threads: Option<usize>,

    /// Declare a topic: `--topic name` or `--topic name:durable`. Repeatable.
    #[arg(long = "topic", value_name = "NAME[:MODE]")]
    pub topics: Vec<String>,

    /// Declare a queue: `--queue name` or `--queue name:ack_secs`. Repeatable.
    #[arg(long = "queue", value_name = "NAME[:ACK_SECS]")]
    pub queues: Vec<String>,

    /// Declare a stream: `--stream name` or `--stream name:partitions`. Repeatable.
    #[arg(long = "stream", value_name = "NAME[:PARTITIONS]")]
    pub streams: Vec<String>,

    /// Enable WebSocket realtime subscriptions globally.
    #[arg(long, env = "FALCON_SUBSCRIPTIONS")]
    pub subscriptions: bool,

    #[arg(long, env = "FALCON_LOG_LEVEL", default_value = "info")]
    pub log_level: String,
}

/// Options shared by every client subcommand.
#[derive(Args, Debug, Clone)]
pub struct ClientArgs {
    /// Base URL of the node's HTTP API.
    #[arg(long, env = "FALCON_ADDR", default_value = "http://127.0.0.1:8080")]
    pub addr: String,
    /// API key, if the node has auth enabled.
    #[arg(long, env = "FALCON_API_KEY")]
    pub api_key: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct KeyArgs {
    pub key: String,
    /// Keyspace (default: "default").
    #[arg(long, default_value = "default")]
    pub keyspace: String,
    #[command(flatten)]
    pub client: ClientArgs,
}

#[derive(Args, Debug, Clone)]
pub struct PutArgs {
    pub key: String,
    /// The value. If omitted, read from stdin.
    pub value: Option<String>,
    #[arg(long, default_value = "default")]
    pub keyspace: String,
    /// Optional TTL in seconds.
    #[arg(long)]
    pub ttl: Option<u64>,
    #[command(flatten)]
    pub client: ClientArgs,
}

#[derive(Args, Debug, Clone)]
pub struct ScanArgs {
    #[arg(long, default_value = "")]
    pub prefix: String,
    #[arg(long, default_value = "default")]
    pub keyspace: String,
    #[command(flatten)]
    pub client: ClientArgs,
}

#[derive(Subcommand, Debug)]
pub enum TopicCmd {
    /// Publish a payload to a topic.
    Publish {
        topic: String,
        /// Payload; if omitted, read from stdin.
        payload: Option<String>,
        #[command(flatten)]
        client: ClientArgs,
    },
}

#[derive(Subcommand, Debug)]
pub enum QueueCmd {
    /// Push a job onto a queue.
    Push {
        queue: String,
        payload: Option<String>,
        #[command(flatten)]
        client: ClientArgs,
    },
    /// Pop (and auto-ack) one job from a queue for a consumer group.
    Pop {
        queue: String,
        #[arg(long, default_value = "cli")]
        group: String,
        #[command(flatten)]
        client: ClientArgs,
    },
}

#[derive(Subcommand, Debug)]
pub enum StreamCmd {
    /// Append a record to a stream, routed by --key.
    Append {
        stream: String,
        payload: Option<String>,
        #[arg(long, default_value = "")]
        key: String,
        #[command(flatten)]
        client: ClientArgs,
    },
    /// Poll a partition for a consumer group (records after its commit).
    Poll {
        stream: String,
        #[arg(long)]
        partition: usize,
        #[arg(long, default_value = "cli")]
        group: String,
        #[command(flatten)]
        client: ClientArgs,
    },
}
