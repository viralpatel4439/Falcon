use clap::{Args, Parser, Subcommand};

/// Falcon — installable data products behind one CLI: Falcon Cache,
/// Falcon KV Store, Falcon Pub/Sub, Falcon Queue, and Falcon Event Stream.
/// Every product supports multi-region low-latency replication.
///
/// Install what you want, then run it:
///   falcon install cache            # set up a cache-only node
///   falcon serve                    # run it (reads your profile)
///
/// Falcon is configured ONLY through this CLI (and the web UI) — it never reads
/// environment variables. `falcon config set <key> <value>` edits your profile.
#[derive(Parser, Debug)]
#[command(name = "falcon", version, about, long_about = None)]
pub struct Cli {
    /// Path to the profile file (default: ~/.falcon/profile.toml). A flag, not
    /// an env var — Falcon never reads the environment for configuration.
    #[arg(long, global = true)]
    pub profile: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

// The CLI is parsed once at startup, so a large `Install` variant costs
// nothing at runtime; clap-derived subcommands can't hold a boxed args struct.
#[allow(clippy::large_enum_variant)]
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Install a Falcon product into your profile (cache | kv | pubsub | queue | stream).
    Install(InstallArgs),
    /// Remove a product from your profile.
    Uninstall(UninstallArgs),
    /// Show what's installed and the build's compiled products.
    Status,

    /// View or change configuration (the CLI/UI-only config path).
    #[command(subcommand)]
    Config(ConfigCmd),

    /// Manage multi-region replication peers.
    #[command(subcommand)]
    Peers(PeersCmd),

    /// Run this node using the installed profile.
    Serve(ServeArgs),

    // --- Client subcommands: talk to a running node over HTTP ---
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

/// `falcon install <feature>` — record a product in the profile and optionally
/// set its network/replication settings in one shot. No env vars: every option
/// is a flag, persisted to the profile.
#[derive(Args, Debug)]
pub struct InstallArgs {
    /// Which product to install: cache | kv | pubsub | queue | stream.
    pub feature: String,
    /// Node region (for replication routing / display).
    #[arg(long)]
    pub region: Option<String>,
    /// HTTP/WebSocket/UI bind address.
    #[arg(long)]
    pub http_bind: Option<String>,
    /// Node id.
    #[arg(long)]
    pub node_id: Option<String>,
    /// Data directory.
    #[arg(long)]
    pub data_dir: Option<String>,
    /// Shared-secret API key required on all connections (omit = auth off).
    #[arg(long)]
    pub api_key: Option<String>,
    /// Enable multi-region replication for this product.
    #[arg(long)]
    pub replicate: bool,
    /// Replication role: leader | follower.
    #[arg(long)]
    pub role: Option<String>,
    /// Peer node addresses for multi-region low-latency replication (repeatable).
    #[arg(long = "peer", value_name = "ADDR")]
    pub peers: Vec<String>,
    /// Leader address (required when role=follower).
    #[arg(long)]
    pub leader_addr: Option<String>,
    /// Write model: single-leader (default), multi-leader, or primary-queue.
    #[arg(long)]
    pub write_mode: Option<String>,

    /// Storage backend: `local` (default) or `remote` for third-party object storage.
    #[arg(long)]
    pub storage: Option<String>,
    /// Remote store endpoint URL (required for `--storage remote`; no default).
    #[arg(long)]
    pub remote_url: Option<String>,
    /// Remote store region label, if the store requires one (else omit).
    #[arg(long)]
    pub remote_region: Option<String>,
    /// Remote store bucket/container name (required for `--storage remote`).
    #[arg(long)]
    pub remote_bucket: Option<String>,
    /// Remote store access key id (required for `--storage remote`).
    #[arg(long)]
    pub remote_access_key: Option<String>,
    /// Remote store secret key (required for `--storage remote`).
    #[arg(long)]
    pub remote_secret_key: Option<String>,
}

#[derive(Args, Debug)]
pub struct UninstallArgs {
    /// Which product to remove: cache | kv | pubsub | queue | stream.
    pub feature: String,
}

#[derive(Subcommand, Debug)]
pub enum ConfigCmd {
    /// Set a config key in the profile, e.g. `falcon config set region us-east-1`.
    Set { key: String, value: String },
    /// Print one config value.
    Get { key: String },
    /// List every config key and its current value.
    List,
}

/// Manage the replication peer set for multi-region deployments. Peers are the
/// gRPC addresses (`host:7070`) of the other regions' nodes.
#[derive(Subcommand, Debug)]
pub enum PeersCmd {
    /// Add a peer's gRPC address, e.g. `falcon peers add 10.0.0.2:7070`.
    Add { addr: String },
    /// Remove a peer by address.
    Remove { addr: String },
    /// List configured peers and the local replication settings.
    List,
}

/// Options for `falcon serve`. Every field overrides the profile for ONE run;
/// the profile (written by `install`/`config`) remains the durable source of
/// truth. None of these are environment variables.
#[derive(Args, Debug, Default)]
pub struct ServeArgs {
    /// Advanced/testing escape hatch: load a full engine config TOML directly,
    /// bypassing the profile. Lets you declare arbitrary keyspaces, topics,
    /// queues, and streams for one run (used by the benchmark harness). Normal
    /// operation uses the installed profile instead.
    #[arg(long)]
    pub config: Option<String>,
    /// HTTP/WebSocket/UI bind address (overrides the profile for this run).
    #[arg(long)]
    pub http_bind: Option<String>,
    /// Binary wire-protocol bind address.
    #[arg(long)]
    pub wire_bind: Option<String>,
    /// Disable the fast binary protocol server for this run.
    #[arg(long)]
    pub wire_disabled: bool,
    #[arg(long)]
    pub node_id: Option<String>,
    #[arg(long)]
    pub region: Option<String>,
    #[arg(long)]
    pub data_dir: Option<String>,
    #[arg(long, default_value = "info")]
    pub log_level: String,
}

/// Options shared by every client subcommand. Addresses are flags (with a
/// sensible default), never environment variables.
#[derive(Args, Debug, Clone)]
pub struct ClientArgs {
    /// Base URL of the node's HTTP API.
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub addr: String,
    /// API key, if the node has auth enabled.
    #[arg(long)]
    pub api_key: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct KeyArgs {
    pub key: String,
    /// Target the Cache product (`/cache`) instead of the KV Store (`/kv`).
    #[arg(long)]
    pub cache: bool,
    #[command(flatten)]
    pub client: ClientArgs,
}

#[derive(Args, Debug, Clone)]
pub struct PutArgs {
    pub key: String,
    /// The value. If omitted, read from stdin.
    pub value: Option<String>,
    /// Optional TTL in seconds.
    #[arg(long)]
    pub ttl: Option<u64>,
    /// Target the Cache product (`/cache`) instead of the KV Store (`/kv`).
    #[arg(long)]
    pub cache: bool,
    #[command(flatten)]
    pub client: ClientArgs,
}

#[derive(Args, Debug, Clone)]
pub struct ScanArgs {
    #[arg(long, default_value = "")]
    pub prefix: String,
    #[command(flatten)]
    pub client: ClientArgs,
}

#[derive(Subcommand, Debug)]
pub enum TopicCmd {
    /// Publish a value to the node's topic.
    Publish {
        /// Value; if omitted, read from stdin.
        value: Option<String>,
        #[command(flatten)]
        client: ClientArgs,
    },
}

#[derive(Subcommand, Debug)]
pub enum QueueCmd {
    /// Push a job onto the queue.
    Push {
        value: Option<String>,
        #[command(flatten)]
        client: ClientArgs,
    },
    /// Dequeue (and auto-ack) one job.
    Pop {
        #[command(flatten)]
        client: ClientArgs,
    },
}

#[derive(Subcommand, Debug)]
pub enum StreamCmd {
    /// Append a record to the stream, optionally routed by --key.
    Append {
        value: Option<String>,
        #[arg(long, default_value = "")]
        key: String,
        #[command(flatten)]
        client: ClientArgs,
    },
    /// Read the next batch of records from the stream.
    Next {
        #[command(flatten)]
        client: ClientArgs,
    },
}
