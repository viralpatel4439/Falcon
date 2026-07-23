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

    /// Storage backend: `local` (default) or `s3` for third-party object storage.
    #[arg(long)]
    pub storage: Option<String>,
    /// S3-compatible endpoint URL (e.g. https://s3.amazonaws.com, http://localhost:9000).
    #[arg(long)]
    pub s3_url: Option<String>,
    /// S3 region (default us-east-1; use `auto` for Cloudflare R2).
    #[arg(long)]
    pub s3_region: Option<String>,
    /// S3 bucket name.
    #[arg(long)]
    pub s3_bucket: Option<String>,
    /// S3 access key id.
    #[arg(long)]
    pub s3_access_key: Option<String>,
    /// S3 secret access key.
    #[arg(long)]
    pub s3_secret_key: Option<String>,
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
    /// Tokio worker threads (multi-core). 0 or unset = one per logical CPU.
    #[arg(long)]
    pub worker_threads: Option<usize>,
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
