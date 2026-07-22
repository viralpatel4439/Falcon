use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "falcon",
    version,
    about = "Falcon — a fast, safe, multi-region data platform: FalconDB (key-value), Falcon Queue, Falcon Pub/Sub, and Falcon Realtime DB"
)]
pub struct Cli {
    /// Path to a TOML config file. If omitted, built-in defaults are used.
    #[arg(long, env = "FALCON_CONFIG")]
    pub config: Option<String>,

    #[arg(long, env = "FALCON_HTTP_BIND")]
    pub http_bind: Option<String>,

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

    /// Shared-secret API key required on all client + inter-node
    /// connections. When unset (default), auth is OFF and no checks run
    /// anywhere. Accepts `--api-key` or the legacy `--auth-token`.
    #[arg(long = "api-key", visible_alias = "auth-token", env = "FALCON_API_KEY")]
    pub auth_token: Option<String>,

    #[arg(long, env = "FALCON_LOG_LEVEL", default_value = "info")]
    pub log_level: String,
}
