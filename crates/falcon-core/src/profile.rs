//! The Falcon profile — the *only* way a node is configured.
//!
//! Falcon does not read environment variables. A node's entire configuration
//! lives in a single TOML profile file that is written and edited exclusively
//! through the CLI (`falcon install`, `falcon config set`) or the web UI
//! (`POST /config`). At startup `falcon serve` loads this file and nothing
//! else; CLI flags to `serve` may override individual fields for one run, but
//! the durable source of truth is always the profile.
//!
//! The profile records **which product(s)** the node runs (its [`FeatureSet`])
//! plus the settings for each. It is deliberately small and hand-editable, but
//! users are expected to go through `falcon config` rather than a text editor.

use crate::config::{Config, KeyspaceConfig, QueueConfig, StreamConfig, TierName, TopicConfig};
use crate::feature::{Feature, FeatureSet};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Default profile location: `~/.falcon/profile.toml`. Overridable per-invocation
/// with `--profile <path>` (a flag, never an env var).
pub fn default_profile_path() -> PathBuf {
    let base = home_dir().unwrap_or_else(|| PathBuf::from("."));
    base.join(".falcon").join("profile.toml")
}

/// Minimal `$HOME` resolution without pulling in a crate. We read the process
/// environment for HOME here only to *locate* the profile file — this is not
/// configuration (no Falcon setting is taken from the environment).
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

/// The node identity + network settings a profile carries. Mirrors the parts of
/// [`Config`] a user configures; the rest of `Config` is derived per-feature.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileNode {
    #[serde(default = "default_node_id")]
    pub id: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default = "default_http_bind")]
    pub http_bind: String,
    #[serde(default = "default_wire_bind")]
    pub wire_bind: String,
    #[serde(default)]
    pub wire_enabled: bool,
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// Enable in-process TLS on every server hop (HTTP, wire, gRPC).
    #[serde(default)]
    pub tls_enabled: bool,
    /// PEM certificate chain file (required when `tls_enabled`).
    #[serde(default)]
    pub tls_cert: String,
    /// PEM private key file (required when `tls_enabled`).
    #[serde(default)]
    pub tls_key: String,
}

impl Default for ProfileNode {
    fn default() -> Self {
        Self {
            id: default_node_id(),
            region: default_region(),
            http_bind: default_http_bind(),
            wire_bind: default_wire_bind(),
            wire_enabled: false,
            api_key: String::new(),
            data_dir: default_data_dir(),
            log_level: default_log_level(),
            tls_enabled: false,
            tls_cert: String::new(),
            tls_key: String::new(),
        }
    }
}

fn default_node_id() -> String {
    "node-1".into()
}
fn default_region() -> String {
    "local".into()
}
fn default_http_bind() -> String {
    "0.0.0.0:8080".into()
}
fn default_wire_bind() -> String {
    "0.0.0.0:6380".into()
}
fn default_data_dir() -> String {
    "./data".into()
}
fn default_log_level() -> String {
    "info".into()
}

/// Multi-region replication settings, available to any product.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileReplication {
    #[serde(default)]
    pub enabled: bool,
    /// `leader` or `follower`.
    #[serde(default = "default_role")]
    pub role: String,
    #[serde(default = "default_grpc_bind")]
    pub grpc_bind: String,
    /// Leader address, required when `role = "follower"`.
    #[serde(default)]
    pub leader_addr: String,
    /// Peer node addresses for multi-region low-latency replication.
    #[serde(default)]
    pub peers: Vec<String>,
    /// Write model for replicated KV/cache keyspaces: `single-leader` (default),
    /// `multi-leader` (active-active, last-write-wins), or `primary-queue`
    /// (forward to a primary, ordered, no lost concurrent writes).
    #[serde(default = "default_write_mode")]
    pub write_mode: String,
}

fn default_write_mode() -> String {
    "single-leader".into()
}

impl Default for ProfileReplication {
    fn default() -> Self {
        Self {
            enabled: false,
            role: default_role(),
            grpc_bind: default_grpc_bind(),
            leader_addr: String::new(),
            peers: Vec::new(),
            write_mode: default_write_mode(),
        }
    }
}

fn default_role() -> String {
    "leader".into()
}
fn default_grpc_bind() -> String {
    "0.0.0.0:7070".into()
}

/// Where the `sharded` object-store tier keeps its bucket objects. `local`
/// (default) uses the node's `data_dir`; `remote` attaches a third-party object
/// store the operator fully specifies — Falcon ships no defaults for it. Fields
/// accept `remote_*` names (with `s3_*` aliases for older profiles).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileStorage {
    /// `local` or `remote`.
    #[serde(default = "default_backend")]
    pub backend: String,
    #[serde(default, alias = "s3_endpoint_url")]
    pub remote_endpoint_url: String,
    #[serde(default, alias = "s3_region")]
    pub remote_region: String,
    #[serde(default, alias = "s3_bucket")]
    pub remote_bucket: String,
    #[serde(default, alias = "s3_access_key_id")]
    pub remote_access_key_id: String,
    #[serde(default, alias = "s3_secret_access_key")]
    pub remote_secret_access_key: String,
    #[serde(default, alias = "s3_prefix")]
    pub remote_prefix: String,
}

impl Default for ProfileStorage {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            remote_endpoint_url: String::new(),
            remote_region: String::new(),
            remote_bucket: String::new(),
            remote_access_key_id: String::new(),
            remote_secret_access_key: String::new(),
            remote_prefix: String::new(),
        }
    }
}

fn default_backend() -> String {
    "local".into()
}

/// The full profile: which products run here, plus their settings.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Profile {
    /// The installed product(s). A slim binary holds exactly one; the `full`
    /// binary may hold several.
    #[serde(default)]
    pub features: FeatureSet,
    #[serde(default)]
    pub node: ProfileNode,
    #[serde(default)]
    pub replication: ProfileReplication,
    #[serde(default)]
    pub storage: ProfileStorage,
}

#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("no profile found at {0} — run `falcon install <feature>` first")]
    NotFound(PathBuf),
    #[error("failed to parse profile: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("failed to serialize profile: {0}")]
    Serialize(#[from] toml::ser::Error),
    #[error("io error on profile file: {0}")]
    Io(#[from] std::io::Error),
    #[error("unknown config key '{0}'")]
    UnknownKey(String),
    #[error("invalid value for '{key}': {reason}")]
    InvalidValue { key: String, reason: String },
}

impl Profile {
    /// Load a profile from disk, or a friendly NotFound if absent.
    pub fn load(path: &Path) -> Result<Self, ProfileError> {
        if !path.exists() {
            return Err(ProfileError::NotFound(path.to_path_buf()));
        }
        let s = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&s)?)
    }

    /// Load if present, else a default empty profile (no features). Used by
    /// `config set` so the first `set` before any `install` still works.
    pub fn load_or_default(path: &Path) -> Result<Self, ProfileError> {
        match Self::load(path) {
            Ok(p) => Ok(p),
            Err(ProfileError::NotFound(_)) => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }

    /// Persist the profile, creating the parent directory if needed.
    pub fn save(&self, path: &Path) -> Result<(), ProfileError> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let s = toml::to_string_pretty(self)?;
        std::fs::write(path, s)?;
        Ok(())
    }

    /// Set a dotted config key to a string value (the CLI/UI write path).
    /// Recognised keys mirror the profile fields users tune.
    pub fn set(&mut self, key: &str, value: &str) -> Result<(), ProfileError> {
        let bad = |reason: &str| ProfileError::InvalidValue {
            key: key.to_string(),
            reason: reason.to_string(),
        };
        match key {
            "node.id" | "id" => self.node.id = value.to_string(),
            "node.region" | "region" => self.node.region = value.to_string(),
            "http-bind" | "http_bind" | "node.http_bind" => self.node.http_bind = value.to_string(),
            "wire-bind" | "wire_bind" | "node.wire_bind" => self.node.wire_bind = value.to_string(),
            "wire-enabled" | "wire_enabled" => {
                self.node.wire_enabled = parse_bool(value).map_err(|_| bad("expected true/false"))?
            }
            "api-key" | "api_key" | "auth.api_key" => self.node.api_key = value.to_string(),
            "data-dir" | "data_dir" | "node.data_dir" => self.node.data_dir = value.to_string(),
            "log-level" | "log_level" => self.node.log_level = value.to_string(),
            "tls-enabled" | "tls_enabled" => {
                self.node.tls_enabled = parse_bool(value).map_err(|_| bad("expected true/false"))?
            }
            "tls-cert" | "tls_cert" => self.node.tls_cert = value.to_string(),
            "tls-key" | "tls_key" => self.node.tls_key = value.to_string(),
            "replication.enabled" | "replicate" => {
                self.replication.enabled =
                    parse_bool(value).map_err(|_| bad("expected true/false"))?
            }
            "replication.role" => {
                if value != "leader" && value != "follower" {
                    return Err(bad("expected 'leader' or 'follower'"));
                }
                self.replication.role = value.to_string();
            }
            "replication.grpc_bind" | "grpc-bind" => self.replication.grpc_bind = value.to_string(),
            "replication.leader_addr" | "leader-addr" => {
                self.replication.leader_addr = value.to_string()
            }
            "replication.peers" | "peers" => {
                self.replication.peers = value
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            "replication.write_mode" | "write-mode" => {
                match value {
                    "single-leader" | "multi-leader" | "primary-queue" => {
                        self.replication.write_mode = value.to_string()
                    }
                    _ => {
                        return Err(bad(
                            "expected 'single-leader', 'multi-leader', or 'primary-queue'",
                        ))
                    }
                }
            }
            "storage.backend" | "storage" => {
                // Accept `s3` as a legacy alias for `remote`.
                let v = if value == "s3" { "remote" } else { value };
                if v != "local" && v != "remote" {
                    return Err(bad("expected 'local' or 'remote'"));
                }
                self.storage.backend = v.to_string();
            }
            "storage.remote.url" | "remote-url" | "s3-url" => {
                self.storage.remote_endpoint_url = value.to_string()
            }
            "storage.remote.region" | "remote-region" | "s3-region" => {
                self.storage.remote_region = value.to_string()
            }
            "storage.remote.bucket" | "remote-bucket" | "s3-bucket" => {
                self.storage.remote_bucket = value.to_string()
            }
            "storage.remote.access_key_id" | "remote-access-key" | "s3-access-key" => {
                self.storage.remote_access_key_id = value.to_string()
            }
            "storage.remote.secret_access_key" | "remote-secret-key" | "s3-secret-key" => {
                self.storage.remote_secret_access_key = value.to_string()
            }
            "storage.remote.prefix" | "remote-prefix" | "s3-prefix" => {
                self.storage.remote_prefix = value.to_string()
            }
            other => return Err(ProfileError::UnknownKey(other.to_string())),
        }
        Ok(())
    }

    /// Read a dotted config key back as a display string (the `config get` path).
    pub fn get(&self, key: &str) -> Option<String> {
        Some(match key {
            "node.id" | "id" => self.node.id.clone(),
            "node.region" | "region" => self.node.region.clone(),
            "http-bind" | "http_bind" | "node.http_bind" => self.node.http_bind.clone(),
            "wire-bind" | "wire_bind" | "node.wire_bind" => self.node.wire_bind.clone(),
            "wire-enabled" | "wire_enabled" => self.node.wire_enabled.to_string(),
            "api-key" | "api_key" | "auth.api_key" => self.node.api_key.clone(),
            "data-dir" | "data_dir" | "node.data_dir" => self.node.data_dir.clone(),
            "log-level" | "log_level" => self.node.log_level.clone(),
            "tls-enabled" | "tls_enabled" => self.node.tls_enabled.to_string(),
            "tls-cert" | "tls_cert" => self.node.tls_cert.clone(),
            "tls-key" | "tls_key" => self.node.tls_key.clone(),
            "replication.enabled" | "replicate" => self.replication.enabled.to_string(),
            "replication.role" => self.replication.role.clone(),
            "replication.grpc_bind" | "grpc-bind" => self.replication.grpc_bind.clone(),
            "replication.leader_addr" | "leader-addr" => self.replication.leader_addr.clone(),
            "replication.peers" | "peers" => self.replication.peers.join(","),
            "replication.write_mode" | "write-mode" => self.replication.write_mode.clone(),
            "storage.backend" | "storage" => self.storage.backend.clone(),
            "storage.remote.url" | "remote-url" | "s3-url" => {
                self.storage.remote_endpoint_url.clone()
            }
            "storage.remote.region" | "remote-region" | "s3-region" => {
                self.storage.remote_region.clone()
            }
            "storage.remote.bucket" | "remote-bucket" | "s3-bucket" => {
                self.storage.remote_bucket.clone()
            }
            "storage.remote.access_key_id" | "remote-access-key" | "s3-access-key" => {
                self.storage.remote_access_key_id.clone()
            }
            "storage.remote.secret_access_key" | "remote-secret-key" | "s3-secret-key" => {
                self.storage.remote_secret_access_key.clone()
            }
            "storage.remote.prefix" | "remote-prefix" | "s3-prefix" => {
                self.storage.remote_prefix.clone()
            }
            _ => return None,
        })
    }

    /// All settable keys with their current values, for `config list` / the UI.
    pub fn entries(&self) -> Vec<(&'static str, String)> {
        [
            "node.id",
            "node.region",
            "http-bind",
            "wire-bind",
            "wire-enabled",
            "api-key",
            "data-dir",
            "log-level",
            "tls-enabled",
            "tls-cert",
            "tls-key",
            "replication.enabled",
            "replication.role",
            "replication.grpc_bind",
            "replication.leader_addr",
            "replication.peers",
            "replication.write_mode",
            "storage.backend",
            "storage.remote.url",
            "storage.remote.region",
            "storage.remote.bucket",
            "storage.remote.access_key_id",
            "storage.remote.secret_access_key",
            "storage.remote.prefix",
        ]
        .into_iter()
        .map(|k| (k, self.get(k).unwrap_or_default()))
        .collect()
    }

    /// Materialise the runtime [`Config`] this profile describes. Each active
    /// feature contributes its own keyspaces/topics/queues/streams so a
    /// cache-only node builds only the tiered cache keyspace, a pubsub node
    /// builds only topics, and so on. Multi-feature (`full`) profiles compose.
    pub fn to_config(&self) -> Config {
        let mut cfg = Config::default();
        cfg.node.id = self.node.id.clone();
        cfg.node.region = self.node.region.clone();
        cfg.http.bind = self.node.http_bind.clone();
        cfg.wire.enabled = self.node.wire_enabled;
        cfg.wire.bind = self.node.wire_bind.clone();
        cfg.auth.api_key = self.node.api_key.clone();
        cfg.tls = crate::config::TlsConfig {
            enabled: self.node.tls_enabled,
            cert_file: self.node.tls_cert.clone(),
            key_file: self.node.tls_key.clone(),
        };
        cfg.storage.data_dir = self.node.data_dir.clone();

        // Storage backend for the sharded tier: local dir or third-party remote.
        cfg.storage.backend = if self.storage.backend == "remote" || self.storage.backend == "s3" {
            crate::config::StorageBackend::Remote(crate::config::RemoteBackendConfig {
                endpoint_url: self.storage.remote_endpoint_url.clone(),
                region: self.storage.remote_region.clone(),
                bucket: self.storage.remote_bucket.clone(),
                access_key_id: self.storage.remote_access_key_id.clone(),
                secret_access_key: self.storage.remote_secret_access_key.clone(),
                prefix: self.storage.remote_prefix.clone(),
            })
        } else {
            crate::config::StorageBackend::Local
        };

        cfg.replication.enabled = self.replication.enabled;
        cfg.replication.role = if self.replication.role == "follower" {
            crate::config::ReplicationRole::Follower
        } else {
            crate::config::ReplicationRole::Leader
        };
        cfg.replication.grpc_bind = self.replication.grpc_bind.clone();
        cfg.replication.leader_addr = if self.replication.leader_addr.is_empty() {
            None
        } else {
            Some(self.replication.leader_addr.clone())
        };
        cfg.replication.peers = self
            .replication
            .peers
            .iter()
            .enumerate()
            .map(|(i, addr)| crate::config::PeerConfig {
                node_id: format!("peer-{i}"),
                addr: addr.clone(),
            })
            .collect();

        // Each feature owns its slice of the runtime config.
        cfg.keyspaces = Vec::new();
        cfg.topics = Vec::new();
        cfg.queues = Vec::new();
        cfg.streams = Vec::new();
        let replicated = self.replication.enabled;
        // With remote storage attached, KV-ish keyspaces use the sharded
        // object-store tier; on local storage they use their fast local tiers.
        let on_remote = self.storage.backend == "remote" || self.storage.backend == "s3";
        // Write mode applies to a warm, replicated KV keyspace (multi-leader and
        // primary-queue both require warm + replication).
        let write_mode = match self.replication.write_mode.as_str() {
            "multi-leader" => crate::config::WriteMode::MultiLeader,
            "primary-queue" => crate::config::WriteMode::PrimaryQueue,
            _ => crate::config::WriteMode::SingleLeader,
        };
        let kv_write_mode = if replicated && !on_remote {
            write_mode
        } else {
            crate::config::WriteMode::SingleLeader
        };
        for f in self.features.iter() {
            match f {
                Feature::Cache => cfg.keyspaces.push(KeyspaceConfig {
                    name: "cache".into(),
                    tier: if on_remote { TierName::Sharded } else { TierName::Tiered },
                    replication: replicated && !on_remote,
                    // Own subdirectory so a cache co-located with other products
                    // never shares a storage directory with them.
                    storage_subdir: Feature::Cache.as_str().into(),
                    ..KeyspaceConfig::default_keyspace()
                }),
                Feature::Kv => cfg.keyspaces.push(KeyspaceConfig {
                    name: "default".into(),
                    tier: if on_remote { TierName::Sharded } else { TierName::Warm },
                    subscriptions: true,
                    replication: replicated && !on_remote,
                    write_mode: kv_write_mode,
                    storage_subdir: Feature::Kv.as_str().into(),
                    ..KeyspaceConfig::default_keyspace()
                }),
                Feature::Pubsub => cfg.topics.push(TopicConfig {
                    name: "events".into(),
                    mode: crate::config::TopicModeConfig::Ephemeral,
                    capacity: 1024,
                }),
                Feature::Queue => cfg.queues.push(QueueConfig {
                    name: "jobs".into(),
                    ack_timeout_secs: 30,
                }),
                Feature::Stream => cfg.streams.push(StreamConfig {
                    name: "events".into(),
                    partitions: 1,
                    capacity: 1024,
                    interval_fsync_ms: 0,
                }),
            }
        }
        // A node with no KV-ish feature still needs a keyspace for the store to
        // build cleanly; give pubsub/queue/stream-only nodes a tiny hot one.
        if cfg.keyspaces.is_empty() {
            cfg.keyspaces.push(KeyspaceConfig {
                name: "default".into(),
                tier: TierName::Hot,
                ..KeyspaceConfig::default_keyspace()
            });
        }
        cfg
    }
}

fn parse_bool(s: &str) -> Result<bool, ()> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_roundtrip() {
        let mut p = Profile::default();
        p.set("region", "us-east-1").unwrap();
        p.set("http-bind", "0.0.0.0:9000").unwrap();
        p.set("replicate", "true").unwrap();
        p.set("peers", "a:7070, b:7070").unwrap();
        assert_eq!(p.get("region").unwrap(), "us-east-1");
        assert_eq!(p.get("http-bind").unwrap(), "0.0.0.0:9000");
        assert_eq!(p.get("replication.enabled").unwrap(), "true");
        assert_eq!(p.replication.peers, vec!["a:7070", "b:7070"]);
    }

    #[test]
    fn unknown_key_errors() {
        let mut p = Profile::default();
        assert!(matches!(
            p.set("nope", "x"),
            Err(ProfileError::UnknownKey(_))
        ));
    }

    #[test]
    fn each_product_keyspace_gets_its_own_storage_subdir() {
        // Cache and KV co-located on one node must land in DIFFERENT storage
        // subdirectories so they never share a directory on the container.
        let mut p = Profile::default();
        p.features.insert(Feature::Cache);
        p.features.insert(Feature::Kv);
        let cfg = p.to_config();
        let cache = cfg.keyspaces.iter().find(|k| k.name == "cache").unwrap();
        let kv = cfg.keyspaces.iter().find(|k| k.name == "default").unwrap();
        assert_eq!(cache.storage_subdir, "cache");
        assert_eq!(kv.storage_subdir, "kv");
        assert_ne!(cache.storage_subdir, kv.storage_subdir);
    }

    #[test]
    fn cache_profile_builds_tiered_keyspace() {
        let mut p = Profile::default();
        p.features.insert(Feature::Cache);
        let cfg = p.to_config();
        assert_eq!(cfg.keyspaces.len(), 1);
        assert_eq!(cfg.keyspaces[0].name, "cache");
        assert_eq!(cfg.keyspaces[0].tier, TierName::Tiered);
        assert!(cfg.topics.is_empty() && cfg.queues.is_empty());
    }

    #[test]
    fn pubsub_only_gets_placeholder_keyspace_and_topic() {
        let mut p = Profile::default();
        p.features.insert(Feature::Pubsub);
        let cfg = p.to_config();
        assert_eq!(cfg.topics.len(), 1);
        assert_eq!(cfg.keyspaces.len(), 1);
        assert_eq!(cfg.keyspaces[0].tier, TierName::Hot);
    }

    #[test]
    fn remote_backend_roundtrips_and_switches_tier_to_sharded() {
        let mut p = Profile::default();
        p.features.insert(Feature::Cache);
        p.set("storage.backend", "remote").unwrap();
        p.set("remote-url", "https://store.example.com").unwrap();
        p.set("remote-bucket", "my-bucket").unwrap();
        p.set("remote-access-key", "KEYID").unwrap();
        p.set("remote-secret-key", "sekret").unwrap();
        assert_eq!(p.get("storage.backend").unwrap(), "remote");
        assert_eq!(p.get("remote-bucket").unwrap(), "my-bucket");

        let cfg = p.to_config();
        // With remote storage attached, KV keyspaces use the object-store tier.
        assert_eq!(cfg.keyspaces[0].tier, TierName::Sharded);
        match cfg.storage.backend {
            crate::config::StorageBackend::Remote(r) => {
                assert_eq!(r.bucket, "my-bucket");
                assert_eq!(r.endpoint_url, "https://store.example.com");
            }
            _ => panic!("expected remote backend"),
        }
    }

    #[test]
    fn s3_keys_are_accepted_as_legacy_aliases() {
        let mut p = Profile::default();
        p.set("storage.backend", "s3").unwrap(); // legacy -> normalized to remote
        p.set("s3-bucket", "b").unwrap();
        assert_eq!(p.get("storage.backend").unwrap(), "remote");
        assert_eq!(p.get("remote-bucket").unwrap(), "b");
    }

    #[test]
    fn storage_backend_rejects_unknown_kind() {
        let mut p = Profile::default();
        assert!(p.set("storage.backend", "gcs").is_err());
    }

    #[test]
    fn primary_queue_write_mode_plumbs_to_kv_keyspace() {
        let mut p = Profile::default();
        p.features.insert(Feature::Kv);
        p.set("replicate", "true").unwrap();
        p.set("write-mode", "primary-queue").unwrap();
        let cfg = p.to_config();
        let kv = cfg.keyspaces.iter().find(|k| k.name == "default").unwrap();
        assert_eq!(kv.write_mode, crate::config::WriteMode::PrimaryQueue);
        assert!(kv.replication);
    }

    #[test]
    fn write_mode_rejects_unknown() {
        let mut p = Profile::default();
        assert!(p.set("write-mode", "quorum").is_err());
    }

    #[test]
    fn tls_config_plumbs_through() {
        let mut p = Profile::default();
        p.set("tls-enabled", "true").unwrap();
        p.set("tls-cert", "/etc/falcon/cert.pem").unwrap();
        p.set("tls-key", "/etc/falcon/key.pem").unwrap();
        let cfg = p.to_config();
        assert!(cfg.tls.is_enabled());
        assert_eq!(cfg.tls.cert_file, "/etc/falcon/cert.pem");
    }

    #[test]
    fn peers_and_replication_roundtrip_to_config() {
        let mut p = Profile::default();
        p.features.insert(Feature::Kv);
        p.set("replicate", "true").unwrap();
        p.set("role", "leader").ok(); // role alias may not exist; ignore
        p.set("replication.role", "leader").unwrap();
        p.set("peers", "10.0.0.2:7070,10.0.0.3:7070").unwrap();
        let cfg = p.to_config();
        assert!(cfg.replication.enabled);
        assert_eq!(cfg.replication.peers.len(), 2);
        assert_eq!(cfg.replication.peers[0].addr, "10.0.0.2:7070");
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("falcon-prof-{}", std::process::id()));
        let path = dir.join("profile.toml");
        let mut p = Profile::default();
        p.features.insert(Feature::Cache);
        p.set("region", "eu-west-1").unwrap();
        p.save(&path).unwrap();
        let loaded = Profile::load(&path).unwrap();
        assert!(loaded.features.contains(Feature::Cache));
        assert_eq!(loaded.node.region, "eu-west-1");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
