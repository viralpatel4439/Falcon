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
}

impl Default for ProfileReplication {
    fn default() -> Self {
        Self {
            enabled: false,
            role: default_role(),
            grpc_bind: default_grpc_bind(),
            leader_addr: String::new(),
            peers: Vec::new(),
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
/// (default) uses the node's `data_dir`; `s3` attaches any S3-compatible
/// third-party store by URL + credentials.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileStorage {
    /// `local` or `s3`.
    #[serde(default = "default_backend")]
    pub backend: String,
    #[serde(default)]
    pub s3_endpoint_url: String,
    #[serde(default = "default_s3_region")]
    pub s3_region: String,
    #[serde(default)]
    pub s3_bucket: String,
    #[serde(default)]
    pub s3_access_key_id: String,
    #[serde(default)]
    pub s3_secret_access_key: String,
    #[serde(default)]
    pub s3_prefix: String,
}

impl Default for ProfileStorage {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            s3_endpoint_url: String::new(),
            s3_region: default_s3_region(),
            s3_bucket: String::new(),
            s3_access_key_id: String::new(),
            s3_secret_access_key: String::new(),
            s3_prefix: String::new(),
        }
    }
}

fn default_backend() -> String {
    "local".into()
}
fn default_s3_region() -> String {
    "us-east-1".into()
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
            "storage.backend" | "storage" => {
                if value != "local" && value != "s3" {
                    return Err(bad("expected 'local' or 's3'"));
                }
                self.storage.backend = value.to_string();
            }
            "storage.s3.url" | "s3-url" => self.storage.s3_endpoint_url = value.to_string(),
            "storage.s3.region" | "s3-region" => self.storage.s3_region = value.to_string(),
            "storage.s3.bucket" | "s3-bucket" => self.storage.s3_bucket = value.to_string(),
            "storage.s3.access_key_id" | "s3-access-key" => {
                self.storage.s3_access_key_id = value.to_string()
            }
            "storage.s3.secret_access_key" | "s3-secret-key" => {
                self.storage.s3_secret_access_key = value.to_string()
            }
            "storage.s3.prefix" | "s3-prefix" => self.storage.s3_prefix = value.to_string(),
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
            "replication.enabled" | "replicate" => self.replication.enabled.to_string(),
            "replication.role" => self.replication.role.clone(),
            "replication.grpc_bind" | "grpc-bind" => self.replication.grpc_bind.clone(),
            "replication.leader_addr" | "leader-addr" => self.replication.leader_addr.clone(),
            "replication.peers" | "peers" => self.replication.peers.join(","),
            "storage.backend" | "storage" => self.storage.backend.clone(),
            "storage.s3.url" | "s3-url" => self.storage.s3_endpoint_url.clone(),
            "storage.s3.region" | "s3-region" => self.storage.s3_region.clone(),
            "storage.s3.bucket" | "s3-bucket" => self.storage.s3_bucket.clone(),
            "storage.s3.access_key_id" | "s3-access-key" => self.storage.s3_access_key_id.clone(),
            "storage.s3.secret_access_key" | "s3-secret-key" => {
                self.storage.s3_secret_access_key.clone()
            }
            "storage.s3.prefix" | "s3-prefix" => self.storage.s3_prefix.clone(),
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
            "replication.enabled",
            "replication.role",
            "replication.grpc_bind",
            "replication.leader_addr",
            "replication.peers",
            "storage.backend",
            "storage.s3.url",
            "storage.s3.region",
            "storage.s3.bucket",
            "storage.s3.access_key_id",
            "storage.s3.secret_access_key",
            "storage.s3.prefix",
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
        cfg.storage.data_dir = self.node.data_dir.clone();

        // Storage backend for the sharded tier: local dir or S3-compatible.
        cfg.storage.backend = if self.storage.backend == "s3" {
            crate::config::StorageBackend::S3(crate::config::S3BackendConfig {
                endpoint_url: self.storage.s3_endpoint_url.clone(),
                region: self.storage.s3_region.clone(),
                bucket: self.storage.s3_bucket.clone(),
                access_key_id: self.storage.s3_access_key_id.clone(),
                secret_access_key: self.storage.s3_secret_access_key.clone(),
                prefix: self.storage.s3_prefix.clone(),
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
        // With S3 attached, KV-ish keyspaces use the sharded object-store tier
        // (the tier S3 backs); on local storage they use their fast local tiers.
        let on_s3 = self.storage.backend == "s3";
        for f in self.features.iter() {
            match f {
                Feature::Cache => cfg.keyspaces.push(KeyspaceConfig {
                    name: "cache".into(),
                    tier: if on_s3 { TierName::Sharded } else { TierName::Tiered },
                    replication: replicated && !on_s3,
                    ..KeyspaceConfig::default_keyspace()
                }),
                Feature::Kv => cfg.keyspaces.push(KeyspaceConfig {
                    name: "default".into(),
                    tier: if on_s3 { TierName::Sharded } else { TierName::Warm },
                    subscriptions: true,
                    replication: replicated && !on_s3,
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
    fn s3_backend_roundtrips_and_switches_tier_to_sharded() {
        let mut p = Profile::default();
        p.features.insert(Feature::Cache);
        p.set("storage.backend", "s3").unwrap();
        p.set("s3-url", "https://s3.example.com").unwrap();
        p.set("s3-bucket", "my-bucket").unwrap();
        p.set("s3-access-key", "AKIA").unwrap();
        p.set("s3-secret-key", "sekret").unwrap();
        assert_eq!(p.get("storage.backend").unwrap(), "s3");
        assert_eq!(p.get("s3-bucket").unwrap(), "my-bucket");

        let cfg = p.to_config();
        // With S3 attached, the cache keyspace uses the object-store (sharded) tier.
        assert_eq!(cfg.keyspaces[0].tier, TierName::Sharded);
        match cfg.storage.backend {
            crate::config::StorageBackend::S3(s3) => {
                assert_eq!(s3.bucket, "my-bucket");
                assert_eq!(s3.endpoint_url, "https://s3.example.com");
            }
            _ => panic!("expected S3 backend"),
        }
    }

    #[test]
    fn storage_backend_rejects_unknown_kind() {
        let mut p = Profile::default();
        assert!(p.set("storage.backend", "gcs").is_err());
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
