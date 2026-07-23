//! `install` / `uninstall` / `status` / `config` — the profile-management
//! commands. These are the ONLY way (besides the web UI) to configure Falcon;
//! there is no environment-variable path.

use crate::cli::{ConfigCmd, InstallArgs, PeersCmd, UninstallArgs};
use crate::features;
use anyhow::{bail, Context, Result};
use falcon_core::{Feature, Profile};
use std::path::PathBuf;
use std::str::FromStr;

fn profile_path(explicit: &Option<String>) -> PathBuf {
    explicit
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(falcon_core::default_profile_path)
}

/// Ensure the requested product is actually compiled into this binary. A slim
/// `feat-cache` build cannot install `pubsub`, since that code isn't present.
fn ensure_compiled(feature: Feature) -> Result<()> {
    let compiled = features::compiled();
    if !compiled.contains(feature) {
        bail!(
            "this build does not include the '{}' product (compiled: {}).\n\
             Install a build that includes it, or use the full build:\n  \
             cargo build --release   # full build with every product",
            feature,
            compiled
        );
    }
    Ok(())
}

pub fn install(profile_flag: &Option<String>, args: InstallArgs) -> Result<()> {
    let feature = Feature::from_str(&args.feature)?;
    ensure_compiled(feature)?;

    let path = profile_path(profile_flag);
    let mut profile = Profile::load_or_default(&path)?;

    let is_new = profile.features.insert(feature);

    // Apply the one-shot settings the user passed to install.
    if let Some(v) = args.region {
        profile.set("region", &v)?;
    }
    if let Some(v) = args.http_bind {
        profile.set("http-bind", &v)?;
    }
    if let Some(v) = args.node_id {
        profile.set("node.id", &v)?;
    }
    if let Some(v) = args.data_dir {
        profile.set("data-dir", &v)?;
    }
    if let Some(v) = args.api_key {
        profile.set("api-key", &v)?;
    }
    if args.replicate {
        profile.set("replicate", "true")?;
    }
    if let Some(v) = args.role {
        profile.set("replication.role", &v)?;
    }
    if let Some(v) = args.leader_addr {
        profile.set("leader-addr", &v)?;
    }
    if !args.peers.is_empty() {
        profile.set("peers", &args.peers.join(","))?;
    }
    if let Some(v) = args.storage {
        profile.set("storage.backend", &v)?;
    }
    if let Some(v) = args.s3_url {
        profile.set("s3-url", &v)?;
    }
    if let Some(v) = args.s3_region {
        profile.set("s3-region", &v)?;
    }
    if let Some(v) = args.s3_bucket {
        profile.set("s3-bucket", &v)?;
    }
    if let Some(v) = args.s3_access_key {
        profile.set("s3-access-key", &v)?;
    }
    if let Some(v) = args.s3_secret_key {
        profile.set("s3-secret-key", &v)?;
    }

    profile.save(&path).with_context(|| format!("writing {}", path.display()))?;

    println!(
        "{} {} at {}",
        if is_new { "Installed" } else { "Updated" },
        feature.product_name(),
        path.display()
    );
    println!("  {}", feature.tagline());
    if profile.replication.enabled {
        println!(
            "  replication: {} ({}){}",
            profile.replication.role,
            if profile.replication.peers.is_empty() {
                "no peers yet".to_string()
            } else {
                format!("{} peer(s)", profile.replication.peers.len())
            },
            if profile.replication.role == "follower" && profile.replication.leader_addr.is_empty() {
                "  ⚠ set a leader: falcon config set leader-addr <url>"
            } else {
                ""
            }
        );
    }
    println!();
    println!("Next:");
    println!("  falcon serve                       # run this node");
    println!(
        "  open http://{}/                    # the {} UI",
        display_host(&profile.node.http_bind),
        feature.product_name()
    );
    Ok(())
}

pub fn uninstall(profile_flag: &Option<String>, args: UninstallArgs) -> Result<()> {
    let feature = Feature::from_str(&args.feature)?;
    let path = profile_path(profile_flag);
    let mut profile = Profile::load(&path)?;
    if profile.features.remove(feature) {
        profile.save(&path)?;
        println!("Uninstalled {} from {}", feature.product_name(), path.display());
    } else {
        println!("{} was not installed", feature.product_name());
    }
    Ok(())
}

pub fn status(profile_flag: &Option<String>) -> Result<()> {
    println!("Build: {}", features::build_label());
    println!("  compiled products: {}", features::compiled());
    println!();

    let path = profile_path(profile_flag);
    match Profile::load(&path) {
        Ok(profile) => {
            println!("Profile: {}", path.display());
            if profile.features.is_empty() {
                println!("  (no products installed — run `falcon install <feature>`)");
            } else {
                println!("  installed products:");
                for f in profile.features.iter() {
                    println!("    • {} — {}", f.product_name(), f.tagline());
                }
            }
            println!("  node:   id={} region={}", profile.node.id, profile.node.region);
            println!("  http:   {}", profile.node.http_bind);
            println!(
                "  auth:   {}",
                if profile.node.api_key.is_empty() { "off" } else { "on" }
            );
            println!(
                "  repl:   {}",
                if profile.replication.enabled {
                    format!(
                        "on ({}, {} peer(s))",
                        profile.replication.role,
                        profile.replication.peers.len()
                    )
                } else {
                    "off".to_string()
                }
            );
        }
        Err(falcon_core::ProfileError::NotFound(_)) => {
            println!("Profile: none yet at {}", path.display());
            println!("  run `falcon install <feature>` to create one");
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

pub fn config(profile_flag: &Option<String>, cmd: ConfigCmd) -> Result<()> {
    let path = profile_path(profile_flag);
    match cmd {
        ConfigCmd::Set { key, value } => {
            let mut profile = Profile::load_or_default(&path)?;
            profile.set(&key, &value)?;
            profile.save(&path)?;
            println!("set {key} = {value}");
        }
        ConfigCmd::Get { key } => {
            let profile = Profile::load(&path)?;
            match profile.get(&key) {
                Some(v) => println!("{v}"),
                None => bail!("unknown config key '{key}'"),
            }
        }
        ConfigCmd::List => {
            let profile = Profile::load_or_default(&path)?;
            for (k, v) in profile.entries() {
                println!("{k} = {v}");
            }
        }
    }
    Ok(())
}

pub fn peers(profile_flag: &Option<String>, cmd: PeersCmd) -> Result<()> {
    let path = profile_path(profile_flag);
    match cmd {
        PeersCmd::Add { addr } => {
            let mut profile = Profile::load_or_default(&path)?;
            if profile.replication.peers.iter().any(|p| p == &addr) {
                println!("peer {addr} already present");
                return Ok(());
            }
            profile.replication.peers.push(addr.clone());
            // Adding a peer implies you want replication on.
            profile.replication.enabled = true;
            profile.save(&path)?;
            println!("added peer {addr} ({} total)", profile.replication.peers.len());
        }
        PeersCmd::Remove { addr } => {
            let mut profile = Profile::load(&path)?;
            let before = profile.replication.peers.len();
            profile.replication.peers.retain(|p| p != &addr);
            if profile.replication.peers.len() == before {
                println!("peer {addr} was not in the list");
            } else {
                profile.save(&path)?;
                println!("removed peer {addr}");
            }
        }
        PeersCmd::List => {
            let profile = Profile::load_or_default(&path)?;
            println!(
                "replication: {} (role: {}, grpc: {})",
                if profile.replication.enabled { "on" } else { "off" },
                profile.replication.role,
                profile.replication.grpc_bind
            );
            if !profile.replication.leader_addr.is_empty() {
                println!("leader: {}", profile.replication.leader_addr);
            }
            if profile.replication.peers.is_empty() {
                println!("peers: (none) — add one with `falcon peers add <host:7070>`");
            } else {
                println!("peers:");
                for p in &profile.replication.peers {
                    println!("  • {p}");
                }
            }
        }
    }
    Ok(())
}

/// Turn a bind like `0.0.0.0:8080` into a browsable host (`127.0.0.1:8080`).
fn display_host(bind: &str) -> String {
    match bind.strip_prefix("0.0.0.0") {
        Some(rest) => format!("127.0.0.1{rest}"),
        None => bind.to_string(),
    }
}
