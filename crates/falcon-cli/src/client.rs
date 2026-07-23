//! Client subcommands: talk to a running Falcon node over its HTTP API.
//! Synchronous (blocking reqwest) — these are one-shot CLI commands, so a full
//! async runtime would be overkill.

use crate::cli::{ClientArgs, KeyArgs, PutArgs, QueueCmd, ScanArgs, StreamCmd, TopicCmd};
use anyhow::{bail, Context, Result};
use std::io::Read;

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::new()
}

/// Attach the API key (if set) as a Bearer header.
fn auth(
    req: reqwest::blocking::RequestBuilder,
    c: &ClientArgs,
) -> reqwest::blocking::RequestBuilder {
    match &c.api_key {
        Some(k) => req.bearer_auth(k),
        None => req,
    }
}

/// Read a payload from an Option arg or, if None, from stdin.
fn payload_or_stdin(arg: Option<String>) -> Result<Vec<u8>> {
    match arg {
        Some(s) => Ok(s.into_bytes()),
        None => {
            let mut buf = Vec::new();
            std::io::stdin().read_to_end(&mut buf).context("reading stdin")?;
            Ok(buf)
        }
    }
}

pub fn get(a: KeyArgs) -> Result<()> {
    let url = format!("{}/keyspaces/{}/kv/{}", a.client.addr, a.keyspace, a.key);
    let resp = auth(client().get(&url), &a.client).send()?;
    if resp.status() == 404 {
        bail!("key not found");
    }
    let body: serde_json::Value = resp.error_for_status()?.json()?;
    println!("{}", body["value"].as_str().unwrap_or(""));
    Ok(())
}

pub fn put(a: PutArgs) -> Result<()> {
    let value = payload_or_stdin(a.value)?;
    let mut url = format!("{}/keyspaces/{}/kv/{}", a.client.addr, a.keyspace, a.key);
    if let Some(ttl) = a.ttl {
        url.push_str(&format!("?ttl={ttl}"));
    }
    let body: serde_json::Value = auth(client().put(&url).body(value), &a.client)
        .send()?
        .error_for_status()?
        .json()?;
    println!("OK (sequence {})", body["sequence"].as_u64().unwrap_or(0));
    Ok(())
}

pub fn del(a: KeyArgs) -> Result<()> {
    let url = format!("{}/keyspaces/{}/kv/{}", a.client.addr, a.keyspace, a.key);
    auth(client().delete(&url), &a.client).send()?.error_for_status()?;
    println!("OK");
    Ok(())
}

pub fn scan(a: ScanArgs) -> Result<()> {
    let url = format!(
        "{}/keyspaces/{}/kv?prefix={}",
        a.client.addr, a.keyspace, a.prefix
    );
    let body: serde_json::Value = auth(client().get(&url), &a.client)
        .send()?
        .error_for_status()?
        .json()?;
    if let Some(items) = body["items"].as_array() {
        for item in items {
            println!(
                "{}\t{}",
                item["key"].as_str().unwrap_or(""),
                item["value"].as_str().unwrap_or("")
            );
        }
    }
    Ok(())
}

pub fn topic(cmd: TopicCmd) -> Result<()> {
    match cmd {
        TopicCmd::Publish { topic, payload, client: c } => {
            let value = payload_or_stdin(payload)?;
            let url = format!("{}/topics/{}/publish", c.addr, topic);
            let body: serde_json::Value =
                auth(client().post(&url).body(value), &c).send()?.error_for_status()?.json()?;
            println!("OK (offset {})", body["offset"].as_u64().unwrap_or(0));
            Ok(())
        }
    }
}

pub fn queue(cmd: QueueCmd) -> Result<()> {
    match cmd {
        QueueCmd::Push { queue, payload, client: c } => {
            let value = payload_or_stdin(payload)?;
            let url = format!("{}/queues/{}/push", c.addr, queue);
            auth(client().post(&url).body(value), &c).send()?.error_for_status()?;
            println!("OK");
            Ok(())
        }
        QueueCmd::Pop { queue, group, client: c } => {
            let url = format!("{}/queues/{}/pop?group={}", c.addr, queue, group);
            let resp = auth(client().post(&url), &c).send()?;
            if resp.status() == 204 {
                println!("(empty)");
                return Ok(());
            }
            let body: serde_json::Value = resp.error_for_status()?.json()?;
            println!("{}", body["payload"].as_str().unwrap_or(""));
            Ok(())
        }
    }
}

pub fn stream(cmd: StreamCmd) -> Result<()> {
    match cmd {
        StreamCmd::Append { stream, payload, key, client: c } => {
            let value = payload_or_stdin(payload)?;
            let url = format!("{}/streams/{}/records?key={}", c.addr, stream, key);
            let body: serde_json::Value =
                auth(client().post(&url).body(value), &c).send()?.error_for_status()?.json()?;
            println!(
                "OK (partition {}, offset {})",
                body["partition"].as_u64().unwrap_or(0),
                body["offset"].as_u64().unwrap_or(0)
            );
            Ok(())
        }
        StreamCmd::Poll { stream, partition, group, client: c } => {
            let url = format!(
                "{}/streams/{}/poll?group={}&partition={}",
                c.addr, stream, group, partition
            );
            let body: serde_json::Value =
                auth(client().get(&url), &c).send()?.error_for_status()?.json()?;
            if let Some(records) = body["records"].as_array() {
                for r in records {
                    println!(
                        "offset {}\t{}",
                        r["offset"].as_u64().unwrap_or(0),
                        r["payload"].as_str().unwrap_or("")
                    );
                }
            }
            Ok(())
        }
    }
}

pub fn health(c: ClientArgs) -> Result<()> {
    let body: serde_json::Value =
        auth(client().get(format!("{}/healthz", c.addr)), &c).send()?.error_for_status()?.json()?;
    println!("{}", serde_json::to_string_pretty(&body)?);
    Ok(())
}

pub fn metrics(c: ClientArgs) -> Result<()> {
    let text = auth(client().get(format!("{}/metrics", c.addr)), &c)
        .send()?
        .error_for_status()?
        .text()?;
    print!("{text}");
    Ok(())
}
