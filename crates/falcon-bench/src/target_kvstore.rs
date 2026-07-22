use anyhow::{bail, Context, Result};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

pub struct KvStoreHandle {
    child: Child,
    pub base_url: String,
    pub wire_addr: String,
}

impl KvStoreHandle {
    /// Spawns the release `kvstored` binary against a fresh temp data dir,
    /// serving both HTTP (`port`) and the binary wire protocol (`port+1`),
    /// warm tier (the default), no replication, no subscriptions — the
    /// config a real user gets out of the box.
    pub async fn spawn(binary_path: &str, port: u16, data_dir: &std::path::Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let bind = format!("127.0.0.1:{port}");
        let wire_addr = format!("127.0.0.1:{}", port + 1);

        let child = Command::new(binary_path)
            .env("FALCON_DATA_DIR", data_dir)
            .env("FALCON_HTTP_BIND", &bind)
            .env("FALCON_WIRE_BIND", &wire_addr)
            .env("FALCON_LOG_LEVEL", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("failed to spawn falcon; build it first with `cargo build --release -p falcon-cli`")?;

        let base_url = format!("http://{bind}");
        wait_for_ready(&base_url).await?;

        Ok(Self {
            child,
            base_url,
            wire_addr,
        })
    }

    /// Spawn kvstored with the default keyspace on the warm tier using
    /// interval-fsync durability (ms). Used to demonstrate the write-tail
    /// improvement vs the default fsync-every-write.
    pub async fn spawn_interval_fsync(
        binary_path: &str,
        port: u16,
        data_dir: &std::path::Path,
        interval_ms: u64,
    ) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let bind = format!("127.0.0.1:{port}");
        let wire_addr = format!("127.0.0.1:{}", port + 1);
        let cfg_path = data_dir.join("bench-config.toml");
        std::fs::write(
            &cfg_path,
            format!(
                "[http]\nbind = \"{bind}\"\n[wire]\nbind = \"{wire_addr}\"\n[storage]\ndata_dir = \"{}\"\n[[keyspace]]\nname = \"default\"\ntier = \"warm\"\ninterval_fsync_ms = {interval_ms}\n",
                data_dir.display()
            ),
        )?;

        let child = Command::new(binary_path)
            .arg("--config")
            .arg(&cfg_path)
            .env("FALCON_LOG_LEVEL", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("failed to spawn kvstored")?;

        let base_url = format!("http://{bind}");
        wait_for_ready(&base_url).await?;
        Ok(Self {
            child,
            base_url,
            wire_addr,
        })
    }

    pub fn client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .pool_max_idle_per_host(256)
            .build()
            .expect("failed to build reqwest client")
    }
}

impl Drop for KvStoreHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn wait_for_ready(base_url: &str) -> Result<()> {
    let client = reqwest::Client::new();
    for _ in 0..50 {
        if let Ok(resp) = client.get(format!("{base_url}/healthz")).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("kvstored did not become ready at {base_url} in time")
}

pub async fn put(client: &reqwest::Client, base_url: &str, key: &str, value: &[u8]) {
    let resp = client
        .put(format!("{base_url}/kv/{key}"))
        .body(value.to_vec())
        .send()
        .await
        .expect("kvstored PUT request failed");
    assert!(resp.status().is_success(), "kvstored PUT returned {}", resp.status());
}

pub async fn get(client: &reqwest::Client, base_url: &str, key: &str) {
    let resp = client
        .get(format!("{base_url}/kv/{key}"))
        .send()
        .await
        .expect("kvstored GET request failed");
    // 404 is expected for a GET issued before that key has been PUT during
    // this run; only treat non-404 failures as a real error.
    assert!(
        resp.status().is_success() || resp.status().as_u16() == 404,
        "kvstored GET returned {}",
        resp.status()
    );
}
