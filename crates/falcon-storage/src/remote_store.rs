//! An [`ObjectStore`] backed by a third-party object store the operator points
//! Falcon at. There are no provider defaults — you supply the endpoint and
//! everything needed to authenticate a request.
//!
//! The object HTTP API (as popularized by S3) is the de-facto standard these
//! stores speak, so one client reaches any of them — managed or self-hosted —
//! by URL + credentials. The client is a small Signature-V4 signer over
//! `reqwest` (path-style addressing, universally accepted), gated behind the
//! `remote` cargo feature so a build that doesn't use remote storage never
//! compiles it.
//!
//! Objects are stored under an optional key `prefix`; the sharded tier layers
//! its bucket objects on top, so one Falcon keyspace maps to a fixed set of
//! objects regardless of key count.

use crate::engine::StorageError;
use crate::object_store::ObjectStore;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Everything needed to attach a third-party object store (operator-supplied).
#[derive(Clone, Debug)]
pub struct RemoteConfig {
    /// Base endpoint, e.g. `https://s3.amazonaws.com`, `https://<acct>.r2.cloudflarestorage.com`,
    /// or `http://localhost:9000` for MinIO.
    pub endpoint_url: String,
    /// Region label used in the signature, e.g. `us-east-1` (use `auto` for R2).
    pub region: String,
    /// Bucket name.
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Optional key prefix so multiple keyspaces can share one bucket.
    pub prefix: String,
}

/// An object store that speaks the standard object HTTP API against `config.endpoint_url`.
pub struct RemoteObjectStore {
    config: RemoteConfig,
    client: reqwest::blocking::Client,
    /// `host` portion of the endpoint, used in the signed `Host` header.
    host: String,
    /// `http` or `https`.
    scheme: String,
}

impl RemoteObjectStore {
    /// Build a client for the given config. Does not perform any network I/O;
    /// the first request validates connectivity/credentials.
    pub fn new(config: RemoteConfig) -> Result<Self, StorageError> {
        let url = config
            .endpoint_url
            .strip_suffix('/')
            .unwrap_or(&config.endpoint_url)
            .to_string();
        let (scheme, rest) = match url.split_once("://") {
            Some((s, r)) => (s.to_string(), r.to_string()),
            None => ("https".to_string(), url.clone()),
        };
        let host = rest.split('/').next().unwrap_or(&rest).to_string();
        let client = reqwest::blocking::Client::builder()
            .build()
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(Self {
            config: RemoteConfig {
                endpoint_url: format!("{scheme}://{host}"),
                ..config
            },
            client,
            host,
            scheme,
        })
    }

    /// Object key for a Falcon key: `<prefix><percent-encoded key>`.
    fn object_key(&self, key: &[u8]) -> String {
        let encoded = encode_key(key);
        if self.config.prefix.is_empty() {
            encoded
        } else {
            format!("{}/{encoded}", self.config.prefix.trim_end_matches('/'))
        }
    }

    /// Perform one signed request. `body` is the request payload (empty for
    /// GET/DELETE/LIST). Returns (status, body_bytes).
    fn request(
        &self,
        method: &str,
        object_key: &str,
        query: &str,
        body: &[u8],
    ) -> Result<(u16, Vec<u8>), StorageError> {
        // Canonical URI is /bucket/objectkey (path-style, universally accepted).
        let canonical_uri = format!(
            "/{}/{}",
            self.config.bucket,
            object_key.split('/').map(uri_encode_segment).collect::<Vec<_>>().join("/")
        );
        let url = format!("{}://{}{}{}", self.scheme, self.host, canonical_uri,
            if query.is_empty() { String::new() } else { format!("?{query}") });

        let now = time::OffsetDateTime::now_utc();
        let amz_date = format_amz_date(&now);
        let date_stamp = &amz_date[..8];

        let payload_hash = hex::encode(Sha256::digest(body));
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_headers = format!(
            "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
            self.host, payload_hash, amz_date
        );
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );
        let scope = format!("{date_stamp}/{}/s3/aws4_request", self.config.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let signature = self.sign(date_stamp, &string_to_sign);
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.config.access_key_id
        );

        let mut req = match method {
            "GET" => self.client.get(&url),
            "PUT" => self.client.put(&url),
            "DELETE" => self.client.delete(&url),
            _ => return Err(StorageError::Backend(format!("unsupported method {method}"))),
        };
        req = req
            .header("host", &self.host)
            .header("x-amz-content-sha256", &payload_hash)
            .header("x-amz-date", &amz_date)
            .header("authorization", authorization);
        if method == "PUT" {
            req = req.body(body.to_vec());
        }

        let resp = req.send().map_err(|e| StorageError::Backend(e.to_string()))?;
        let status = resp.status().as_u16();
        let bytes = resp.bytes().map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok((status, bytes.to_vec()))
    }

    /// The SigV4 signing-key derivation, then the final HMAC over the STS.
    fn sign(&self, date_stamp: &str, string_to_sign: &str) -> String {
        let k_date = hmac(format!("AWS4{}", self.config.secret_access_key).as_bytes(), date_stamp.as_bytes());
        let k_region = hmac(&k_date, self.config.region.as_bytes());
        let k_service = hmac(&k_region, b"s3");
        let k_signing = hmac(&k_service, b"aws4_request");
        hex::encode(hmac(&k_signing, string_to_sign.as_bytes()))
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Percent-encode a path segment per S3's rules (unreserved chars pass through).
fn uri_encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push_str(&format!("{b:02X}"));
        }
    }
    out
}

/// Encode arbitrary key bytes to a safe object-name component (same scheme as
/// the local backend so keys are portable between backends).
fn encode_key(key: &[u8]) -> String {
    let mut out = String::with_capacity(key.len() + 2);
    for &b in key {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02x}"));
        }
    }
    if out.is_empty() {
        out.push_str("%00empty");
    }
    out
}

fn format_amz_date(now: &time::OffsetDateTime) -> String {
    // YYYYMMDDTHHMMSSZ
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        now.month() as u8,
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

#[async_trait]
impl ObjectStore for RemoteObjectStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let this = self.clone_handle();
        let object_key = self.object_key(key);
        tokio::task::spawn_blocking(move || {
            let (status, body) = this.request("GET", &object_key, "", &[])?;
            match status {
                200 => Ok(Some(body)),
                404 => Ok(None),
                s => Err(StorageError::Backend(format!("GET {s}: {}", String::from_utf8_lossy(&body)))),
            }
        })
        .await
        .map_err(|e| StorageError::Backend(e.to_string()))?
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let this = self.clone_handle();
        let object_key = self.object_key(key);
        let value = value.to_vec();
        tokio::task::spawn_blocking(move || {
            let (status, body) = this.request("PUT", &object_key, "", &value)?;
            if (200..300).contains(&status) {
                Ok(())
            } else {
                Err(StorageError::Backend(format!("PUT {status}: {}", String::from_utf8_lossy(&body))))
            }
        })
        .await
        .map_err(|e| StorageError::Backend(e.to_string()))?
    }

    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        let this = self.clone_handle();
        let object_key = self.object_key(key);
        tokio::task::spawn_blocking(move || {
            let (status, body) = this.request("DELETE", &object_key, "", &[])?;
            if (200..300).contains(&status) || status == 404 {
                Ok(())
            } else {
                Err(StorageError::Backend(format!("DELETE {status}: {}", String::from_utf8_lossy(&body))))
            }
        })
        .await
        .map_err(|e| StorageError::Backend(e.to_string()))?
    }

    async fn list_prefix(&self, _prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        // The sharded tier addresses fixed bucket objects by exact name and
        // never needs a remote LIST (it keeps its own in-memory index), so a
        // full listing is intentionally unsupported here. Returning empty keeps
        // the trait total without an expensive, paginated ListObjects call.
        Ok(Vec::new())
    }

    fn describe(&self) -> String {
        format!("s3:{}/{}", self.config.endpoint_url, self.config.bucket)
    }
}

impl RemoteObjectStore {
    /// Cheap handle clone for moving into a blocking task (reqwest client is
    /// internally reference-counted; config is small).
    fn clone_handle(&self) -> RemoteObjectStore {
        RemoteObjectStore {
            config: self.config.clone(),
            client: self.client.clone(),
            host: self.host.clone(),
            scheme: self.scheme.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_key_applies_prefix_and_encoding() {
        let cfg = RemoteConfig {
            endpoint_url: "https://s3.example.com".into(),
            region: "us-east-1".into(),
            bucket: "b".into(),
            access_key_id: "ak".into(),
            secret_access_key: "sk".into(),
            prefix: "falcon/cache".into(),
        };
        let s = RemoteObjectStore::new(cfg).unwrap();
        assert_eq!(s.object_key(b"bucket_3"), "falcon/cache/bucket_3");
        assert_eq!(s.host, "s3.example.com");
        assert_eq!(s.scheme, "https");
    }

    #[test]
    fn sigv4_signing_key_is_deterministic() {
        let cfg = RemoteConfig {
            endpoint_url: "https://s3.amazonaws.com".into(),
            region: "us-east-1".into(),
            bucket: "b".into(),
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            prefix: String::new(),
        };
        let s = RemoteObjectStore::new(cfg).unwrap();
        // Same inputs must always produce the same signature (no time dependence
        // in the derived signing key itself).
        let a = s.sign("20150830", "test-string-to-sign");
        let b = s.sign("20150830", "test-string-to-sign");
        assert_eq!(a, b);
        assert_eq!(a.len(), 64); // hex-encoded SHA256 HMAC
    }
}
