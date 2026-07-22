//! A simple durable append-only message log, offset-addressed. Backs
//! durable topics and queues. Each appended message gets a monotonic
//! offset (its sequence number, starting at 1). Framing mirrors the KV
//! WAL: `[len:u32][offset:u64][ts:u128][payload]`, so a crash mid-append
//! truncates cleanly to the last whole record on replay.

use crate::error::MessagingError;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub type Offset = u64;

#[derive(Debug, Clone)]
pub struct LogMessage {
    pub offset: Offset,
    /// Append time (unix millis). Used for retention/age in a later phase.
    #[allow(dead_code)]
    pub timestamp: u128,
    pub payload: Vec<u8>,
}

pub struct MessageLog {
    file: Mutex<File>,
    path: PathBuf,
    next_offset: Mutex<Offset>,
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis()
}

impl MessageLog {
    pub fn open(path: &Path) -> Result<Self, MessagingError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Determine the current highest offset by replaying.
        let existing = Self::replay(path)?;
        let next = existing.last().map(|m| m.offset + 1).unwrap_or(1);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Self {
            file: Mutex::new(file),
            path: path.to_path_buf(),
            next_offset: Mutex::new(next),
        })
    }

    /// Append a message, fsync, and return its assigned offset.
    pub fn append(&self, payload: &[u8]) -> Result<Offset, MessagingError> {
        let mut next = self.next_offset.lock().expect("offset mutex poisoned");
        let offset = *next;
        let ts = now_millis();

        let mut body = Vec::with_capacity(8 + 16 + payload.len());
        body.extend_from_slice(&offset.to_be_bytes());
        body.extend_from_slice(&ts.to_be_bytes());
        body.extend_from_slice(payload);

        let mut framed = Vec::with_capacity(body.len() + 4);
        framed.extend_from_slice(&(body.len() as u32).to_be_bytes());
        framed.extend_from_slice(&body);

        let mut file = self.file.lock().expect("log file mutex poisoned");
        file.write_all(&framed)?;
        file.sync_data()?;
        *next = offset + 1;
        Ok(offset)
    }

    /// Read all messages with offset >= `from`, in order.
    pub fn read_from(&self, from: Offset) -> Result<Vec<LogMessage>, MessagingError> {
        Ok(Self::replay(&self.path)?
            .into_iter()
            .filter(|m| m.offset >= from)
            .collect())
    }

    pub fn next_offset(&self) -> Offset {
        *self.next_offset.lock().expect("offset mutex poisoned")
    }

    fn replay(path: &Path) -> Result<Vec<LogMessage>, MessagingError> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut out = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len];
            if reader.read_exact(&mut body).is_err() {
                break; // partial trailing record from a crash; stop
            }
            let (Ok(off_bytes), Ok(ts_bytes)) = (
                <[u8; 8]>::try_from(body.get(0..8).unwrap_or(&[])),
                <[u8; 16]>::try_from(body.get(8..24).unwrap_or(&[])),
            ) else {
                break; // truncated/garbage header; stop cleanly
            };
            let offset = u64::from_be_bytes(off_bytes);
            let timestamp = u128::from_be_bytes(ts_bytes);
            let payload = body[24..].to_vec();
            out.push(LogMessage {
                offset,
                timestamp,
                payload,
            });
        }
        Ok(out)
    }
}
