//! Benchmark client for kvstored's binary wire protocol, with pipelining.

use anyhow::Result;
use bytes::BytesMut;
use falcon_wire::{
    encode_request, OP_ACK, OP_GET, OP_POP, OP_PUBLISH, OP_PUSH, OP_SET, OP_STREAM_APPEND,
    STATUS_EMPTY, STATUS_MESSAGE, STATUS_STORED,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

/// A parsed wire reply: the status byte and any payload bytes that followed.
pub struct Reply {
    pub status: u8,
    pub payload: Vec<u8>,
}

pub struct WireClient {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
    scratch: BytesMut,
}

impl WireClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (r, w) = stream.into_split();
        Ok(Self {
            reader: BufReader::with_capacity(64 * 1024, r),
            writer: BufWriter::with_capacity(64 * 1024, w),
            scratch: BytesMut::with_capacity(64 * 1024),
        })
    }

    /// Pipeline a batch of SETs: send all requests, then read all replies.
    pub async fn pipeline_set(&mut self, keys: &[String], value: &[u8]) -> Result<()> {
        self.scratch.clear();
        for k in keys {
            encode_request(&mut self.scratch, OP_SET, b"", k.as_bytes(), value);
        }
        self.writer.write_all(&self.scratch).await?;
        self.writer.flush().await?;
        for _ in keys {
            self.read_reply().await?;
        }
        Ok(())
    }

    /// Pipeline a batch of GETs: send all requests, then read all replies.
    pub async fn pipeline_get(&mut self, keys: &[String]) -> Result<()> {
        self.scratch.clear();
        for k in keys {
            encode_request(&mut self.scratch, OP_GET, b"", k.as_bytes(), b"");
        }
        self.writer.write_all(&self.scratch).await?;
        self.writer.flush().await?;
        for _ in keys {
            self.read_reply().await?;
        }
        Ok(())
    }

    async fn read_reply(&mut self) -> Result<()> {
        self.read_reply_full().await.map(|_| ())
    }

    /// Read one reply and return its status + payload (for messaging/stream
    /// ops that carry data back).
    pub async fn read_reply_full(&mut self) -> Result<Reply> {
        let mut header = [0u8; 5];
        self.reader.read_exact(&mut header).await?;
        let status = header[0];
        let val_len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let mut payload = vec![0u8; val_len];
        if val_len > 0 {
            self.reader.read_exact(&mut payload).await?;
        }
        Ok(Reply { status, payload })
    }

    // ---- Pub/Sub ----

    /// Pipeline a batch of PUBLISH to `topic`; read all OKs.
    pub async fn pipeline_publish(&mut self, topic: &str, payloads: &[Vec<u8>]) -> Result<()> {
        self.scratch.clear();
        for p in payloads {
            encode_request(&mut self.scratch, OP_PUBLISH, topic.as_bytes(), b"", p);
        }
        self.writer.write_all(&self.scratch).await?;
        self.writer.flush().await?;
        for _ in payloads {
            self.read_reply().await?;
        }
        Ok(())
    }

    // ---- Queue ----

    /// Pipeline a batch of PUSH to `queue`; read all OKs.
    pub async fn pipeline_push(&mut self, queue: &str, payloads: &[Vec<u8>]) -> Result<()> {
        self.scratch.clear();
        for p in payloads {
            encode_request(&mut self.scratch, OP_PUSH, queue.as_bytes(), b"", p);
        }
        self.writer.write_all(&self.scratch).await?;
        self.writer.flush().await?;
        for _ in payloads {
            self.read_reply().await?;
        }
        Ok(())
    }

    /// POP one message for `group`. Returns `(offset, payload)` or None if the
    /// queue is empty.
    pub async fn pop(&mut self, queue: &str, group: &str) -> Result<Option<(u64, Vec<u8>)>> {
        self.scratch.clear();
        encode_request(&mut self.scratch, OP_POP, queue.as_bytes(), group.as_bytes(), b"");
        self.writer.write_all(&self.scratch).await?;
        self.writer.flush().await?;
        let reply = self.read_reply_full().await?;
        match reply.status {
            STATUS_EMPTY => Ok(None),
            STATUS_MESSAGE => {
                let offset = u64::from_le_bytes(reply.payload[0..8].try_into().unwrap());
                Ok(Some((offset, reply.payload[8..].to_vec())))
            }
            _ => Ok(None),
        }
    }

    /// ACK a popped message by offset.
    pub async fn ack(&mut self, queue: &str, group: &str, offset: u64) -> Result<()> {
        self.scratch.clear();
        encode_request(
            &mut self.scratch,
            OP_ACK,
            queue.as_bytes(),
            group.as_bytes(),
            &offset.to_be_bytes(),
        );
        self.writer.write_all(&self.scratch).await?;
        self.writer.flush().await?;
        self.read_reply().await?;
        Ok(())
    }

    // ---- Streams ----

    /// Pipeline a batch of STREAM_APPEND to `stream`, each routed by `key`.
    /// Returns the (partition, offset) of each appended record.
    pub async fn pipeline_stream_append(
        &mut self,
        stream: &str,
        key: &[u8],
        payloads: &[Vec<u8>],
    ) -> Result<Vec<(u32, u64)>> {
        self.scratch.clear();
        for p in payloads {
            encode_request(&mut self.scratch, OP_STREAM_APPEND, stream.as_bytes(), key, p);
        }
        self.writer.write_all(&self.scratch).await?;
        self.writer.flush().await?;
        let mut out = Vec::with_capacity(payloads.len());
        for _ in payloads {
            let reply = self.read_reply_full().await?;
            if reply.status == STATUS_STORED {
                let partition = u32::from_le_bytes(reply.payload[0..4].try_into().unwrap());
                let offset = u64::from_le_bytes(reply.payload[4..12].try_into().unwrap());
                out.push((partition, offset));
            }
        }
        Ok(out)
    }
}
