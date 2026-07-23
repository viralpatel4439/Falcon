//! Proves the "attach any storage" seam: the sharded engine runs over an
//! arbitrary `ObjectStore` implementation, not just the local disk. A real S3
//! backend is one such implementation; here we use an in-memory fake so the
//! test needs no network — if the sharded engine is correct over this, it is
//! correct over any conforming backend (S3, MinIO, R2, …).

use async_trait::async_trait;
use falcon_storage::{FlushPolicy, ObjectStore, ShardedObjectStore, StorageEngine, StorageError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A minimal in-memory object store standing in for any third-party backend.
#[derive(Default)]
struct MemStore {
    objects: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

#[async_trait]
impl ObjectStore for MemStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.objects.lock().unwrap().insert(key.to_vec(), value.to_vec());
        Ok(())
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
    async fn list_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
    fn describe(&self) -> String {
        "mem".into()
    }
}

#[tokio::test]
async fn sharded_engine_runs_over_a_custom_backend() {
    let backend = Arc::new(MemStore::default());
    let engine = ShardedObjectStore::with_store(backend.clone(), 8, FlushPolicy::Sync).unwrap();

    // Write, read, overwrite, delete — full point semantics over the fake store.
    for i in 0..200u32 {
        engine
            .put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .await
            .unwrap();
    }
    assert_eq!(engine.get(b"k42").await.unwrap(), Some(b"v42".to_vec()));

    engine.put(b"k42", b"updated").await.unwrap();
    assert_eq!(engine.get(b"k42").await.unwrap(), Some(b"updated".to_vec()));

    engine.delete(b"k42").await.unwrap();
    assert_eq!(engine.get(b"k42").await.unwrap(), None);

    // The bucket count guarantee holds regardless of backend: 200 keys land in
    // at most 8 bucket objects, not 200.
    let object_count = backend.objects.lock().unwrap().len();
    assert!(object_count <= 8, "expected <= 8 bucket objects, got {object_count}");
}
