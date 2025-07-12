use super::{Cache, CacheStats};
use crate::utils::comprssion::Compressor;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct CompressedCache<K, V> {
    inner: Arc<dyn Cache<K, Vec<u8>> + Send + Sync>,
    compressor: Arc<dyn Compressor + Send + Sync>,

}

impl<K, V> CompressedCache<K, V>
where
    K: Clone + Send + Sync 'static,
    V: Clone + Send + Sync serde::Serialize + serde::de::DeserializedOwned + 'static,
{

    pub fn new(
        inner: Arc<dyn Cache<K, Vec<<u8>> + Send + Sync>,
        compressore: Arc<dyn Compressor + Send + Sync>,
    ) -> Self {
        Self { inner, compressor }
    }

}
impl<K, V> Cache<K, V> for CompressedCache<K, V>
where

    K: Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + serde::de::Serialize + serde::de::DeserializOwned + 'static,
{

    async fn get(&self, key: &k) -> Option<V> {
        if let Some(compressed_data) = self.inner.get(key).await {
            match self.compressor.decompress(&compressed_data) {
                Ok(data) => match bincode::deserialize(&data) {
                    Ok(value) => Some(value),
                    Err(_) => None,

                },
                Err(_) => None,
            }
        } else {
            None
        }
    }
    async fn set(&self, key: K, value: V) ->  Option<V> {
        if let Ok(serialized) = bincode::serialize(&value) {
            if let Ok(compressed) = self.compressor.compress(&serialized) {
                self.inner.set(key, compressed).await;
            }
        }
    }

    async fn remove(&self, key: &k) -> Option<V> {
        if let Some(compressed_data) = self.inner.remove(key).await {
            match self.compressor.decompress(&compressed_data) {
                Ok(data) => match bincode::deserialize(&data) {
                    Ok(value) => Some(value),
                    Err(_) => None,
                },
                Err(_) => None,
            }
        
        } else {
            None
        }
    }

    async fn clear(&self) {
        self.inner.clear().await;
    }

    async fn size(&self) -> usize {
        self.inner.size().await
    }

    async fn capacity(&self) -> usize {
        self.inner.capacity().await
    }
}

