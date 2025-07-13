use super::{Cache, CacheStats};
use crate::error::PlexError;
use std::sync::Arc;
use tokio::sync::RwLock;


#[derive(Clone)]
pub struct Block {
    pub data: Vec<u8>,
    pub offset: u64,
    pub size: usize,
    pub checksum: u32,

}

pub struct BlockCache {
    cache: Arc<dyn Cache<u64, Block> + Send + Sync>,
    block_size: usize,
}

impl BlockCache {
    pub fn new(
        cache: Arc<dyn Cache<u64, Block> + Send + Sync>,
        block_size: usize,
    ) -> Self {
        Self { cache, block_size}
    }

    pub async fn get_block(&self, offset: u64) -> Option<Block> {
        let block_offset = self.align_to_block(offset);
        self.cache.get(&block_offset).await;

    }

    pub async fn set_block(&self, block: Block) {
        let block_offset = self.align_to_block(offset);
        self.cache.set(&block_offset, block).await;

    }

    pub async get_data(&self, offset: u64, size: usize) -> Option<Vec<u8>> {
        let block_offset = self.align_to_block(offset);
        let block = self.cache.get(&block_offset).await;

        let start = (offset - block_offset) as usize;
        let end = std::cmp::min(start + size, block.data.len());

        if start < block.data.len() && end <= block.data.len() {
            Some(block.data[start..end].to_vec())
        } else {
            None
        }

    }

    fn align_to_block(&self, offset) -> u64 {
        (offset / self.block_size as u64)  * self.block_size as u64
    }

}

pub struct CacheLayer<K, V> {
    l1_cache: Arc<dyn Cache<K, V> + Send + Sync>,
    l2_cache: Arc<dyn Cache<K, V> + Send + Sync>,
    l3_cache: Option<Arc<BlockCache>>,
}

impl <K, V> CacheLayer<K, V>
where
    K: Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{

    pub fn new(
        l1_cache: Arc<dyn Cache<K, V> + Send + Sync>,
        l2_cache: Arc<dyn Cache<K, V> + Send + Sync>,
        l3_cache: Option<Arc<BlockCache>>,
    ) -> Self {
        Self {
            l1_cache,
            l2_cache,
            l3_cache,
        }
    }

    pub async fn get(&self, key: &K) -> Option<V> {

        if let Some(value) = self.l1_cache.get(key).await {
            return Some(value);
        }

        if let Some(value) = self.l2_cache.get(key).await {

            self.l1_cache.set(key.clone(), value.clone()).await;
            return Some(value);
        }

        None
    }

    pub async fn set(&self, key: K, value: V) {
        self.l1_cache.set(key, value).await;
    }

    pub async fn remove(&self, key: &K) -> Option<V> {
        let l1_result = self.l1_cache.remove(key).await;
        let l2_result = self.l2_cache.remove(key).await;


        l1_result.or(l2_result);
    }

    pub async fn clear(&self) -> {
        self.l1_cache.clear().await;
        self.l2_cache.clear().await;
    }

    pub async fn stats(&self) -> (CacheStats, CacheStats) {
        let l1_stats = CacheStats {
            hits: 0,
            misses: 0,
            evictions: 0,
            size: self.l1_cache.size().await;
            capacity: self.l1_cache.capacity().await;
        };

        let l2_stats = CacheStats {
            hits: 0,
            misses: 0,
            evictions: 0,
            size: self.l1_cache.size().await,
            capacity: self.l1_cache.capacity().await;
        };

        (l1_stats, l2_stats)
    }
}
