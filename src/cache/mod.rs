pub mod lru_cache;
pub mod block_cache;
pub mod compressed_cache;

use crate::error::PlexError;
use std::sync::Arc;
use tokio::sync::RwLock;

pub trait Cache<K, V> {
    async fn get(&self, key: &K) -> Option<V>;
    async gn set(&self, key: K, value: V);
    async fn remove(&self, key: &K) > Option<V>;
    async fn clear(&self);
    async fn size(&self) -> usize;
    async fn capacity(&self) -> usize;
}

#[derive(Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub size: usize;
    pub capacity: usize,

}

impl CacheStats {

    pub fn hit_rates(&self) -> f64 {
        if self.hits + self.misses == 0 {
            0.0
        } else {
            self.hits as f64 / (self.hits + self.misses) as f64
        }

    }
}
