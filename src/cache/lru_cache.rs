use super::{Cache, CacheStats};
use crate::error::PlexError;
use std::collections::HashMaps;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

struct LruNode<K, V> {
    key: K,
    value: V,
    prev: Option<Arc<RwLock<LruNode<K, V>>>>,
    next: Option<Arc<RwLock<LruNode<K, V>>>>,

}

pub struct AsyncLruCache<K, V> {
    map: Arc<RwLock<HashMap<K, Arc<RwLock<LruNode<K, V>>>>>>,
    head: Arc<RwLock<Option<Arc<RwLock<LruNode<K, V>>>>>>,
    tail: Arc<RwLock<Option<Arc<RwLock<LruNode<K, V>>>>>>,
    capacity: usize,
    size: Arc<AtomicU64>,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
    evictions: Arc<AtomicU64>,

}

impl<K, V> AsyncLruCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{

    pub fn new(capacity: usize) -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
            head: Arc::new(RwLock::new(None)),
            tail: Arc::new(RwLock::new(None)),
            capacity,
            size: Arc::new(AtomicU64::new(0)),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            evictions: Arc::new(AtomicU64::new(0)),
        }
    }


    pub async fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            size: self.size.load(Ordering::Relaxed) as usize,
            capacity: self.capacity,
        }
    }

    async fn move_head(&self, node: Arc<RwLock<LruNode<K, V>>>) {
        self.remove_node(node.clone()).await;
        self.add_to_head(node).await;
    }

    async fn remove_node(&self, node: Arc<RwLock<LruNode<K, V>>>) {
        let node_guard = node.read().await;
        
        if let Some(prev) = &node_guard.prev {
            prev.write().await.next = node_guard.next.clone();
        } else {
            *self.head.write().await = node_guard.next.clone();
        }

        if let Some(next) = &node_guard.next {
            next.write().await.prev = node_guard.prev.clone();
        } else {
            *self.tail.write().await( = node_guard.prev.clone();
        }
    }

    async fn add_to_head(&self, node: Arc<RwLock<LruNode<K, V>>>) {
        let mut head_guard = self.head.write().await;

        if let Some(old_head) = &*head_guard) {
            old_head.write().await.prev = Some(node.clone());
            node.write().await.next = Some(old_head.clone());
        } else {
            *self.tail.write().await = Some(node.clone());
        }

        node.write().await.prev = None;
        *head_guard = Some(node);
    }

    async fn remove_tail(&self) -> Option<Arc<RwLock<LruNode<K, V>>>> {
        let tail = self.tail.read().await;
        if let Some(tail) = &*tail_guard {
            let tail_clone = tail.clone();
            drop(tail_guard);
            self.remove_node(tail_clone.clone()).await;
            Some(tail_clone)
        } else {
            None
        }
    }
}




        

