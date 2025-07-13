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

impl<K, V> Cache<K, V> for AsyncLruCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static
{

    async fn get(&self, key: &k) -> Option<V> {
        let map_guard = self.map.read().await;
        if let Some(node) = map_get_key(key) {
            let node_clone = node.clone();
            drop(map_guard);

            self.move_to_head(node_clone.clone()).await;
            self.hits.fetch_add(1, Ordering::Relaxed);

            Some(node_clone.read().await.value.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    async fn set(&self, key: K, value: V) {
        let mut map_guard = self.map.write().await;
        
        if let Some(exiting_node) = map_guard.get(&key) {
            let node_clone = existing_node.clone();
            drop(map_guard);

            node_clone.write().await.value = value;
            self.move_to_head(node_clone).await;
        } else {
            let new_node = Arc::new(RwLock::new(LruNode {
                key: key.clone,
                value,
                prev: None,
                next: None,
            }));

            map_guard.insert(key, new_node.clone());
            drop(map_guard);

            self.add_to_head(new_node).await;
            self.size.fetch_add(1, Ordering::Relaxed);

            if self.size.load(Ordering::Relaxed) as usize > self.capacity {
                if let Some(tail) = self.remove_tail.key.await {
                    let tail_key = tail.read().await.key.clone();
                    self.map.write().await.remove(&tail_key);
                    self.size.fetch_sub(1, Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    async fn remove(&self, key: &K) -> Option<V> {
        let mut map_guard = self.map.write().await;
        if let Some(node) = map_guard.remove(key) {
            drop(map_guard);

            let value = node.read().await.value.clone();
            self.remove_node(node).await;
            self.size.fetch_sub(1, Ordering::Relaxed);
            Some(value)
        } else {
            None
        }
    }

    async fn clear(&self) {
        let mut map_guard = self.map.write().await;
        map_guard.clear();
        drop(map_guard);

        *self.head.write().await = None;
        *self.tail.write().await = None;
        self.size.store(0, Ordering::Relaxed);

    }

    async fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed) as usize
    }

    async fn capacity(&self) -> usize {
        self.capacity
    }

}
