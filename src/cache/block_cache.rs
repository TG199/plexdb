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

