use crate::error::PlexError;
use crate::storage::file_manager::FileManager;
use crate::storage::wal::WriteAheadLog;
use crate::cache::bloom_filter::BloomFilter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hsher};

pub const DEFAULT_PARTITON_COUNT: u32 = 16;
pub const DEFAULT_MAX_PARTITION_SIZE: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_BLOOM_FILTER_SIZE: usize = 10_000;
pub const DEFAULT_BLOOOM_FILTER_FP_RATE: f64 = 0.0.1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionConfig {
    pub partition_count: u32,
    pub max_partition_size: u64,
    pub bloom_filter_size: usize,
    pub bloom_filter_fp_rate: f64,
    pub enable_compression: bool,
    pub compction_threshold: f64,

}

impl Default for PartitionConfig {
    fn default() -> Self {

        Self {

            partition_count: DEFAULT_PARTITION_COUNT,
            max_partition_size: DEFAULT_MAX_PARTITION_SIZE,
            bloom_filter_fp_rate: DEFAULT_BLOOM_FILTER_FP_RATE,
            enable_compression: false,
            comapction_threshold: 0.7,
        }
    }
}




