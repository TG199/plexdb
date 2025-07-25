use crate::error::{PlexError, PlexResult};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    bit_array: Vec<u8>,
    size: usize,
    hash_functions: u32,
    inserted_elements: u64,
    false_positive_rate: f64,
}

impl BloomFilter {
    pub fn new(expected_elements: usize, false_positive: f64) -> PlexResult<Self> {
        if false_positive <= 0.0 || false_positive >= 1.0 {
            return Err(PlexError::BloomFilter(
                "False positive must be between 0 and 1".to_string(),
            ));
        }
        
        let size = Self::optimal_size(expected_elements, false_positive);
        let hash_functions = Self::optimal_hash_functions(size, expected_elements);

        Ok(Self {
            bit_array: vec![0; (size + 7) / 8],
            size,
            hash_functions,
            inserted_elements: 0,
            false_positive_rate: false_positive,
        })
    }

    pub fn from_data(
        bit_array: Vec<u8>,
        size: usize,
        hash_functions: u32,
        inserted_elements: u64,
        false_positive_rate: f64,
    ) -> Self {
        Self {
            bit_array,
            size,
            hash_functions,
            inserted_elements,
            false_positive_rate,
        }
    }

    fn optimal_size(expected_elements: usize, false_positive_rate: f64) -> usize {
        let ln2 = std::f64::consts::LN_2;
        let size = -(expected_elements as f64 * false_positive_rate.ln()) / (ln2 * ln2);
        size.ceil() as usize
    }

    fn optimal_hash_functions(size: usize, expected_elements: usize) -> u32 {
        let ln2 = std::f64::consts::LN_2;
        let k = (size as f64 / expected_elements as f64) * ln2;
        k.ceil() as u32
    }

    pub fn insert<T: Hash>(&mut self, element: &T) {
        let hashes = self.hash_element(element);

        for hash in hashes {
            let index = (hash % self.size as u64) as usize;
            self.set_bit(index);
        }

        self.inserted_elements += 1;
    }

    pub fn contains<T: Hash>(&self, element: &T) -> bool {
        let hashes = self.hash_element(element);

        for hash in hashes {
            let index = (hash % self.size as u64) as usize;
            if !self.get_bit(index) {
                return false;
            }
        }
        true
    }

    fn hash_element<T: Hash>(&self, element: &T) -> Vec<u64> {
        let mut hashes = Vec::with_capacity(self.hash_functions as usize);

        let mut hasher1 = DefaultHasher::new();
        element.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        hash1.hash(&mut hasher2);
        element.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        for i in 0..self.hash_functions {
            let hash = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
            hashes.push(hash);
        }

        hashes
    }

    fn set_bit(&mut self, index: usize) {
        if index >= self.size {
            return;
        }

        let byte_index = index / 8;
        let bit_index = index % 8;

        if byte_index < self.bit_array.len() {
            self.bit_array[byte_index] |= 1 << bit_index;
        }
    }

    fn get_bit(&self, index: usize) -> bool {
        if index >= self.size {
            return false;
        }

        let byte_index = index / 8;
        let bit_index = index % 8;

        if byte_index < self.bit_array.len() {
            (self.bit_array[byte_index] >> bit_index) & 1 == 1
        } else {
            false
        }
    }

    pub fn current_false_positive_rate(&self) -> f64 {
        if self.inserted_elements == 0 {
            return 0.0;
        }

        let set_bits = self.count_set_bits();
        let ratio = set_bits as f64 / self.size as f64;

        let exponent = -(self.hash_functions as f64 * self.inserted_elements as f64) / self.size as f64;
        let base = 1.0 - (-exponent).exp();
        base.powf(self.hash_functions as f64)
    }

    fn count_set_bits(&self) -> usize {
        self.bit_array.iter().map(|byte| byte.count_ones() as usize).sum()
    }

    pub fn clear(&mut self) {
        self.bit_array.fill(0);
        self.inserted_elements = 0;
    }

    pub fn stats(&self) -> BloomFilterStats {
        BloomFilterStats {
            size: self.size,
            hash_functions: self.hash_functions,
            inserted_elements: self.inserted_elements,
            set_bits: self.count_set_bits(),
            current_false_positive_rate: self.current_false_positive_rate(),
            target_false_positive_rate: self.false_positive_rate,
            memory_usage: self.bit_array.len(),
        }
    }


    fn create_bloom_filter_error(operation: &str, error: impl std::fmt::Display) -> PlexError {
        PlexError::BloomFilter(format!("Failed to {}: {}", operation, error))
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> PlexResult<()> {
        let file = File::create(&path).map_err(|e| {
            Self::create_bloom_filter_error("create bloom filter file", e)
        })?;

        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, self).map_err(|e| {
            Self::create_bloom_filter_error("serialize bloom filter", e)
        })?;

        Ok(())
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> PlexResult<Self> {
        let file = File::open(&path).map_err(|e| {
            Self::create_bloom_filter_error("open bloom filter file", e)
        })?;

        let reader = BufReader::new(file);
        let filter = bincode::deserialize_from(reader).map_err(|e| {
            Self::create_bloom_filter_error("deserialize bloom filter", e)
        })?;

        Ok(filter)
    }

    pub fn merge(&mut self, other: &BloomFilter) -> PlexResult<()> {
        if self.size != other.size || self.hash_functions != other.hash_functions {
            return Err(PlexError::BloomFilter(
                "Cannot merge bloom filters with different parameters".to_string(),
            ));
        }

        for (i, byte) in other.bit_array.iter().enumerate() {
            if i < self.bit_array.len() {
                self.bit_array[i] |= byte;
            }
        }

        self.inserted_elements += other.inserted_elements;
        Ok(())
    }

    pub fn should_resize(&self) -> bool {
        self.current_false_positive_rate() > self.false_positive_rate * 2.0
    }
}

#[derive(Debug, Clone)]
pub struct BloomFilterStats {
    pub size: usize,
    pub hash_functions: u32,
    pub inserted_elements: u64,
    pub set_bits: usize,
    pub current_false_positive_rate: f64,
    pub target_false_positive_rate: f64,
    pub memory_usage: usize,
}

impl BloomFilterStats {
    pub fn is_healthy(&self) -> bool {
        self.current_false_positive_rate <= self.target_false_positive_rate * 1.5
    }
}

#[derive(Debug)]
pub struct BloomFilterCollection {
    filters: Vec<BloomFilter>,
    default_capacity: usize,
    default_fp_rate: f64,
}

impl BloomFilterCollection {
    pub fn new(partition_count: usize, default_capacity: usize, default_fp_rate: f64) -> PlexResult<Self> {
        let mut filters = Vec::with_capacity(partition_count);
        
        for _ in 0..partition_count {
            let filter = BloomFilter::new(default_capacity, default_fp_rate)?;
            filters.push(filter);
        }

        Ok(Self {
            filters,
            default_capacity,
            default_fp_rate,
        })
    }

    pub fn get_filter(&self, partition_id: usize) -> Option<&BloomFilter> {
        self.filters.get(partition_id)
    }

    pub fn get_filter_mut(&mut self, partition_id: usize) -> Option<&mut BloomFilter> {
        self.filters.get_mut(partition_id)
    }

    fn validate_partition_id(&self, partition_id: usize) -> PlexResult<()> {
        if partition_id >= self.filters.len() {
            return Err(PlexError::Config(format!("Invalid partition ID: {}", partition_id)));
        }
        Ok(())
    }

    pub fn insert<T: Hash>(&mut self, partition_id: usize, item: &T) -> PlexResult<()> {
        self.validate_partition_id(partition_id)?;
        
        let filter = &mut self.filters[partition_id];
        filter.insert(item);
        Ok(())
    }

    pub fn contains<T: Hash>(&self, partition_id: usize, item: &T) -> PlexResult<bool> {
        self.validate_partition_id(partition_id)?;
        
        let filter = &self.filters[partition_id];
        Ok(filter.contains(item))
    }

    pub fn stats(&self) -> Vec<BloomFilterStats> {
        self.filters.iter().map(|f| f.stats()).collect()
    }

    pub fn rebuild_degraded_filters(&mut self) -> PlexResult<()> {
        for filter in &mut self.filters {
            if filter.should_resize() {
                let new_filter = BloomFilter::new(self.default_capacity, self.default_fp_rate)?;
                *filter = new_filter;
            }
        }
        Ok(())
    }

    pub fn save_to_directory<P: AsRef<Path>>(&self, dir_path: P) -> PlexResult<()> {
        let dir = dir_path.as_ref();
        std::fs::create_dir_all(dir).map_err(|e| {
            PlexError::BloomFilter(format!("Failed to create directory: {}", e))
        })?;

        for (i, filter) in self.filters.iter().enumerate() {
            let file_path = dir.join(format!("bloom_filter_{:03}.bf", i));
            filter.save_to_file(file_path)?;
        }

        Ok(())
    }

    pub fn load_from_directory<P: AsRef<Path>>(dir_path: P) -> PlexResult<Self> {
        let dir = dir_path.as_ref();
        let mut filters = Vec::new();

        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .map_err(|e| PlexError::BloomFilter(format!("Failed to read directory: {}", e)))?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().extension()
                    .map_or(false, |ext| ext == "bf")
            })
            .collect();

        entries.sort_by_key(|entry| entry.path());

        for entry in entries {
            let filter = BloomFilter::load_from_file(entry.path())?;
            filters.push(filter);
        }

        if filters.is_empty() {
            return Err(PlexError::Config("No bloom filter files found".to_string()));
        }

        let default_capacity = filters[0].inserted_elements.max(1000) as usize;
        let default_fp_rate = filters[0].false_positive_rate;

        Ok(Self {
            filters,
            default_capacity,
            default_fp_rate,
        })
    }
}
