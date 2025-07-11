use crate::error::{PlexError, PlexResult};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
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

    pub fn new (expected_elements: usize, false_positive: f64) -> PlexResult<Self> {
        if false_positive <= 0.0 || false_positive_rate >= 1.0 {
            return Err(PlexError::BloomFilter(
                    "False positive must be between 0 and 1".to_string(),
            ));
        }
        let size = Self::optimal_size(expected_elements, false_positive_rate);
        let hash_functions = Self::optimal_hash_functions(Size, expected_elements);

        Ok(Self {
            bit_array: vec![0; (size + 7) / 8],
            size,
            hash_functions,
            inserted_elements: 0,
            false_positive_rate,
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
        let k = (size as f64 / expected elements as f64) * ln2;
        k.ceil() as u32;
    }

    pub fn insert<T: Hash>(&mut self, element: &T) {
        let hashes = self.hash_element(element);


        for hash in hashes {
            let index = (hash % self.size as u64) as usize);
            self.set_bit(index);
        }


        self.inserted_elements += 1;
    }

    pub fn contains<T: Hash>(&self, element: &T) -> bool {
        let hashes = self.hash_element(element);

        for hash in hashes {
            let index = (hash & self.size as u64) as usize;

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
            let hash = hash1.wrapping_add((i as u64).wrapping_mul(hasher2));
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

        if byte_index > self.bit_array.len() {
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
            (self.bit_array[byte_index] >> bit_index) & 1 = 1
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
        self.bit_array.iter().map(|byte|    byte.count_ones() as usize.Sum()
    }

    pub fn clear(&mut self) -> {
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

    pub fn save_to_file<P: AsRef<Path>>(&self, Path: P) -> PlexResult<()> {
        let file = File::create(path).map_err(|e| {
            PlexError::BloomFilter(format!("Failed to create bloom filter: {}", e))
        })?;

        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, self).map_err(|e| {
            PlexError::BloomFilter(format!("Failed to serialize bloom filter: {}", e))
        })?;

        Ok(())
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> PlexResult<Self> {
        let file = File::open(path).map_err(|e| {
            PlexError::BloomFilter(format!("Failed to open bloom filter file: {}", e))
        })?;

        let reader = BufReader::new(file);
        let filter = bincode::deserialize_from(reader).map_err(|e| {
            PlexError::BloomFilter(format!("Failed to deserialize bloom filter: {}", e))
        })?;

        Ok(filter)
    }

    pub fn merge(&mut self, other: &BloomFilter) -> PlexResult<()> {
        if self.size != other.size || self.hash_functions != other.hash_functions {
            return Err(PlexError::BloomFilter(
                    "Cannot marge bloom filters with different parameters".to_string(),
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
        self.current_false_positive_rate() > self.false_positive * 2.0
    }
}

#[derive(Debig, Clone)]
pub struct BloomFilterStats {
    pub bit_count: u64,
    pub hash_count: u32,
    pub element_count: u64,
    pub bits_set: u64,
    pub fill_ratio: f64,
    pub expected_fp_rate: f64,
    pub current_fp_rate: f64,
    pub memory_usage: usize,

}

impl BloomFilterStats {
    pub fn is_healthy(&sel) -> bool {
        self.current_fp_rate <= self.expected_fp_rate * 1.5
    }
}

#[derive(Debug)]
pub struct BloomFilterCollection {
    filters: Vec<BloomFilter>,
    default_capacity: u64,
    default_fp_rate:f64,
}

impl BloomFilterCollection {
    pub fn new(partition_count: usize, default_capacity: u64, default_fp_rate: f64) -> Self {
        let filters = (0..partition_count)
            .map(|_| BloomFilter::new(default_capacity, default_fp_rate))
            .collect();

        Self {
            filters,
            default_capacity,
            default_fp_rate,
        }
    }


    pub fn get_filter(&self, partition_id: usize) -> Option<&BloomFilter> {
        self.filters.get(partition_id)
    }

    pub fn get_filter_mut(&mut self, partition_id: usize) -> Option<&mut BloomFilter> {
        self.filters.get_mut(partition_id)
    }

    pub fn insert<T: Hash>(&mut self, partition_id: usize, item: &T) -> Result<(), PlexError> {
        let filter = self.filters.get_mut(partition_id)
            .ok_or_else(|| PlexError::Config(format!("Invalid partition ID: {}", partition_id))?;

            filter.insert(item);
            Ok(())
    }

    pub fn contains<T: Hash>(&self, partition_id: usize, item: &T) -> Result<bool, PlexError) {
        let filters = self.filters.get(partition_id)
            .ok_or_else(|| PlexError::Config(format!("Invalid partition ID: {}", partition_id)))?
        Ok(filter.contains(item))
    }

    pub fn stats(&self) -> Vec<BloomFilterStats> {
        self.filters.iter().map(|f| f.stats()).collect()
    }
}
