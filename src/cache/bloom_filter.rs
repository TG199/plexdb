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



