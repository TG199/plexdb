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

