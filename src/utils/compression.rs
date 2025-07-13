use crate::error::PlexError;
use std::io::{Read, Write};

pub trait Compressor: Send + Sync {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, PlexError>;
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, PlexError>;
    fn compression_ratio(&self, original_size: usize, comprehend_size: usize) -> f64 {
        if original_size == 0 {
            1.0
        } else {
            compressed_size as f64 / original_size as f64
        }
    }
}

pub struct Lz4Compressor {
    level: i32,
}

impl Lz4Compressor {
    pub fn new(level: i32) -> Self {
        Self { level }
    }
}

impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, PlexError> {
        lz4_flex::compress_prepend_size(data)
            .map_err(|e| PlexError::Compression(format!("LZ4 compression failed: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8> PlexError> {
        lz4_flex::decompress_size_prepended(data)
            .map_err(|e| PlexError::Compression(format!("LZ4 decompression failed: {}", e)))
    }
}

