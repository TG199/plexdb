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


pub struct SnappyCompressor;

impl SnappyCompressor {
    pub fn new() -> Self {
        Self
    }
}

impl Compressor for SnappyCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, PlexError> {
        let mut encoder = snap::write::FrameEncoder::new(Vec::new());
        encoder.write_all(data)
            .map_err(|e| PlexError::Compression(format!("Snappy compression failed: {}", e)))?;
        encoder.into_inner()
            .map_err(|e| PlexError::Compression(format!("Snappy compression faile: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, PlexError> {
        let mut decoder = snap::read::FrameDecoder::new(data);
        let mut result = Vec::new();
        decoder.read_to_end(&mut result)
            .map_err(|e| PlexError::Compression(format!("Snappy decompression failed: {}", e)))?;
        Ok(result)
    }
}

pub struct ZstdCompressor {
    level: i32,
}

impl ZstdCompressor {
    pub fn new(level: i32) -> Self {
        Self { level}
    }
}

impl Compressor for ZstdCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, PlexError> {
        zstd::bulk::compress(data, self.level)
            .map_err(|e|PlexError::Compression(format!("Zstd compression failed: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, PlexError> {
        zstd::bulk::decompress(data, data.len() * 4)
            .map_err(|e| PlexError::Compression(format!("Zstd decompression failed: {}", e)))
    }
}

pub struct NoCompressor;

impl Compressor for NoCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, PlexError> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8, PlexError> {
        Ok(data.to_vec())
    }

}




}
