use brotlic::{CompressorWriter};
use std::io::{Write};

pub fn to_brotli(data: Vec<u8>) -> Vec<u8> {
    let buff: Vec<u8> = vec![];
    let mut compressor = CompressorWriter::new(buff);
    compressor.write_all(data.as_slice()).unwrap();
    compressor.into_inner().unwrap()
}