//! Encoder for crystality

use revm::primitives::Address;

pub fn addr_from_u64(x: u64) -> Address {
    let mut bytes = [0u8; 20];
    bytes[12..20].copy_from_slice(&x.to_be_bytes());
    Address::from(bytes)
}