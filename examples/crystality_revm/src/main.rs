//! Example: crystality_revm

use std::{fs, sync::Arc};

use revm::{context::{result::{ExecutionResult, Output}, BlockEnv, CfgEnv, TxEnv}, primitives::{hex, Address, Bytes, TxKind, B256, U256}, state::AccountInfo};

use crate::{codec::encoder::addr_from_u64, core::shard::{self, Shard, ShardRouter}};

mod core;
mod codec;

fn main() {
    let router = Arc::new(ShardRouter::new());
    let mut shard1 = Shard::new(1, CfgEnv::default(), BlockEnv::default(), router.clone());
    let mut shard2 = Shard::new(2, CfgEnv::default(), BlockEnv::default(), router.clone());
    
    let bin_path = "./contracts/bin/ERC20_Crystality.bin";
    let code = fs::read_to_string(bin_path).expect("error");
    let code_bytes = hex::decode(code).expect("decode error");

    println!("--- Deploy on contracts ---");
    let res1 = shard1.deploy(code_bytes.as_ref(), 0x111, &[]).unwrap();
    let res2 = shard2.deploy(code_bytes.as_ref(), 0x111, &[]).unwrap();
    // println!("contract deploy on shard1: {:?}",res1);
    // println!("contract deploy on shard2: {:?}",res2);
    let sender = addr_from_u64(0x1001);
    let selector = hex::decode("40c10f19").unwrap(); 
    let mut data = selector;
    let mut addr_bytes = [0u8; 32];
    addr_bytes[12..].copy_from_slice(sender.as_slice());
    data.extend_from_slice(&addr_bytes);
    let mut amount_bytes = [0u8; 32];
    let input = U256::from(10000u64).to_be_bytes::<32>();
    data.extend_from_slice(&input);

    let tx_mint = TxEnv::builder()
        .caller(sender)                
        .kind(TxKind::Call(addr_from_u64(0x111))) 
        .data(Bytes::from(data))
        .build()
        .unwrap();
    let res = shard1.execute(tx_mint).unwrap();
    println!("res: {:?}",res);
}
