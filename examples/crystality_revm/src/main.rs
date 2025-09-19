//! Example: crystality_revm

use std::{fs, sync::Arc};

use crossbeam_channel::unbounded;
use revm::{context::{BlockEnv, CfgEnv, TxEnv}, primitives::{hex, Bytes, TxKind, U256}};

use crate::{codec::encoder::addr_from_u64, core::{schedule::Simulator, shard::{Shard, ShardMsg, ShardRouter}}};

mod core;
mod codec;

fn main() {
    let mut sim = Simulator::new(2);
    
    let bin_path = "./contracts/bin/ERC20_Crystality.bin";
    let code = fs::read_to_string(bin_path).expect("error");
    let code_bytes = hex::decode(code).expect("decode error");

    println!("--- Deploy on contracts ---");
    let results = sim.deploy(&code_bytes, 0x111, &[]);

     println!("--- All shards deployed, continue call simulation ---");
     
    let sender = addr_from_u64(0x1001);
    let selector = hex::decode("40c10f19").unwrap(); 
    let mut data = selector;
    let mut addr_bytes = [0u8; 32];
    addr_bytes[12..].copy_from_slice(sender.as_slice());
    data.extend_from_slice(&addr_bytes);
    let amount_bytes = [0u8; 32];
    let input = U256::from(10000u64).to_be_bytes::<32>();
    data.extend_from_slice(&input);

    let tx_mint = TxEnv::builder()
        .caller(sender)                
        .kind(TxKind::Call(addr_from_u64(0x111))) 
        .data(Bytes::from(data))
        .build()
        .unwrap();

    for shard in &sim.shards{
        for i in 0..100 {
            shard.send(ShardMsg::PushTxn(tx_mint.clone())).unwrap();
        }
    }

    sim.run();

    let (tx, rx) = unbounded::<u64>();
    sim.globalshard.send(ShardMsg::GetBlockHeight{reply:tx}).unwrap();
    let height = rx.recv().unwrap();
    println!("block height: {:?}", height);
    
    let (tx, rx) = unbounded::<u64>();
    sim.globalshard.send(ShardMsg::GetExecutedTxns{reply:tx}).unwrap();
    let txns = rx.recv().unwrap();
    println!("executed txns: {:?}", txns);
}
