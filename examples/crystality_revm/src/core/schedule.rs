//! Schedule(Simulator) for the crystal revm

use std::sync::Arc;

use crossbeam_channel::{unbounded, Receiver, Sender};
use revm::{bytecode::bitvec::view::BitViewSized, context::{BlockEnv, CfgEnv, Transaction, TxEnv}};

use crate::core::{shard::{Shard, ShardMsg, ShardRouter}, ShardId, GLOBAL_SHARD_ID, PHYSICAL_CORES};


pub struct Simulator{
    pub globalshard: Sender<ShardMsg>,
    pub shards: Vec<Sender<ShardMsg>>,
    pub shard_count:u32
}

impl Simulator {
    pub fn new(order:u32) -> Self {
        let shard_count = 1<<order;
        assert!( shard_count as usize <= PHYSICAL_CORES.len() );

        let mut channels :Vec<(Sender<ShardMsg>,Receiver<ShardMsg>)>  = Vec::with_capacity(shard_count as usize);
        for _ in 0..shard_count{
            channels.push(unbounded());
        }
        let (global_sender,global_receiver) = unbounded::<ShardMsg>();
        
        let mut shards: Vec<Sender<ShardMsg>> = channels.iter().map(|(tx, _)| tx.clone()).collect();
        
        let router = Arc::new(ShardRouter::new(global_sender.clone(),shards.clone()));


        for (i, (_, rx)) in channels.into_iter().enumerate() {
            let shard = Shard::new(i as ShardId, CfgEnv::default(), BlockEnv::default(), router.clone());
            shard.start(rx); 
        }

        let global =Shard::new(GLOBAL_SHARD_ID, CfgEnv::default(), BlockEnv::default(), router.clone());
        global.start(global_receiver);
        
        Self {
            globalshard:global_sender,
            shards,
            shard_count
        }
    }

    pub fn add_global_task(&self, txn:TxEnv){
        let _ = self.globalshard.send(ShardMsg::PushTxn(txn));
    }

    pub fn add_task(&self, txn:TxEnv){
        let sender_index = txn.caller().as_raw_slice()[16..20].try_into().expect("invalid address");
        let sid = (u32::from_be_bytes(sender_index) % self.shard_count) as usize;
        let _ = self.shards[sid].send(ShardMsg::PushTxn(txn));
    }

    pub fn get_shard_count(&self) -> u32{
        self.shard_count
    }

    pub fn deploy(&self, code: &[u8], address_index: u64, input_data: &[u8]) {
        let mut receivers = Vec::new();
        //global shard
        {
            let (tx, rx) = unbounded::<ShardId>();
            let msg = ShardMsg::Deploy {
                code: code.to_vec(),
                address_index,
                input_data: input_data.to_vec(),
                reply: tx,
            };
            let _ = self.globalshard.send(msg);
            receivers.push(rx);
        }
        
        // normal shards
        for shard in &self.shards {
            let (tx, rx) = unbounded::<ShardId>();
            let msg = ShardMsg::Deploy {
                code: code.to_vec(),
                address_index,
                input_data: input_data.to_vec(),
                reply: tx,
            };
            let _ = shard.send(msg);
            receivers.push(rx);
        }
        for receiver in receivers{
            let _ = receiver.recv().unwrap();
        }
    }

    fn finished(&self) -> bool {
            let (tx, rx) = unbounded::<(ShardId,usize)>();
            let _ = self.globalshard.send(ShardMsg::GetTxnSize { reply: tx });
            let (id,size) = rx.recv().unwrap();
            println!("global Shard#{} txn size: {}", id, size);
            if size > 0 {
                return false;
            }
            let mut receivers = Vec::new();
            for shard in &self.shards {
                let (tx, rx) = unbounded::<(ShardId,usize)>();
                let _ = shard.send(ShardMsg::GetTxnSize { reply: tx });
                receivers.push(rx);
            }
            for receiver in receivers{
                let (id,size): (u32, usize) = receiver.recv().unwrap();
                println!("Normal Shard#{} txn size: {}", id, size);
                if size > 0 {
                    return false;
                }
            }
            true
        }

    pub fn run(&self) {
        loop{
            let mut receivers = Vec::new();
            let (tx, rx) = unbounded::<ShardId>();
            let _ = self.globalshard.send(ShardMsg::Step { reply: tx });
            let id = rx.recv().unwrap();
            println!("global Shard#{} finished step", id);
            //bottleneck??? 
            for shard in &self.shards {
                let (tx, rx) = unbounded::<ShardId>();
                let _ = shard.send(ShardMsg::Step { reply: tx});
                receivers.push(rx);
            }
            // crossbeam::scope(|s| {
            //     for shard in &self.shards {
            //         let (tx, rx) = unbounded::<ShardId>();
            //         receivers.push(rx);
            //         s.spawn(move |_| {
            //             let _ = shard.send(ShardMsg::Step { reply: tx });
            //         });
            //     }
            // }).unwrap();
            for receiver in receivers{
                let id = receiver.recv().unwrap();
                println!("Normal Shard#{} finished step", id);
            }
            if self.finished() {
                break;
            }
        }
    }

    pub fn stop(&self){
        let _ = self.globalshard.send(ShardMsg::Stop);
        for shard in &self.shards{
            let _ = shard.send(ShardMsg::Stop);
        }
    }
        

}
    
    