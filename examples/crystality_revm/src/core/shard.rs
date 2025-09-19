//! shard

use std::{cell::RefCell, collections::VecDeque, rc::Rc, sync::{Arc, Mutex}, thread};
use core_affinity::{set_for_current, CoreId};
use crossbeam_channel::{Sender,Receiver,unbounded};

use revm::{context::{result::{EVMError, ExecutionResult}, BlockEnv, CfgEnv, Transaction, TxEnv}, primitives::{Address, StorageKey, TxKind, U256}, Database};

use crate::core::{db::{CrystalityDB, CrystalityDBError}, engine::{self, EvmExecuteEngine}, ShardId, GLOBAL_SHARD_ID, MAX_TXN_PER_BLOCK, PHYSICAL_CORES, RELAY_TO_ADDRESS, RELAY_TO_GLOBAL, RELAY_TO_SHARDS};
use crate::codec::encoder::addr_from_u64;

#[derive(Debug)]
pub enum ShardMsg {
    PushTxn(TxEnv),
    Deploy {
        code: Vec<u8>,
        address_index: u64,
        input_data: Vec<u8>,
        reply: Sender<ShardId>,
    },
    Step {
        reply: Sender<ShardId>,
    },
    GetTxnSize{
        reply: Sender<(ShardId, usize)>
    },
    GetGlobalStorage {
        address: Address,
        slot: StorageKey,
        reply: Sender<Option<U256>>,
    },
    GetBlockHeight{
        reply: Sender<u64>,
    },
    GetExecutedTxns{
        reply: Sender<u64>,
    },
    Stop,
}

#[derive(Clone)]
pub struct ShardRouter{
    globalshard: Sender<ShardMsg>,
    shards:Arc<Vec<Sender<ShardMsg>>>,
}

impl ShardRouter {
    pub fn new(global_sender:Sender<ShardMsg>, senders: Vec<Sender<ShardMsg>>)->Self{
        Self{
            globalshard:global_sender,
            shards:Arc::new(senders),
        }
    }
    pub fn global(&self)->Sender<ShardMsg>{
        self.globalshard.clone()
    }
    pub fn relay_to_global(&self, txn:TxEnv){
        let _ = self.globalshard.send(ShardMsg::PushTxn(txn));
    }
    pub fn relay_to_shard(&self, txn:TxEnv){
        // let _ = self.shards[shard_id as usize].send(ShardMsg::Relay(txn));
    }
    pub fn relay_to_all_shards(&self, txn:TxEnv){
        for shard in self.shards.iter(){
            let _ = shard.send(ShardMsg::PushTxn(txn.clone()));
        }
    }
}

pub struct Shard{
    pub id:ShardId,
    pub db:CrystalityDB,
    pub cfg:CfgEnv,
    pub block:BlockEnv,
    pub engine:EvmExecuteEngine,
    pub pending_txns: Mutex<VecDeque<TxEnv>>,
    pub relay_txns:Receiver<TxEnv>,
    pub router: Arc<ShardRouter>,
    block_height:u64,
    executed_txns:u64,
}

impl Shard {
    pub fn new(id:ShardId, cfg:CfgEnv, block:BlockEnv, router: Arc<ShardRouter>)->Self{
        let db = CrystalityDB::new(id, router.global());
        let (relay_emits,relay_txns) = unbounded::<TxEnv>();
        let engine = EvmExecuteEngine::new(relay_emits);
        Self{
            id,
            db,
            cfg,
            block,
            engine,
            pending_txns:Mutex::new(VecDeque::new()),
            relay_txns,
            router,
            block_height:0,
            executed_txns:0,
        }
    }

    pub fn push_txn(&mut self, txn: TxEnv) {
         self.pending_txns.lock().unwrap().push_back(txn);
    }

    pub fn pop_txn(&mut self)->Option<TxEnv>{
        self.pending_txns.lock().unwrap().pop_front()
    }

    pub fn txn_size(&self)->usize{
        self.pending_txns.lock().unwrap().len()
    }

    pub fn execute(
        &mut self,
        tx:TxEnv
    )->Result<ExecutionResult, EVMError<<CrystalityDB as Database>::Error>>{
        self.engine.execute(&mut self.db, &self.cfg, &self.block, tx)
    }

    pub fn deploy(
        &mut self,
        code: &[u8],
        address_index: u64,
        input_data: &[u8],
    )->Result<ExecutionResult, EVMError<<CrystalityDB as Database>::Error>>{
        self.engine.deploy(&mut self.db, &self.cfg, &self.block, code, address_index, input_data)
    }

    pub fn dispatch(&mut self){
        for relay in self.relay_txns.try_iter(){
            let target = match &relay.kind { TxKind::Call(to) => *to, _ => panic!() };
            if target == addr_from_u64(RELAY_TO_GLOBAL) {
                let mut tx = relay.clone();
                tx.kind = TxKind::Call(tx.caller);
                self.router.relay_to_global(tx);  
            } else if target == addr_from_u64(RELAY_TO_ADDRESS) {
               let mut tx = relay.clone();
                tx.kind = TxKind::Call(tx.caller);
                self.router.relay_to_shard(tx);
            } else if target == addr_from_u64(RELAY_TO_SHARDS) {
                let mut tx = relay.clone();
                tx.kind = TxKind::Call(tx.caller);
                self.router.relay_to_all_shards(tx);
            } else {
                panic!()
            }
           
        }
    }

    pub fn start(
        mut self,
        receiver: Receiver<ShardMsg>,
    ) {
        thread::spawn(move||{
            if self.id == GLOBAL_SHARD_ID {
                let core = CoreId{id: 0 as usize};
                set_for_current(core);
                println!("Shard#{} bound to core {}", self.id, core.id);
            }else{
                let core = CoreId{id: PHYSICAL_CORES[self.id as usize % PHYSICAL_CORES.len()] as usize};
                set_for_current(core);
                println!("Shard#{} bound to core {}", self.id, core.id);
            }
            self.run(receiver);
        });
    }

    pub fn run(mut self, receiver: Receiver<ShardMsg>){
        for msg in receiver{
            match msg {
                ShardMsg::PushTxn(txn)=>{
                    self.push_txn(txn);
                }
                ShardMsg::Deploy { code, address_index, input_data,reply} => {
                    match self.deploy(&code, address_index,&input_data) {
                        Ok(_) =>{
                            println!("Shard#{} contract deployed at addr=0x{:x}", self.id, address_index);
                        }
                        Err(e) => {
                            eprintln!("Shard#{} deploy failed: {:?}", self.id, e);
                        }
                    }
                    let _ = reply.send(self.id);
                }
                ShardMsg::Step{reply}=>{
                    let mut executed = 0;
                    while let Some(txn) = self.pop_txn() {
                        let res = self.execute(txn);
                        executed += 1;
                        self.executed_txns += 1;
                        if executed == MAX_TXN_PER_BLOCK{
                            break;
                        }
                    }
                    self.block_height += 1;
                    self.dispatch();
                    let _ = reply.send(self.id); 
                }
                ShardMsg::GetTxnSize { reply } =>{
                    let size = self.txn_size();
                    let _ = reply.send((self.id,size));
                }
                ShardMsg::GetGlobalStorage { address, slot, reply } => {
                    let val = self.db.local_get_storage(address, slot);
                    let _ = reply.send(val);
                }
                ShardMsg::GetBlockHeight{reply}=>{
                    let height = self.block_height;
                    let _ = reply.send(height);
                },
                ShardMsg::GetExecutedTxns{reply}=>{
                    let txns = self.executed_txns;
                    let _ = reply.send(txns);
                },
                ShardMsg::Stop=>{
                    break;
                },
            }
        }
    }
}



