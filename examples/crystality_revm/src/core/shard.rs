//! shard

use std::{collections::VecDeque, sync::{Arc, Mutex}, thread};
use core_affinity::{set_for_current, CoreId};
use crossbeam_channel::{Sender,Receiver,unbounded};

use revm::{context::{result::{EVMError, ExecutionResult}, BlockEnv, CfgEnv, TxEnv}, Database};

use crate::core::{db::{CrystalityDB, CrystalityDBError}, engine::{self, EvmExecuteEngine}, ShardId, GLOBAL_SHARD_ID, PHYSICAL_CORES};


#[derive(Default)]
pub struct ShardRouter;

impl ShardRouter {
    pub fn new()->Self{
        Self
    }
}


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
    Stop,
}

pub struct Shard{
    pub id:ShardId,
    pub db:CrystalityDB,
    pub cfg:CfgEnv,
    pub block:BlockEnv,
    pub engine:EvmExecuteEngine,
    pub pending_txns: Mutex<VecDeque<TxEnv>>,
    pub relay_emits: VecDeque<TxEnv>,
}

impl Shard {
    pub fn new(id:ShardId, cfg:CfgEnv, block:BlockEnv, router: Arc<ShardRouter>)->Self{
        let db = CrystalityDB::default();
        let engine = EvmExecuteEngine::new(router);
        Self{
            id,
            db,
            cfg,
            block,
            engine,
            pending_txns:Mutex::new(VecDeque::new()),
            relay_emits: VecDeque::new(),
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

    pub fn init(
        mut self,
    ) -> Sender<ShardMsg> {
        let (sender, receiver) = unbounded::<ShardMsg>();
        println!("Shard#{} init", self.id);
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
        sender
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
                    while let Some(txn) = self.pop_txn() {
                        let res = self.execute(txn);
                        // println!("Shard#{} executed txn => {:?}", self.id, res);
                    }
                    let _ = reply.send(self.id); 
                }
                ShardMsg::GetTxnSize { reply } =>{
                    let size = self.txn_size();
                    let _ = reply.send((self.id,size));
                }
                ShardMsg::Stop=>{
                    break;
                },
            }
        }
    }
}