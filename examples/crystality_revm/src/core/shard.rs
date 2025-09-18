//! shard

use std::sync::Arc;

use revm::{context::{result::{EVMError, ExecutionResult}, BlockEnv, CfgEnv, TxEnv}, Database};

use crate::core::{db::CrystalityDB, engine::{self, EvmExecuteEngine}, schedule::TxnPool, ShardId};


#[derive(Default)]
pub struct ShardRouter;

impl ShardRouter {
    pub fn new()->Self{
        Self
    }
}

pub struct Shard{
    pub id:ShardId,
    pub db:CrystalityDB,
    pub cfg:CfgEnv,
    pub block:BlockEnv,
    pub engine:EvmExecuteEngine,
    pub txn_pool: TxnPool,
}

impl Shard {
    pub fn new(id:ShardId, cfg:CfgEnv, block:BlockEnv, router: Arc<ShardRouter>)->Self{
        let db = CrystalityDB::default();
        let engine = EvmExecuteEngine::new(router);
        let txn_pool = TxnPool::new();
        Self{
            id,
            db,
            cfg,
            block,
            engine,
            txn_pool
        }
    }

    pub fn push_txn(&mut self, txn: TxEnv) {
        self.txn_pool.push(txn);
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
}