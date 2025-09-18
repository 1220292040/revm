//! Crystality execution engine

use std::{io::Read, sync::Arc};

use revm::{
    context::{
        result::{EVMError, ExecutionResult, Output}, BlockEnv, CfgEnv, Context, ContextTr, Database, TxEnv
    }, interpreter::{CallInputs, CallOutcome, Interpreter}, primitives::{keccak256, Address, Bytes, TxKind, U256}, state::{AccountInfo, Bytecode}, InspectCommitEvm, Inspector, MainBuilder, MainContext
};

use crate::{
    codec::encoder::addr_from_u64,
    core::{
        db::{CrystalityAccount, CrystalityDB}, 
        shard::ShardRouter
    }
};

#[derive(Clone)]
pub struct CrystalityInspector{
    router: Arc<ShardRouter>,
}

pub struct EvmExecuteEngine{
    router: Arc<ShardRouter>,
}

/** 
 * CrystalityInspector
*/
impl CrystalityInspector {
    pub fn new(router: Arc<ShardRouter>)->Self{
        Self{
            router
        }
    }
}

impl<CTX> Inspector<CTX> for CrystalityInspector
where CTX:ContextTr
{
    #[inline]
    fn call(&mut self,context: &mut CTX,inputs: &mut CallInputs) -> Option<CallOutcome>{
        println!("call");
        None
    }
}

/**
 * EvmExecuteEngine
 */
impl EvmExecuteEngine {
    pub fn new(router: Arc<ShardRouter>)->Self{
        Self{
            router
        }
    }

    pub fn execute(
        &self, 
        db:&mut CrystalityDB, 
        cfg: &CfgEnv, 
        block: &BlockEnv, 
        tx: TxEnv
    )->Result<ExecutionResult, EVMError<<CrystalityDB as Database>::Error>>{
        let ctx = Context::mainnet()
            .with_cfg(cfg.clone())
            .with_block(block.clone())
            .with_db(db);
        
        let insp = CrystalityInspector::new(self.router.clone());
        let mut evm = ctx.build_mainnet_with_inspector(insp);
        
        evm.inspect_tx_commit(tx)

    }

    pub fn deploy(
        &self, 
        db:&mut CrystalityDB, 
        cfg: &CfgEnv, 
        block: &BlockEnv, 
        code: &[u8],
        address_index: u64,
        input_data: &[u8],
    )->Result<ExecutionResult, EVMError<<CrystalityDB as Database>::Error>>{
        let contract_address = addr_from_u64(address_index);

        let mut data = Vec::with_capacity(code.len() + input_data.len());
        data.extend_from_slice(code);
        data.extend_from_slice(input_data);
        let deploy_data = Bytes::from(data);

        let tx = TxEnv::builder()
            .caller(addr_from_u64(0))
            .kind(TxKind::Create)
            .data(deploy_data)
            .build()
            .unwrap();

        let res = self.execute(db, cfg, block, tx);

        if let Ok(success) = &res {
            if let  Some(code_bytes) = success.output() {

                let code_hash = keccak256(code_bytes);
                let code = code_bytes.clone();
                db.accounts.insert(
                    contract_address,
                    CrystalityAccount {
                        code: code.to_vec(),
                        storage: Default::default(),
                    },
                );

            }
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use revm::context::{BlockEnv, CfgEnv, TxEnv};
    use revm::primitives::{Bytes, TxKind, Address};
    
    #[test]
    fn test_execute_engine_runs() {
        let router = Arc::new(ShardRouter::default());
        let engine = EvmExecuteEngine::new(router);
        let mut db = CrystalityDB::default();
        let cfg = CfgEnv::default();
        let block = BlockEnv::default();
        let tx = TxEnv::default();

        let res = engine.execute(&mut db, &cfg, &block, tx);

        assert!(
            res.is_ok(),
            "execution should succeed but got error: {:?}",
            res
        );
    }

    #[test]
    fn test_inspector_call_is_triggered() {
        let router = Arc::new(ShardRouter::default());
        let engine = EvmExecuteEngine::new(router);

        let mut db = CrystalityDB::default();
        let cfg = CfgEnv::default();
        let block = BlockEnv::default();

        let bytecodes = Bytes::from(vec![
            0x60, 0x00,       // PUSH1 0x00 (out size)
            0x60, 0x00,       // PUSH1 0x00 (out offset)
            0x60, 0x00,       // PUSH1 0x00 (in size)
            0x60, 0x00,       // PUSH1 0x00 (in offset)
            0x60, 0x00,       // PUSH1 0x00 (value)
            0x61, 0x01, 0x23, // PUSH2 0x0123 (target address 0x123)
            0x61, 0xff, 0xff, // PUSH2 0xffff (gas)
            0xf1,             // CALL
            0x60, 0x00,       // PUSH1 0x00 (out size)
            0x60, 0x00,       // PUSH1 0x00 (out offset)
            0x60, 0x00,       // PUSH1 0x00 (in size)
            0x60, 0x00,       // PUSH1 0x00 (in offset)
            0x60, 0x00,       // PUSH1 0x00 (value)
            0x61, 0x03, 0x23, // PUSH2 0x0123 (target address 0x123)
            0x61, 0xff, 0xff, // PUSH2 0xffff (gas)
            0xf1,             // CALL
            0x00,             // STOP
        ]);
        
        let tx = TxEnv::builder()
            .caller(addr_from_u64(0x1001))
            .kind(TxKind::Create)
            .data(bytecodes.clone())
            .build()
            .unwrap();
        
        let res = engine.execute(&mut db, &cfg, &block, tx);
        println!("res: {:?}",res);

    }

    #[test]
    fn test_deploy() {
        use revm::primitives::hex;
        let router = Arc::new(ShardRouter::default());
        let engine = EvmExecuteEngine::new(router);

        let mut db = CrystalityDB::default();
        let cfg = CfgEnv::default();
        let block = BlockEnv::default();

        let code = "608060405234801561001057600080fd5b506040516101683803806101688339818101604052810190610032919061007a565b80600081905550506100a7565b600080fd5b6000819050919050565b61005781610044565b811461006257600080fd5b50565b6000815190506100748161004e565b92915050565b6000602082840312156100905761008f61003f565b5b600061009e84828501610065565b91505092915050565b60b3806100b56000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c8063e582dd3114602d575b600080fd5b60336047565b604051603e91906064565b60405180910390f35b60005481565b6000819050919050565b605e81604d565b82525050565b6000602082019050607760008301846057565b9291505056fea2646970667358221220c40df85484e68a9cc01f8123118d7d88d684712917822dd52e1f58d0a8339ec764736f6c63430008120033";
        let code = hex::decode(code).unwrap();

        // (uint256=42）
        let input_data: Vec<u8> = vec![0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                                       0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,42];

        let res = engine.deploy(
            &mut db,
            &cfg,
            &block,
            &code,
            1,
            &input_data,
        );

        assert!(res.is_ok(), "Deploy transaction should succeed");
        let account = db.accounts.get(&addr_from_u64(1)).unwrap();
        println!("code of account#{} => {:?}",addr_from_u64(1), hex::encode(account.code.clone()));

    }
}