//! Crystality execution engine
use crossbeam_channel::Sender;
use revm::{
    context::{
        result::{EVMError, ExecutionResult}, BlockEnv, CfgEnv, Context, ContextTr, Database, LocalContextTr, TxEnv
    }, interpreter::{CallInput, CallInputs, CallOutcome, Gas, InstructionResult, InterpreterResult}, primitives::{Address, Bytes, TxKind}, InspectCommitEvm, Inspector, MainBuilder, MainContext
};

use crate::{
    codec::encoder::addr_from_u64,
    core::{
        db::{CrystalityAccount, CrystalityDB}, shard::RelayEmission, RELAY_TO_ADDRESS, RELAY_TO_GLOBAL, RELAY_TO_SHARDS
    }
};

#[derive(Clone)]
pub struct CrystalityInspector{
    pub relay_emits: Sender<RelayEmission>,
    pub origin: Address,
}

/** 
 * CrystalityInspector
*/
impl CrystalityInspector {
    pub fn new(relay_emits:  Sender<RelayEmission>, origin:Address)->Self{
        Self{
            relay_emits,
            origin,
        }
    }
}

#[inline]
fn calldata_as_bytes<CTX: ContextTr>(ctx: &mut CTX, input: &CallInput) -> Bytes {
    match input {
        CallInput::Bytes(b) => b.clone(),
        CallInput::SharedBuffer(r) => {
            if r.is_empty() {
                return Bytes::new();
            }
            match ctx.local().shared_memory_buffer_slice(r.clone()) {
                Some(buf_ref) => {
                    Bytes::copy_from_slice(&*buf_ref)
                }
                None => {
                    Bytes::new()
                }
            }
        }
    }
}
impl<CTX> Inspector<CTX> for CrystalityInspector
where CTX:ContextTr
{
     #[inline]
    fn call(&mut self, ctx: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
        if inputs.bytecode_address == addr_from_u64(RELAY_TO_GLOBAL)
            || inputs.bytecode_address == addr_from_u64(RELAY_TO_ADDRESS)
            || inputs.bytecode_address == addr_from_u64(RELAY_TO_SHARDS)
        {
            let data = calldata_as_bytes(ctx, &inputs.input);
            
            let relay_tx = TxEnv::builder()
                .caller(inputs.caller)
                .kind(TxKind::Call(inputs.bytecode_address))
                .data(data)
                .build()
                .unwrap();

            let _ = self.relay_emits.send(RelayEmission{txn:relay_tx, origin:self.origin});

            let result = InterpreterResult::new(InstructionResult::Return, Bytes::new(), Gas::new(0));
            return Some(CallOutcome { result, memory_offset: 0..0 });
        }
        None
    }
}

pub struct EvmExecuteEngine{
    pub relay_emits: Sender<RelayEmission>,
}
/**
 * EvmExecuteEngine
 */
impl EvmExecuteEngine {
    pub fn new(relay_emits: Sender<RelayEmission>)->Self{
        Self{
            relay_emits
        }
    }

    #[inline]
    fn is_eoa(db: &CrystalityDB, addr: Address) -> bool {
        db.accounts
            .get(&addr)
            .map(|acc| acc.code.is_empty())
            .unwrap_or(true)
    }

    pub fn execute(
        &self, 
        db:&mut CrystalityDB, 
        cfg: &CfgEnv, 
        block: &BlockEnv, 
        tx: TxEnv
    )->Result<ExecutionResult, EVMError<<CrystalityDB as Database>::Error>>{
        let origin = if Self::is_eoa(db, tx.caller) {
            tx.caller
        } else {
            Address::ZERO
        };

        let ctx = Context::mainnet()
            .with_cfg(cfg.clone())
            .with_block(block.clone())
            .with_db(db);

        let insp = CrystalityInspector::new(self.relay_emits.clone(), origin);
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