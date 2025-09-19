//! Crystality database for sharding

use std::collections::HashMap;
use std::fmt;
use std::error::Error;
use crossbeam_channel::{bounded, Sender};
use revm::{context::DBErrorMarker, primitives::{alloy_primitives::KECCAK256_EMPTY, hex, keccak256, Address, Bytes, StorageKey, StorageValue, B256, U256}, state::{Account, AccountInfo, Bytecode}, Database, DatabaseCommit};

use crate::core::{shard::ShardMsg, ShardId, GLOBAL_SHARD_ID};


#[derive(Debug)]
pub enum CrystalityDBError {
    AccountNotFound(Address),
    CodeNotFound(B256),
    StorageError(Address, StorageKey),
}

impl fmt::Display for CrystalityDBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CrystalityDBError::AccountNotFound(addr) => {
                write!(f, "Account not found: {addr:?}")
            }
            CrystalityDBError::CodeNotFound(code) => {
                write!(f, "Code not found for hash: {code:?}")
            }
            CrystalityDBError::StorageError(addr, slot) => {
                write!(f, "Storage not found: {addr:?} @ {slot:?}")
            }
        }
    }
}

impl Error for CrystalityDBError {}
impl DBErrorMarker for CrystalityDBError {}

#[derive(Debug,Default)]
pub struct CrystalityAccount{
    pub code:Vec<u8>,
    pub storage:HashMap<StorageKey,StorageValue>,
}

pub struct CrystalityDB{
    pub accounts:HashMap<Address,CrystalityAccount>,
    pub id: ShardId,
    pub global: Sender<ShardMsg>,
}

impl CrystalityDB {
    pub fn new(id: ShardId, global: Sender<ShardMsg>) -> Self {
        Self {
            accounts: HashMap::new(),
            id,
            global,
        }
    }

    #[inline]
    pub fn local_get_storage(&self, addr: Address, slot: StorageKey) -> Option<U256> {
        self.accounts
            .get(&addr)
            .and_then(|acc| acc.storage.get(&slot).copied())
    }

    #[inline]
    pub fn fetch_from_global(&self, addr: Address, slot: StorageKey) -> Option<U256> {
        if self.id == GLOBAL_SHARD_ID {
            return self.local_get_storage(addr, slot);
        }
        let (tx, rx) = bounded::<Option<U256>>(1);
        let _ = self.global.send(ShardMsg::GetGlobalStorage { address: addr, slot, reply: tx });
        rx.recv().ok().flatten()
    }
}

impl Database for CrystalityDB{
    
    type Error = CrystalityDBError;

    fn basic(&mut self,address:Address) -> Result<Option<AccountInfo> ,Self::Error>  {
        // println!("basic => address:{:?}",address);
        Ok(self.accounts.get(&address).map(|acc| {
            let code_hash = if acc.code.is_empty() {
                KECCAK256_EMPTY
            } else {
                keccak256(&acc.code)
            };

            AccountInfo {
                nonce: 0,
                balance: U256::ZERO,
                code_hash,
                code: Some(Bytecode::new_raw(Bytes::from(acc.code.clone()))),
            }
        }))
    }

    fn code_by_hash(&mut self,code_hash:B256) -> Result<Bytecode,Self::Error>  {
        Ok(Bytecode::new_raw(Bytes::new()))
    }

    fn storage(&mut self,address:Address,index:StorageKey) -> Result<StorageValue,Self::Error>  {
        if let Some(v) = self.local_get_storage(address, index) {
            Ok(v)
        } else if let Some(v) = self.fetch_from_global(address, index) {
            Ok(v)
        } else {
            Ok(U256::ZERO) 
        }     
    }

    fn block_hash(&mut self,number:u64) -> Result<B256,Self::Error>  {
        Ok(B256::default())
    }
}

impl DatabaseCommit for CrystalityDB {
    fn commit(&mut self, changes: HashMap<Address, Account>) {
        for (addr, account) in changes {
            for (slot, value) in account.storage {
                if value.is_changed(){
                    // println!("set storage => address:{:?}, StorageKey:{},Value:{}",addr,slot,value.present_value());
                    self.accounts
                        .entry(addr)
                        .or_default()
                        .storage
                        .insert(slot, value.present_value());
                }
            
            }
        }
    }
}