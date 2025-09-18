//! Crystality database for sharding

use std::collections::HashMap;
use std::fmt;
use std::error::Error;
use revm::{context::DBErrorMarker, primitives::{alloy_primitives::KECCAK256_EMPTY, hex, keccak256, Address, Bytes, StorageKey, StorageValue, B256, U256}, state::{Account, AccountInfo, Bytecode}, Database, DatabaseCommit};


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

#[derive(Debug,Default)]
pub struct CrystalityDB{
    pub accounts:HashMap<Address,CrystalityAccount>,
}

impl Database for CrystalityDB{
    
    type Error = CrystalityDBError;

    fn basic(&mut self,address:Address) -> Result<Option<AccountInfo> ,Self::Error>  {
        println!("basic => address:{:?}",address);
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
        Ok(self
            .accounts
            .get(&address)
            .and_then(|acc| acc.storage.get(&index).copied())
            .unwrap_or(StorageValue::ZERO))        
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
                    println!("set storage => address:{:?}, StorageKey:{},Value:{}",addr,slot,value.present_value());
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