//! Schedule(Simulator) for the crystal revm

use std::{collections::VecDeque, sync::{Arc, Mutex}};

use revm::context::TxEnv;

#[derive(Debug)]
pub struct TxnPool {
    pub pending_txns: Arc<Mutex<VecDeque<TxEnv>>> ,
}

impl TxnPool {
    pub fn new() -> Self {
        Self {
            pending_txns: Arc::new(Mutex::new(VecDeque::new())) ,
        }
    }
    
    pub fn push(&mut self, txn: TxEnv) {
        self.pending_txns.lock().unwrap().push_back(txn);
    }

    pub fn pop(&mut self) -> Option<TxEnv> {
        self.pending_txns.lock().unwrap().pop_front()
    }
}