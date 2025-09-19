pub mod db;
pub mod engine;
pub mod shard;
pub mod schedule;

pub type ShardId = u32;

const GLOBAL_SHARD_ID:ShardId = 65535;

const PHYSICAL_CORES: &[u32] = &[0, 2, 4, 6, 8, 10, 12, 14];

const RELAY_TO_GLOBAL:u64 = 888;
const RELAY_TO_ADDRESS:u64 = 666;
const RELAY_TO_SHARDS:u64 = 777;

const MAX_TXN_PER_BLOCK:u32 = 1000;