pub mod db;
pub mod engine;
pub mod shard;
pub mod schedule;

pub type ShardId = u32;

const GLOBAL_SHARD_ID:ShardId = 65535;

const PHYSICAL_CORES: &[u32] = &[0, 2, 4, 6, 8, 10, 12, 14];