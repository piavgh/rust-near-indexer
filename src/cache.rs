pub mod receipts_cache;

use near_lake_framework::near_indexer_primitives::CryptoHash;

pub const CACHE_SIZE: usize = 10000;
pub const CACHE_EXPIRATION_BLOCKS: u64 = 50;

pub type ParentTransactionHashString = String;

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum ReceiptOrDataId {
    ReceiptId(CryptoHash),
}
