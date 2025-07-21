-- Create the swaps table with ClickHouse-specific syntax
CREATE TABLE IF NOT EXISTS swaps
(
    intent_hash         String COMMENT 'The unique identifier for the swap intent',
    origin_asset        String COMMENT 'The asset being swapped from',
    destination_asset   String COMMENT 'The asset being swapped to',
    amount_in           UInt128 COMMENT 'The amount of origin asset in smallest units (wei)',
    amount_out          UInt128 COMMENT 'The amount of destination asset in smallest units (wei)',
    recipient           String COMMENT 'The recipient address of the swap',
    tx_hash             Nullable(String) COMMENT 'The transaction hash associated with this swap',

    INDEX            origin_asset_bloom_index origin_asset TYPE bloom_filter() GRANULARITY 1,
    INDEX            destination_asset_bloom_index destination_asset TYPE bloom_filter() GRANULARITY 1,
    INDEX            recipient_bloom_index recipient TYPE bloom_filter() GRANULARITY 1
)
ENGINE = MergeTree()
PRIMARY KEY (intent_hash)
ORDER BY (intent_hash); 
