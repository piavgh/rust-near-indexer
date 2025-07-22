-- Create the events table based on the ClickHouse schema
CREATE TABLE IF NOT EXISTS events
(
    block_height                     UInt64 COMMENT 'The height of the block',
    block_timestamp                  DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
    block_hash                       String COMMENT 'The hash of the block',
    contract_id                      String COMMENT 'The ID of the account on which the execution outcome happens',
    execution_status                 String COMMENT 'The execution outcome status',
    version                          String COMMENT 'The event version',
    standard                         String COMMENT 'The event standard',
    index_in_log                     UInt64 COMMENT 'The index of the event in the execution outcome log',
    event                            String COMMENT 'The event type',
    data                             String COMMENT 'The event JSON data',
    related_receipt_id               String COMMENT 'The execution outcome receipt ID',
    related_receipt_receiver_id      String COMMENT 'The destination account ID',
    related_receipt_predecessor_id   String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',
    tx_hash                          Nullable(String) COMMENT 'The transaction hash',

    INDEX            block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX            contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
    INDEX            related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
    INDEX            related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1,
    INDEX            tx_hash_bloom_index tx_hash TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree
PRIMARY KEY (block_height, related_receipt_id, index_in_log)
ORDER BY (block_height, related_receipt_id, index_in_log); 
