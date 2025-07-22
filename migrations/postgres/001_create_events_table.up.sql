-- Create the events table based on the ClickHouse schema
CREATE TABLE IF NOT EXISTS events
(
    block_height                     BIGINT NOT NULL,
    block_timestamp                  TIMESTAMP WITH TIME ZONE NOT NULL,
    block_hash                       TEXT NOT NULL,
    contract_id                      TEXT NOT NULL,
    execution_status                 TEXT NOT NULL,
    version                          TEXT NOT NULL,
    standard                         TEXT NOT NULL,
    index_in_log                     BIGINT NOT NULL,
    event                            TEXT NOT NULL,
    data                             TEXT NOT NULL,
    related_receipt_id               TEXT NOT NULL,
    related_receipt_receiver_id      TEXT NOT NULL,
    related_receipt_predecessor_id   TEXT NOT NULL,
    tx_hash                          TEXT,

    -- Primary key constraint equivalent to ClickHouse PRIMARY KEY
    PRIMARY KEY (block_height, related_receipt_id, index_in_log)
);

-- Create indexes for performance optimization based on actual query patterns

-- B-tree index optimized for MAX(block_height) queries in get_last_height()
CREATE INDEX IF NOT EXISTS idx_events_block_height 
    ON events (block_height DESC);

-- Partial B-tree index for transaction hash lookups - critical for get_events() WHERE tx_hash = ? queries
-- Uses partial index to save space since many events may not have tx_hash
CREATE INDEX IF NOT EXISTS idx_events_tx_hash 
    ON events (tx_hash) WHERE tx_hash IS NOT NULL;

-- Composite index for ORDER BY block_height DESC, index_in_log DESC in get_events() queries
CREATE INDEX IF NOT EXISTS idx_events_order_by 
    ON events (block_height DESC, index_in_log DESC);

-- Add comments to document the schema
COMMENT ON TABLE events IS 'NEAR blockchain events indexed from transaction receipts';
COMMENT ON COLUMN events.block_height IS 'The height of the block';
COMMENT ON COLUMN events.block_timestamp IS 'The timestamp of the block in UTC';
COMMENT ON COLUMN events.block_hash IS 'The hash of the block';
COMMENT ON COLUMN events.contract_id IS 'The ID of the account on which the execution outcome happens';
COMMENT ON COLUMN events.execution_status IS 'The execution outcome status';
COMMENT ON COLUMN events.version IS 'The event version';
COMMENT ON COLUMN events.standard IS 'The event standard';
COMMENT ON COLUMN events.index_in_log IS 'The index of the event in the execution outcome log';
COMMENT ON COLUMN events.event IS 'The event type';
COMMENT ON COLUMN events.data IS 'The event JSON data';
COMMENT ON COLUMN events.related_receipt_id IS 'The execution outcome receipt ID';
COMMENT ON COLUMN events.related_receipt_receiver_id IS 'The destination account ID';
COMMENT ON COLUMN events.related_receipt_predecessor_id IS 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system';
COMMENT ON COLUMN events.tx_hash IS 'The transaction hash';
