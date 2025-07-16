-- Create the events table based on the ClickHouse schema
CREATE TABLE IF NOT EXISTS events
(
    block_height                     BIGINT NOT NULL,
    block_timestamp                  TIMESTAMP WITH TIME ZONE NOT NULL,
    block_hash                       VARCHAR(64) NOT NULL,
    contract_id                      TEXT NOT NULL,
    execution_status                 VARCHAR(50) NOT NULL,
    version                          VARCHAR(10) NOT NULL,
    standard                         VARCHAR(20) NOT NULL,
    index_in_log                     BIGINT NOT NULL,
    event                            TEXT NOT NULL,
    data                             TEXT NOT NULL,
    related_receipt_id               VARCHAR(64) NOT NULL,
    related_receipt_receiver_id      TEXT NOT NULL,
    related_receipt_predecessor_id   TEXT NOT NULL,
    tx_hash                          VARCHAR(64),

    -- Primary key constraint equivalent to ClickHouse PRIMARY KEY
    PRIMARY KEY (block_height, related_receipt_id, index_in_log)
);

-- Create indexes for performance optimization equivalent to ClickHouse indexes
CREATE INDEX IF NOT EXISTS idx_events_block_timestamp 
    ON events (block_timestamp);

CREATE INDEX IF NOT EXISTS idx_events_contract_id 
    ON events USING HASH (contract_id);

CREATE INDEX IF NOT EXISTS idx_events_related_receipt_id 
    ON events USING HASH (related_receipt_id);

CREATE INDEX IF NOT EXISTS idx_events_related_receipt_receiver_id 
    ON events USING HASH (related_receipt_receiver_id);

-- Create additional indexes for common queries
CREATE INDEX IF NOT EXISTS idx_events_block_height 
    ON events (block_height DESC); -- Optimize for querying last block height

CREATE INDEX IF NOT EXISTS idx_events_tx_hash 
    ON events (tx_hash) WHERE tx_hash IS NOT NULL;

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
