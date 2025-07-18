-- Create the swaps table
CREATE TABLE IF NOT EXISTS swaps
(
    intent_hash         VARCHAR(64) NOT NULL,
    origin_asset        TEXT NOT NULL,
    destination_asset   TEXT NOT NULL,
    amount_in           TEXT NOT NULL,
    amount_out          TEXT NOT NULL,
    recipient           TEXT NOT NULL,
    tx_hash             TEXT,
    
    -- Primary key
    PRIMARY KEY (intent_hash)
);

-- Create indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_swaps_origin_asset 
    ON swaps USING HASH (origin_asset);

CREATE INDEX IF NOT EXISTS idx_swaps_destination_asset 
    ON swaps USING HASH (destination_asset);

CREATE INDEX IF NOT EXISTS idx_swaps_recipient 
    ON swaps USING HASH (recipient);

-- Add comments to document the schema
COMMENT ON TABLE swaps IS 'Swap transactions extracted from NEAR blockchain events';
COMMENT ON COLUMN swaps.intent_hash IS 'The unique identifier for the swap intent';
COMMENT ON COLUMN swaps.origin_asset IS 'The asset being swapped from';
COMMENT ON COLUMN swaps.destination_asset IS 'The asset being swapped to';
COMMENT ON COLUMN swaps.amount_in IS 'The amount of origin asset';
COMMENT ON COLUMN swaps.amount_out IS 'The amount of destination asset';
COMMENT ON COLUMN swaps.recipient IS 'The recipient address of the swap';
COMMENT ON COLUMN swaps.tx_hash IS 'The transaction hash associated with this swap';
