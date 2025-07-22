CREATE TABLE IF NOT EXISTS assets (
    defuse_asset_identifier TEXT NOT NULL,
    near_token_id TEXT NOT NULL,
    intents_token_id TEXT NOT NULL,
    decimals SMALLINT NOT NULL,
    asset_name TEXT NOT NULL,
    symbol TEXT,
    min_deposit_amount TEXT,
    min_withdrawal_amount TEXT,
    withdrawal_fee TEXT,
    standard TEXT NOT NULL,
    blockchain TEXT NOT NULL,
    price DOUBLE PRECISION,
    price_updated_at TIMESTAMPTZ,
    contract_address TEXT NOT NULL,
    
    PRIMARY KEY (defuse_asset_identifier, intents_token_id)
);

CREATE INDEX IF NOT EXISTS idx_assets_intents_token_id ON assets(intents_token_id); 
