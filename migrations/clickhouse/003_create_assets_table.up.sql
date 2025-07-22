CREATE TABLE IF NOT EXISTS assets (
    defuse_asset_identifier String,
    near_token_id String,
    intents_token_id String,
    decimals UInt8,
    asset_name String,
    symbol Nullable(String),
    min_deposit_amount Nullable(String),
    min_withdrawal_amount Nullable(String),
    withdrawal_fee Nullable(String),
    standard String,
    blockchain String,
    price Nullable(Float64),
    price_updated_at Nullable(String),
    contract_address String
) ENGINE = MergeTree()
ORDER BY (defuse_asset_identifier, intents_token_id)
SETTINGS index_granularity = 8192;
