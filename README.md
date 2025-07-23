# NEAR Intents Indexer

A Rust-based blockchain indexer for NEAR Protocol that processes DeFi events from the Near Intents protocol and stores
them in ClickHouse or PostgreSQL databases. The indexer focuses on NEP-245 (multi-token) and DIP-4 (intent) standards
with comprehensive asset management and graceful shutdown coordination.

## Features

- **Dual CLI Interface**: Separate `indexer` and `api` commands for different operational modes
- **Flexible Data Sources**: Support for both NEAR Lake Framework and neardata.xyz API
- **Multiple Storage Backends**: Interchangeable ClickHouse and PostgreSQL support
- **Asset Management**: Automatic fetching and caching of token metadata with withdrawal fees
- **Graceful Shutdown**: Coordinated shutdown handling with data persistence
- **Advanced Configuration**: YAML and environment variable support with Figment
- **Resilient Processing**: Comprehensive retry logic with exponential backoff
- **Multi-level Caching**: In-memory and Redis caching for transaction hash mapping
- **HTTP API**: RESTful endpoints for querying indexed data

## Requirements

1. [Rust](https://www.rust-lang.org/tools/install) (1.76.0+ recommended)
2. Database: [ClickHouse](https://clickhouse.com/docs/en/quick-start#self-managed-install)
   or [PostgreSQL](https://www.postgresql.org/)
3. [Redis](https://redis.io/) (optional, for transaction caching)
4. Configuration file (YAML format)

## Quick Start

### 1. Build the Project

```bash
cargo build --release
```

### 2. Configuration

Create a configuration file (e.g., `config/local.yaml`):

```yaml
env: development

# Indexer settings
indexer:
  enabled: true
  block_height: 156133097
  data_source:
    source_type: "neardata_api"  # or "lake_framework"
    neardata_api:
      base_url: "https://mainnet.neardata.xyz"
      timeout_seconds: 30
      max_requests_per_second: 8.0
      poll_interval_ms: 200
      max_retries: 3

# Storage backend (choose one)
storage:
  backend: "postgres"  # or "clickhouse"
  postgres:
    connection_string: "postgresql://user:password@localhost/indexer"
  clickhouse:
    url: "http://localhost:18123"
    user: "default"
    password: ""
    database: "mainnet"

# Asset worker for token metadata
asset_worker:
  interval: "20s"
  timeout: "10s"
  max_retries: 3
  mm_supported_tokens_url: "https://bridge.chaindefuser.com/rpc"
  intents_supported_tokens_url: "https://1click.chaindefuser.com/v0/tokens"

# HTTP API server
http:
  bind_address: "0.0.0.0:3000"
  prefix: "/api/v1"

# Graceful shutdown
shutdown:
  shutdown_timeout: "20s"

# Caching
cache:
  redis_url: "redis://127.0.0.1:6379"
```

### 3. Environment Variables

The application supports two types of environment variables:

#### Manual Environment Variable Mappings (Preferred)

These variables are directly mapped and take precedence over `APP__` prefixed variables:

```bash
# Storage Backend
export STORAGE_BACKEND="postgres"  # or "clickhouse"

# PostgreSQL Configuration
export POSTGRES_CONNECTION_STRING="postgresql://user:password@localhost/indexer"

# ClickHouse Configuration  
export CLICKHOUSE_URL="http://localhost:18123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD=""
export CLICKHOUSE_DB="mainnet"

# HTTP Server
export HTTP_BIND_ADDRESS="0.0.0.0:3000"
export HTTP_PREFIX="/api/v1"

# Redis (optional)
export REDIS_URL="redis://127.0.0.1:6379"
```

#### APP__ Prefixed Environment Variables

Alternative configuration using hierarchical structure with `APP__` prefix:

```bash
# Application Environment
export APP__ENV="production"

# Indexer Configuration
export APP__INDEXER__ENABLED=true
export APP__INDEXER__BLOCK_HEIGHT=156133097

# Data Source Configuration
export APP__INDEXER__DATA_SOURCE__SOURCE_TYPE="neardata_api"  # or "lake_framework"

# NearData API Configuration (when using neardata_api)
export APP__INDEXER__DATA_SOURCE__NEARDATA_API__BASE_URL="https://mainnet.neardata.xyz"
export APP__INDEXER__DATA_SOURCE__NEARDATA_API__TIMEOUT_SECONDS=30
export APP__INDEXER__DATA_SOURCE__NEARDATA_API__MAX_REQUESTS_PER_SECOND=8.0
export APP__INDEXER__DATA_SOURCE__NEARDATA_API__POLL_INTERVAL_MS=200
export APP__INDEXER__DATA_SOURCE__NEARDATA_API__MAX_RETRIES=3

# Storage Configuration
export APP__STORAGE__BACKEND="postgres"
export APP__STORAGE__POSTGRES__CONNECTION_STRING="postgresql://user:password@localhost/indexer"
# OR for ClickHouse:
export APP__STORAGE__CLICKHOUSE__URL="http://localhost:18123"
export APP__STORAGE__CLICKHOUSE__USER="default"
export APP__STORAGE__CLICKHOUSE__PASSWORD=""
export APP__STORAGE__CLICKHOUSE__DATABASE="mainnet"

# HTTP Server Configuration
export APP__HTTP__BIND_ADDRESS="0.0.0.0:3000"
export APP__HTTP__PREFIX="/api/v1"

# Asset Worker Configuration
export APP__ASSET_WORKER__INTERVAL="20s"
export APP__ASSET_WORKER__TIMEOUT="10s"
export APP__ASSET_WORKER__MAX_RETRIES=3
export APP__ASSET_WORKER__MM_SUPPORTED_TOKENS_URL="https://bridge.chaindefuser.com/rpc"
export APP__ASSET_WORKER__INTENTS_SUPPORTED_TOKENS_URL="https://1click.chaindefuser.com/v0/tokens"

# Graceful Shutdown Configuration
export APP__SHUTDOWN__SHUTDOWN_TIMEOUT="20s"

# Cache Configuration
export APP__CACHE__REDIS_URL="redis://127.0.0.1:6379"

# AWS Credentials (required for NEAR Lake Framework)
export AWS_ACCESS_KEY_ID="your-aws-access-key"
export AWS_SECRET_ACCESS_KEY="your-aws-secret-key"

# Logging
export RUST_LOG="info"  # trace, debug, info, warn, error
```

#### Environment Variable Reference

| Category        | Variable                                 | Config Field                         | Purpose                            | Default                |
|-----------------|------------------------------------------|--------------------------------------|------------------------------------|------------------------|
| **Storage**     | `STORAGE_BACKEND`                        | `storage.backend`                    | Backend type (postgres/clickhouse) | Required               |
|                 | `POSTGRES_CONNECTION_STRING`             | `storage.postgres.connection_string` | PostgreSQL connection string       | Required if postgres   |
|                 | `CLICKHOUSE_URL`                         | `storage.clickhouse.url`             | ClickHouse server URL              | Required if clickhouse |
|                 | `CLICKHOUSE_USER`                        | `storage.clickhouse.user`            | ClickHouse username                | Required if clickhouse |
|                 | `CLICKHOUSE_PASSWORD`                    | `storage.clickhouse.password`        | ClickHouse password                | Required if clickhouse |
|                 | `CLICKHOUSE_DB`                          | `storage.clickhouse.database`        | ClickHouse database name           | Required if clickhouse |
| **HTTP**        | `HTTP_BIND_ADDRESS`                      | `http.bind_address`                  | Server bind address                | `0.0.0.0:3000`         |
|                 | `HTTP_PREFIX`                            | `http.prefix`                        | API prefix path                    | `/api/v1`              |
| **Cache**       | `REDIS_URL`                              | `cache.redis_url`                    | Redis connection URL               | Optional               |
| **Indexer**     | `APP__INDEXER__BLOCK_HEIGHT`             | `indexer.block_height`               | Starting block height              | Required               |
|                 | `APP__INDEXER__ENABLED`                  | `indexer.enabled`                    | Enable indexer                     | `true`                 |
| **Data Source** | `APP__INDEXER__DATA_SOURCE__SOURCE_TYPE` | `indexer.data_source.source_type`    | Source type                        | `lake_framework`       |
| **AWS**         | `AWS_ACCESS_KEY_ID`                      | -                                    | AWS access key (Lake Framework)    | Required for Lake      |
|                 | `AWS_SECRET_ACCESS_KEY`                  | -                                    | AWS secret key (Lake Framework)    | Required for Lake      |
| **Logging**     | `RUST_LOG`                               | -                                    | Log level                          | `info`                 |

**Notes:**

- Manual mappings (e.g., `CLICKHOUSE_URL`) take precedence over `APP__` prefixed variables
- Duration values use humantime format (e.g., "20s", "5m", "1h")
- Storage backend determines which credentials are required
- Data source type determines whether AWS or NearData API config is needed

### 4. Running the Application

#### Run the Indexer

```bash
# Using configuration file
cargo run --release -- -c config/local.yaml indexer
```

#### Run the API Server

```bash
cargo run --release -- -c config/local.yaml api
```

## CLI Commands

The application provides two main commands:

### `indexer`

Processes blockchain events and stores them in the database.

```bash
near-intents-indexer -c config/local.yaml indexer
```

### `api`

Starts the HTTP API server for querying indexed data.

```bash
near-intents-indexer -c config/local.yaml api
```

**CLI Options:**

- `-c, --config <FILE>`: Path to configuration file (YAML format)
- `-h, --help`: Show help information

## Data Sources

### 1. neardata.xyz API (Recommended)

- HTTP API-based data source with rate limiting
- More cost-efficient than NEAR Lake Framework
- Automatic retries and polling for new blocks

```yaml
data_source:
  source_type: "neardata_api"
  neardata_api:
    base_url: "https://mainnet.neardata.xyz"
    timeout_seconds: 30
    max_requests_per_second: 8.0
    poll_interval_ms: 200
    max_retries: 3
```

### 2. NEAR Lake Framework

- AWS S3-based NEAR Lake data (requires AWS credentials)
- Direct stream from NEAR's official data lake
- Requester pay AWS pricing model, so we need money to use it

```yaml
data_source:
  source_type: "lake_framework"
```

## Storage Backends

### PostgreSQL (Default)

Traditional relational database option (more optimized for the current use case):

```yaml
storage:
  backend: "postgres"
  postgres:
    connection_string: "postgresql://user:password@localhost/indexer"
```

### ClickHouse

Optimized for analytics and large-scale data processing:

```yaml
storage:
  backend: "clickhouse"
  clickhouse:
    url: "http://localhost:18123"
    user: "default"
    password: ""
    database: "mainnet"
```

## Asset Management

The indexer includes an asset worker that automatically fetches and caches token metadata:

**Features:**

- Periodic fetching from Market Maker and Intents APIs
- In-memory caching with database persistence
- Withdrawal fee lookup by token ID
- Graceful shutdown with data flushing

**Configuration:**

```yaml
asset_worker:
  interval: "20s"
  mm_supported_tokens_url: "https://bridge.chaindefuser.com/rpc"
  intents_supported_tokens_url: "https://1click.chaindefuser.com/v0/tokens"
```

## HTTP API Endpoints

When running in `api` mode, the following endpoints are available:

- `GET /health/ready` - Readiness check
- `GET /health/live` - Liveness check
- `GET /api/v1/events` - Query events with filtering
- `GET /api/v1/swaps/{intent_hash}` - Get swap by intent hash

## Docker Development

Use the provided Docker Compose setup for local development:

```bash
# Start all services (ClickHouse, PostgreSQL, Redis)
docker-compose up -d

# Run the indexer
cargo run --release -- -c config/local.yaml indexer
```

## Project Structure

```
src/
├── main.rs                    # CLI entry point and service orchestration
├── config.rs                  # Figment-based configuration management
├── asset_manager.rs           # Asset data management and caching
├── asset_worker.rs            # Background asset fetching service
├── event_handler.rs           # Core event processing logic
├── receipt_processor.rs       # Receipt to transaction hash mapping
├── shutdown_coordinator.rs    # Graceful shutdown coordination
├── storage/
│   ├── clickhouse.rs         # ClickHouse storage implementation
│   └── postgres.rs           # PostgreSQL storage implementation
├── data_source/
│   └── neardata_api.rs       # neardata.xyz API integration
├── cache/
│   └── receipts_cache.rs     # Transaction hash caching
└── types.rs                   # Shared data structures
```

## Database Schema

The indexer uses **3 main tables** across both PostgreSQL (default) and ClickHouse database systems:

1. **`events`** - Core blockchain event data
2. **`swaps`** - Swap transaction records
3. **`assets`** - Asset/token metadata and pricing information

### PostgreSQL Schema (Default)

#### 1. Events Table

Stores raw blockchain events from NEAR Protocol contracts:

```sql
CREATE TABLE IF NOT EXISTS events
(
    block_height
    BIGINT
    NOT
    NULL,
    block_timestamp
    TIMESTAMP
    WITH
    TIME
    ZONE
    NOT
    NULL,
    block_hash
    TEXT
    NOT
    NULL,
    contract_id
    TEXT
    NOT
    NULL,
    execution_status
    TEXT
    NOT
    NULL,
    version
    TEXT
    NOT
    NULL,
    standard
    TEXT
    NOT
    NULL,
    index_in_log
    BIGINT
    NOT
    NULL,
    event
    TEXT
    NOT
    NULL,
    data
    TEXT
    NOT
    NULL,
    related_receipt_id
    TEXT
    NOT
    NULL,
    related_receipt_receiver_id
    TEXT
    NOT
    NULL,
    related_receipt_predecessor_id
    TEXT
    NOT
    NULL,
    tx_hash
    TEXT,

    PRIMARY
    KEY
(
    block_height,
    related_receipt_id,
    index_in_log
)
    );

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_events_block_height ON events (block_height DESC);
CREATE INDEX IF NOT EXISTS idx_events_tx_hash ON events (tx_hash) WHERE tx_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_order_by ON events (block_height DESC, index_in_log DESC);
```

#### 2. Swaps Table

Stores processed swap transaction data from DeFi events:

```sql
CREATE TABLE IF NOT EXISTS swaps
(
    intent_hash
    TEXT
    PRIMARY
    KEY,
    origin_asset
    TEXT
    NOT
    NULL,
    destination_asset
    TEXT
    NOT
    NULL,
    amount_in
    NUMERIC
    NOT
    NULL,
    amount_out
    NUMERIC
    NOT
    NULL,
    withdrawal_fee
    NUMERIC
    NOT
    NULL,
    recipient
    TEXT
    NOT
    NULL,
    tx_hash
    TEXT
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_swaps_origin_asset ON swaps USING HASH (origin_asset);
CREATE INDEX IF NOT EXISTS idx_swaps_destination_asset ON swaps USING HASH (destination_asset);
CREATE INDEX IF NOT EXISTS idx_swaps_recipient ON swaps USING HASH (recipient);
```

#### 3. Assets Table

Stores asset/token metadata and pricing information:

```sql
CREATE TABLE IF NOT EXISTS assets
(
    defuse_asset_identifier
    TEXT
    NOT
    NULL,
    near_token_id
    TEXT
    NOT
    NULL,
    intents_token_id
    TEXT
    NOT
    NULL,
    decimals
    SMALLINT
    NOT
    NULL,
    asset_name
    TEXT
    NOT
    NULL,
    symbol
    TEXT,
    min_deposit_amount
    TEXT,
    min_withdrawal_amount
    TEXT,
    withdrawal_fee
    TEXT,
    standard
    TEXT
    NOT
    NULL,
    blockchain
    TEXT
    NOT
    NULL,
    price
    DOUBLE
    PRECISION,
    price_updated_at
    TIMESTAMPTZ,
    contract_address
    TEXT
    NOT
    NULL,

    PRIMARY
    KEY
(
    defuse_asset_identifier,
    intents_token_id
)
    );

CREATE INDEX IF NOT EXISTS idx_assets_intents_token_id ON assets(intents_token_id);
```

### ClickHouse Schema (Analytics)

#### 1. Events Table

Optimized for high-volume analytical queries:

```sql
CREATE TABLE IF NOT EXISTS events
(
    block_height
    UInt64
    COMMENT
    'The height of the block',
    block_timestamp
    DateTime64
(
    9,
    'UTC'
) COMMENT 'The timestamp of the block in UTC',
    block_hash String COMMENT 'The hash of the block',
    contract_id String COMMENT 'The ID of the account on which the execution outcome happens',
    execution_status String COMMENT 'The execution outcome status',
    version String COMMENT 'The event version',
    standard String COMMENT 'The event standard',
    index_in_log UInt64 COMMENT 'The index of the event in the execution outcome log',
    event String COMMENT 'The event type',
    data String COMMENT 'The event JSON data',
    related_receipt_id String COMMENT 'The execution outcome receipt ID',
    related_receipt_receiver_id String COMMENT 'The destination account ID',
    related_receipt_predecessor_id String COMMENT 'The account ID which issued a receipt',
    tx_hash Nullable
(
    String
) COMMENT 'The transaction hash',
    INDEX block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX contract_id_bloom_index contract_id TYPE bloom_filter
(
) GRANULARITY 1,
    INDEX related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter
(
) GRANULARITY 1,
    INDEX related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter
(
) GRANULARITY 1,
    INDEX tx_hash_bloom_index tx_hash TYPE bloom_filter
(
) GRANULARITY 1
    ) ENGINE = ReplacingMergeTree
    PRIMARY KEY
(
    block_height,
    related_receipt_id,
    index_in_log
)
    ORDER BY
(
    block_height,
    related_receipt_id,
    index_in_log
);
```

#### 2. Swaps Table

Optimized for fast lookups and aggregations:

```sql
CREATE TABLE IF NOT EXISTS swaps
(
    intent_hash
    String
    COMMENT
    'The unique identifier for the swap intent',
    origin_asset
    String
    COMMENT
    'The asset being swapped from',
    destination_asset
    String
    COMMENT
    'The asset being swapped to',
    amount_in
    UInt128
    COMMENT
    'The amount of origin asset in smallest units (wei)',
    amount_out
    UInt128
    COMMENT
    'The amount of destination asset in smallest units (wei)',
    withdrawal_fee
    UInt128
    COMMENT
    'The withdrawal fee for the swap in smallest units (wei)',
    recipient
    String
    COMMENT
    'The recipient address of the swap',
    tx_hash
    Nullable
(
    String
) COMMENT 'The transaction hash associated with this swap',
    INDEX origin_asset_bloom_index origin_asset TYPE bloom_filter
(
) GRANULARITY 1,
    INDEX destination_asset_bloom_index destination_asset TYPE bloom_filter
(
) GRANULARITY 1,
    INDEX recipient_bloom_index recipient TYPE bloom_filter
(
) GRANULARITY 1
    ) ENGINE = MergeTree
(
)
    PRIMARY KEY
(
    intent_hash
)
    ORDER BY
(
    intent_hash
);
```

#### 3. Assets Table

Designed for efficient asset metadata queries:

```sql
CREATE TABLE IF NOT EXISTS assets
(
    defuse_asset_identifier
    String,
    near_token_id
    String,
    intents_token_id
    String,
    decimals
    UInt8,
    asset_name
    String,
    symbol
    Nullable
(
    String
),
    min_deposit_amount Nullable
(
    String
),
    min_withdrawal_amount Nullable
(
    String
),
    withdrawal_fee Nullable
(
    String
),
    standard String,
    blockchain String,
    price Nullable
(
    Float64
),
    price_updated_at Nullable
(
    String
),
    contract_address String
    ) ENGINE = MergeTree
(
)
    ORDER BY
(
    defuse_asset_identifier,
    intents_token_id
)
    SETTINGS index_granularity = 8192;
```

### Migration Management

Database schema migrations are automatically applied on startup:

- **PostgreSQL**: Uses standard migration files in `migrations/postgres/`
- **ClickHouse**: Uses migration files in `migrations/clickhouse/` with tracking table

## Monitoring and Observability

The application provides comprehensive logging through the `tracing` crate:

```bash
# Set log level
export RUST_LOG=info  # trace, debug, info, warn, error
```

Key log events include:

- Block processing progress
- Asset data updates
- Database connection status
- Graceful shutdown coordination
- Error conditions with retry attempts

## Development Commands

```bash
# Check for compilation errors
cargo check

# Run tests
cargo test

# Format code
cargo fmt

# Run linter
cargo clippy

# Build release version
cargo build --release
```

## Graceful Shutdown

The application supports graceful shutdown via SIGINT/SIGTERM signals:

1. Asset worker flushes cached data to database
2. Event processing completes current batch
3. All background tasks complete within the configured timeout
4. Database connections are properly closed

Configure shutdown behavior:

```yaml
shutdown:
  shutdown_timeout: "20s"
```

## Contributing

1. Ensure all tests pass: `cargo test`
2. Format code: `cargo fmt`
3. Run linter: `cargo clippy`
4. Update documentation as needed

## Architecture

The indexer follows a modular, production-ready architecture with:

- **Trait-based abstractions** for storage and data sources
- **Comprehensive retry logic** with exponential backoff
- **Multi-level caching** (in-memory + Redis)
- **Graceful shutdown coordination** across all services
- **Flexible configuration** supporting multiple deployment scenarios
- **Observability** through structured logging

This design enables easy testing, deployment flexibility, and operational reliability in production environments.
