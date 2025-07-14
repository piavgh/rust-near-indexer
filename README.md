# NEAR Defuse Custom Indexer

This project is a Rust-based indexer that processes blockchain events from NEAR Protocol and inserts them into a Clickhouse database for efficient querying. It uses [NEAR Lake Framework](https://github.com/near/near-lake-framework) for streaming blockchain data and stores structured events in a Clickhouse database.

## Features

- Stream NEAR blockchain events starting from a specified block height.
- Filter and structure events based on specific standards.
- Store events in a Clickhouse database.
- Persist transaction cache to Redis with automatic expiration after 50 blocks.

## Requirements

1. [Rust](https://www.rust-lang.org/tools/install) (1.76.0 version recommended)
2. [Clickhouse](https://clickhouse.com/docs/en/quick-start#self-managed-install) database server
3. Environment variables for configuration

### Build the project

```bash
cargo build --release
```

## Environment Configuration

Before running the indexer, ensure that all required environment variables are set:

| Variable                | Description                     | Default Value            |
| ----------------------- | ------------------------------- | ------------------------ |
| `CLICKHOUSE_URL`        | Clickhouse server URL           | `http://localhost:18123` |
| `CLICKHOUSE_USER`       | Username for Clickhouse         | -                        |
| `CLICKHOUSE_PASSWORD`   | Password for Clickhouse         | -                        |
| `CLICKHOUSE_DB`         | Clickhouse database name        | `mainnet`                |
| `BLOCK_HEIGHT`          | Start block height for indexing | -                        |
| `AWS_ACCESS_KEY_ID`     | AWS access key ID               | -                        |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key           | -                        |
| `REDIS_URL`             | Redis connection URL (optional) | -                        |

Example of setting up environment variables (replace with your actual values):

```bash
export CLICKHOUSE_URL="http://localhost:18123"
export CLICKHOUSE_USER="<your-clickhouse-username>"
export CLICKHOUSE_PASSWORD="<your-clickhouse-password>"
export CLICKHOUSE_DB="mainnet"
export BLOCK_HEIGHT="130636886"
export AWS_ACCESS_KEY_ID="<your-aws-access-key-id>"
export AWS_SECRET_ACCESS_KEY="<your-aws-secret-access-key>"
export REDIS_URL="redis://127.0.0.1:6379"
```

## Usage

1. Start the Clickhouse server.
2. Run the application:

   ```bash
   cargo run --release
   ```

The application will:

- Connect to the Clickhouse server.
- Start reading blockchain events from the specified `BLOCK_HEIGHT` (or the last processed height).
- Filter and insert selected events into the Clickhouse database.

### Optional Command-line Execution

You can pass `BLOCK_HEIGHT` when running the program to override the default or previously set block height.

```bash
BLOCK_HEIGHT=130636886 cargo run --release
```

## Project Structure

The project is structured as follows:

- `main.rs`: Initializes the application, connects to Clickhouse, and starts the indexer.
- `database.rs`: Contains functions for connecting to Clickhouse and managing data.
- `event_handler.rs`: Processes streamed events and filters specific event standards.

## Clickhouse schema

Here is the Clickhouse schema to run the indexer:

```sql
CREATE TABLE IF NOT EXISTS events
    (
        block_height                     UInt64 COMMENT 'The height of the block',
        block_timestamp                  DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
        block_hash                       String COMMENT 'The hash of the block',
        contract_id                      String COMMENT 'The ID of the account on which the execution outcome happens',
        execution_status                 String COMMENT 'The execution outcome status',
        version                        	 String COMMENT 'The event version',
        standard                         String COMMENT 'The event standard',
        event                        	 String COMMENT 'The event type',
        data                         	 String COMMENT 'The event JSON data',
        related_receipt_id               String COMMENT 'The execution outcome receipt ID',
        related_receipt_receiver_id      String COMMENT 'The destination account ID',
        related_receipt_predecessor_id   String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',

        INDEX            block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
        INDEX            contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1,
    ) ENGINE = ReplacingMergeTree
    PRIMARY KEY (block_height, related_receipt_id)
    ORDER BY (block_height, related_receipt_id);
```
