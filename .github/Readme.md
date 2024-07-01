# KYVE - **D**ata **L**oad **T**ool

Load datasets from KYVE pools to BigQuery and Postgres. 

## Usage
1. Clone the repository
```bash
git clone https://github.com/KYVENetwork/kyve-dlt.git
```

2. Build the `dlt` binaries
```bash
make build
```

3. Start the syncing process
```bash
./build/dlt start
```

## Config
The **dlt** config can be used to define the syncing processes. With the first start
command, a default config is created under `path-to/kyve-dlt/.kyve-dlt/config.yml`, which
includes some example values and explanations. After specifying the BigQuery or Postgres
credentials, the sync can be started.

## Schemas
- Base (supports all KYVE data pools)
- Tendermint
- TendermintPreprocessed (block is split into block_results, end_blocks, etc.)

## Supported Destinations
- BigQuery
- Postgres