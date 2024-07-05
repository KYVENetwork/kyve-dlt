<p align="center">
    <h1>KYVE - DataLoadTool</h1>
    <strong>Load datasets from KYVE pools to BigQuery and Postgres</strong>
</p>

## Build from Source
```bash
git clone https://github.com/KYVENetwork/kyve-dlt.git
cd kyve-dlt
make build
```

This will build **dlt** in /build directory. Afterward, you may want to put it into your machine's PATH like as follows:
```bash
cp build/dlt ~/go/bin/dlt
```

## Usage
Depending on what you want to achieve with `dlt` there are two commands available. A quick summary of what they do
and when to use them can be found below:

|                  | Description                                                                                                                                                                                                                                                                                 | Recommendation                                                                                                                                |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| **sync**         | Starts an incremental sync based on a start bundle ID - first bundle of the pool by default. The sync runs until all bundles are loaded into the destination. When executing `dlt sync` again, it checks the latest loaded bundle ID in order to incrementally extend the existing dataset. | Generally recommended to sync a whole pool dataset into a destination. Can be used with `run` to keep the dataset in the destination updated. |
| **run**          | Runs a supervised incremental sync in a given hour interval (default: 2).                                                                                                                                                                                                                   | Recommended to keep a dataset updated with incrementally added data.                                                                          |
| **partial-sync** | Starts a partial sync from a start to an end bundle ID. Doesn't check the destination for already loaded bundles or duplicates.                                                                                                                                                             | Recommended to sync a specified range of bundles or to back-fill an existing dataset.                                                         |

### Config
The **dlt** config is used to define the source and the destination. With the first executed
command, a default config is created under `path-to/kyve-dlt/.kyve-dlt/config.yml`, which
includes some example values and explanations. After specifying the KYVE source and the 
BigQuery or Postgres credentials, a sync can be started.

For each sync process, a connection consisting of a source and a destination is required that can be defined in the config.

### `sync`
**Usage:**
```bash
dlt sync --connection connection_1
```
To start the incremental sync from a certain height, simply use the `--from-bundle-id` flag. This won't affect the incremental sync, but only the initial start bundle ID of the dataset.

### `run` 
**Usage:**
```bash
dlt run --connection connection_1 --interval 1
```
To start the supervised incremental sync that is executed in an 1-hour interval, simply use the `--interval` flag that expects the hour value.

### `partial-sync`
**Usage:**
```bash
dlt partial-sync --connection connection_1 --from-bundle-id 0 --to-bundle-id 99
```
The partial sync expects two further flags, `--from-bundle-id` and `--to-bundle-id`. In this example, the first 100 bundles of a defined KYVE source are loaded into the destination. `dlt` doesn't check the destination for existing bundles or duplicates.

## Schemas
- Base (supports all KYVE data pools)
- Tendermint
- TendermintPreprocessed (block is split into block_results, end_blocks, etc.)

## Supported Destinations
- BigQuery
- Postgres