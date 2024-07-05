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

## Initialization
To set up the `dlt` config, run:
```bash
dlt init
```

This will either guide you to set up a first source and destination or create a config with default values.
A connection consisting of a source and a destination is required to start any sync process.

## Usage
Depending on what you want to achieve with `dlt` there are two commands available. A quick summary of what they do
and when to use them can be found below:

|                                 | Description                                                                                                                                                                                                                                                                                 | Recommendation                                                                                                                                    |
|---------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| **sync**                        | Starts an incremental sync based on a start bundle ID - first bundle of the pool by default. The sync runs until all bundles are loaded into the destination. When executing `dlt sync` again, it checks the latest loaded bundle ID in order to incrementally extend the existing dataset. | Generally recommended to sync a whole pool dataset into a destination. Can be used with a cronjob to keep the dataset in the destination updated. |
| **partial-sync**                | Starts a partial sync from a start to an end bundle ID. Doesn't check the destination for already loaded bundles or duplicates.                                                                                                                                                             | Recommended to sync a specified range of bundles or to back-fill an existing dataset.                                                             |

### `sync`
**Usage:**
```bash
dlt sync --connection connection_1
```
To start the incremental sync from a certain height, simple use the `--from-bundle-id` flag. This won't affect the incremental sync, but only the initial start bundle ID of the dataset.

### `partial-sync`
**Usage:**
```bash
dlt partial-sync --connection connection_1 --from-bundle-id 0 --to-bundle-id 99
```
The partial sync expects two further flags, `--from-bundle-id` and `--to-bundle-id`. In this example, the first 100 bundles of a defined KYVE source are loaded into the destination. `dlt` doesn't check the destination for existing bundles or duplicates.

## Manage config
With the following commands, sources, destinations, and connections can be added, removed or listed:
```bash
dlt sources      {add|remove|list}
dlt destinations {add|remove|list}
dlt connections  {add|remove|list}
```

## Schemas
- Base (supports all KYVE data pools)
- Tendermint
- TendermintPreprocessed (block is split into block_results, end_blocks, etc.)

## Supported Destinations
- BigQuery
- Postgres