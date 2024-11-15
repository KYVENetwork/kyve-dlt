<!--

"Features" for new features.
"Improvements" for changes in existing functionality.
"Deprecated" for soon-to-be removed features.
"Bug Fixes" for any bug fixes.

-->

# CHANGELOG

An '!' indicates a CLI or schema breaking change.

## UNRELEASED

### Improvements
- [#24](https://github.com/KYVENetwork/kyve-dlt/pull/24) Add support for Comet38 `finalize_block_events` in tendermint_preprocessed schema.
- [#25](https://github.com/KYVENetwork/kyve-dlt/pull/25) Rename `tendermint` schema to `height`.

### Features
- [#22](https://github.com/KYVENetwork/kyve-dlt/pull/22) Add support for ArTurbo Storage Provider.


## [v1.0.0](https://github.com/KYVENetwork/kyve-dlt/releases/tag/v1.0.0) - 2024-10-30

This is the initial release of the **KYVE Data-Load-Tool (DLT)**! This tool is designed for high-parallelized sync speeds, enabling efficient extraction of data from the KYVE data lake to a specified destination. The DLT supports both single sync operations and daemon mode, allowing for automated, cron-scheduled sync intervals.
Refer to the Readme.md for setup instructions, usage guidelines, and examples of how to configure syncs for different sources and destinations.

### Features
- **High-Speed Parallelized Sync**: Maximizes data transfer efficiency.
- **Daemon Mode with Cron Scheduling**: Automate your data syncs on a customized schedule.
- **Flexible Destination Support**: Sync data from KYVE to your preferred storage solution.

#### Supported Sources
- **KYVE Mainnet**
- **Kaon Testnet**
- **Korellia Devnet**

#### Supported Destinations
- **BigQuery**
- **Postgres**

