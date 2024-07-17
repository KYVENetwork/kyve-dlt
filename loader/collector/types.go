package collector

import "time"

type SourceConfig struct {
	PoolId int64
	// inclusive
	FromBundleId int64
	// inclusive
	ToBundleId int64

	BatchSize int64

	Endpoint string

	PartialSync bool
}

type Source struct {
	poolId int64
	// inclusive
	fromBundleId int64
	// inclusive
	toBundleId int64

	batchSize int64

	endpoint string
}

type Bundle struct {
	PoolId        string `json:"pool_id"`
	Id            string `json:"id"`
	StorageId     string `json:"storage_id"`
	Uploader      string `json:"uploader"`
	FromIndex     string `json:"from_index"`
	ToIndex       string `json:"to_index"`
	FromKey       string `json:"from_key"`
	ToKey         string `json:"to_key"`
	BundleSummary string `json:"bundle_summary"`
	DataHash      string `json:"data_hash"`
	FinalizedAt   struct {
		Height    string    `json:"height"`
		Timestamp time.Time `json:"timestamp"`
	} `json:"finalized_at"`
	StorageProviderId string `json:"storage_provider_id"`
	CompressionId     string `json:"compression_id"`
	StakeSecurity     struct {
		ValidVotePower string `json:"valid_vote_power"`
		TotalVotePower string `json:"total_vote_power"`
	} `json:"stake_security"`
}

type Response struct {
	FinalizedBundles []Bundle `json:"finalized_bundles"`
	Pagination       struct {
		NextKey string `json:"next_key"`
		Total   string `json:"total"`
	} `json:"pagination"`
}
