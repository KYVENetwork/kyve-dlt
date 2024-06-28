package schema

import (
	"cloud.google.com/go/bigquery"
	"encoding/json"
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/google/uuid"
	"strconv"
	"time"
)

type TendermintPreProcessedItem struct {
	Key   string `json:"key"`
	Value struct {
		Block        json.RawMessage `json:"block"`
		BlockResults struct {
			Height                string            `json:"height"`
			TxsResults            []json.RawMessage `json:"txs_results"`
			BeginBlockEvents      []json.RawMessage `json:"begin_block_events"`
			EndBlockEvents        []json.RawMessage `json:"end_block_events"`
			ValidatorUpdates      []interface{}     `json:"validator_updates"`
			ConsensusParamUpdates struct {
			} `json:"consensus_param_updates"`
		} `json:"block_results"`
	} `json:"value"`
}

type TendermintPreProcessedValue struct {
	Block        json.RawMessage                    `json:"block"`
	BlockResults TendermintPreProcessedBlockResults `json:"block_results"`
}

type TendermintPreProcessedBlockResults struct {
	Height                string            `json:"height"`
	TxsResults            []json.RawMessage `json:"txs_results"`
	BeginBlockEvents      []json.RawMessage `json:"begin_block_events"`
	EndBlockEvents        []json.RawMessage `json:"end_block_events"`
	ValidatorUpdates      []interface{}     `json:"validator_updates"`
	ConsensusParamUpdates struct {
	} `json:"consensus_param_updates"`
}

type TendermintPreProcessedRow struct {
	_dlt_raw_id       string
	_dlt_extracted_at string
	item_type         string
	value             string
	height            string
	array_index       int64
	bundle_id         int64
}

func (t TendermintPreProcessedRow) ConvertToCSVLine() []string {
	return []string{
		uuid.New().String(),
		t._dlt_extracted_at,
		"{\"errors\":[], \"loader\": \"KYVE-DLT\"}", // airbyte meta_data
		t.item_type,
		t.value,
		t.height,
		strconv.FormatInt(t.bundle_id, 10), // offset
		strconv.FormatInt(t.array_index, 10),
	}
}

type TendermintPreProcessed struct{}

func (t TendermintPreProcessed) GetBigQuerySchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "_airbyte_raw_id", Type: bigquery.StringFieldType},
		{Name: "_airbyte_extracted_at", Type: bigquery.TimestampFieldType},
		{Name: "_airbyte_meta", Type: bigquery.JSONFieldType},
		{Name: "type", Type: bigquery.StringFieldType},
		{Name: "value", Type: bigquery.JSONFieldType},
		{Name: "height", Type: bigquery.IntegerFieldType},
		{Name: "offset", Type: bigquery.StringFieldType},
		{Name: "array_index", Type: bigquery.IntegerFieldType},
	}
}

func (t TendermintPreProcessed) GetBigQueryTimePartitioning() *bigquery.TimePartitioning {
	return &bigquery.TimePartitioning{
		Field: "_airbyte_extracted_at",
		Type:  bigquery.DayPartitioningType,
	}
}

func (t TendermintPreProcessed) GetBigQueryClustering() *bigquery.Clustering {
	return &bigquery.Clustering{Fields: []string{"_airbyte_extracted_at"}}
}

func (t TendermintPreProcessed) GetCSVSchema() []string {
	return []string{
		"_airbyte_raw_id",
		"_airbyte_extracted_at",
		"_airbyte_meta",
		"type",
		"value",
		"height",
		"offset",
		"array_index",
	}
}

func (t TendermintPreProcessed) GetPostgresCreateTableCommand(name string) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    _airbyte_raw_id varchar NOT NULL,
    _airbyte_extracted_at timestamp NOT NULL,
    _airbyte_meta varchar NOT NULL,
    "type" varchar, 
    "value" varchar, 
    "height" integer NOT NULL, 
    "offset" varchar NOT NULL, 
    "array_index" integer, 
    PRIMARY KEY (key)
    )
    `, name)
}

func (t TendermintPreProcessed) DownloadAndConvertBundle(bundle collector.Bundle) ([]DataRow, error) {

	bundleBuffer, err := downloadBundle(bundle)
	if err != nil {
		return nil, err
	}

	var items []TendermintPreProcessedItem
	err = json.Unmarshal(bundleBuffer.Bytes(), &items)
	if err != nil {
		return nil, err
	}

	columns := make([]DataRow, 0)
	for _, kyveItem := range items {
		utils.AwaitEnoughMemory("TODO")

		prunedBlockResults := TendermintPreProcessedBlockResults{
			Height:                kyveItem.Value.BlockResults.Height,
			TxsResults:            nil,
			BeginBlockEvents:      nil,
			EndBlockEvents:        nil,
			ValidatorUpdates:      kyveItem.Value.BlockResults.ValidatorUpdates,
			ConsensusParamUpdates: kyveItem.Value.BlockResults.ConsensusParamUpdates,
		}

		prunedItem := TendermintPreProcessedValue{
			Block:        kyveItem.Value.Block,
			BlockResults: prunedBlockResults,
		}

		prunedJson, err := json.Marshal(prunedItem)
		if err != nil {
			return nil, err
		}

		// TODO put array_index to null on type null

		bundleId, _ := strconv.ParseUint(bundle.Id, 10, 64)
		columns = append(columns, TendermintPreProcessedRow{
			_dlt_raw_id:       "",
			_dlt_extracted_at: time.Now().Format(time.RFC3339),
			item_type:         "",
			value:             string(prunedJson),
			height:            kyveItem.Key,
			array_index:       0,
			bundle_id:         int64(bundleId),
		})
		for index, beginBlockItem := range kyveItem.Value.BlockResults.BeginBlockEvents {
			columns = append(columns, TendermintPreProcessedRow{
				_dlt_raw_id:       "",
				_dlt_extracted_at: time.Now().Format(time.RFC3339),
				item_type:         "begin_block_event",
				value:             string(beginBlockItem),
				height:            kyveItem.Key,
				array_index:       int64(index),
				bundle_id:         int64(bundleId),
			})
		}
		for index, txResult := range kyveItem.Value.BlockResults.TxsResults {
			columns = append(columns, TendermintPreProcessedRow{
				_dlt_raw_id:       "",
				_dlt_extracted_at: time.Now().Format(time.RFC3339),
				item_type:         "tx_result",
				value:             string(txResult),
				height:            kyveItem.Key,
				array_index:       int64(index),
				bundle_id:         int64(bundleId),
			})
		}
		for index, endBlockEvents := range kyveItem.Value.BlockResults.EndBlockEvents {
			columns = append(columns, TendermintPreProcessedRow{
				_dlt_raw_id:       "",
				_dlt_extracted_at: time.Now().Format(time.RFC3339),
				item_type:         "end_block_event",
				value:             string(endBlockEvents),
				height:            kyveItem.Key,
				array_index:       int64(index),
				bundle_id:         int64(bundleId),
			})
		}
	}

	return columns, nil
}
