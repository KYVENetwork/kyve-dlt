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
		t.height,
		t.item_type,
		strconv.FormatInt(t.array_index, 10),
		t.value,
		strconv.FormatInt(t.bundle_id, 10),
	}
}

type TendermintPreProcessed struct{}

func (t TendermintPreProcessed) GetBigQuerySchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "_dlt_raw_id", Type: bigquery.StringFieldType},
		{Name: "_dlt_extracted_at", Type: bigquery.TimestampFieldType},
		{Name: "height", Type: bigquery.IntegerFieldType},
		{Name: "type", Type: bigquery.StringFieldType},
		{Name: "array_index", Type: bigquery.IntegerFieldType},
		{Name: "value", Type: bigquery.JSONFieldType},
		{Name: "bundle_id", Type: bigquery.IntegerFieldType},
	}
}

func (t TendermintPreProcessed) GetBigQueryTimePartitioning() *bigquery.TimePartitioning {
	return &bigquery.TimePartitioning{
		Field: "_dlt_extracted_at",
		Type:  bigquery.DayPartitioningType,
	}
}

func (t TendermintPreProcessed) GetBigQueryClustering() *bigquery.Clustering {
	return &bigquery.Clustering{Fields: []string{"_dlt_extracted_at"}}
}

func (t TendermintPreProcessed) GetCSVSchema() []string {
	return []string{
		"_dlt_raw_id",
		"_dlt_extracted_at",
		"height",
		"type",
		"array_index",
		"value",
		"bundle_id",
	}
}

func (t TendermintPreProcessed) GetPostgresCreateTableCommand(name string) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    _dlt_raw_id varchar NOT NULL,
    _dlt_extracted_at timestamp NOT NULL,
    "height" integer NOT NULL, 
    "type" varchar, 
    "array_index" integer NOT NULL, 
    "value" varchar, 
    "bundle_id" integer NOT NULL, 
    PRIMARY KEY (height, type, array_index)
    )
    `, name)
}

func (t TendermintPreProcessed) DownloadAndConvertBundle(bundle collector.Bundle, extractedAt string) ([]DataRow, error) {
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

		bundleId, _ := strconv.ParseUint(bundle.Id, 10, 64)
		columns = append(columns, TendermintPreProcessedRow{
			_dlt_raw_id:       "",
			_dlt_extracted_at: time.Now().Format(time.RFC3339),
			item_type:         "block",
			value:             string(prunedJson),
			height:            kyveItem.Key,
			array_index:       0,
			bundle_id:         int64(bundleId),
		})
		for index, beginBlockItem := range kyveItem.Value.BlockResults.BeginBlockEvents {
			columns = append(columns, TendermintPreProcessedRow{
				_dlt_raw_id:       "",
				_dlt_extracted_at: extractedAt,
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
