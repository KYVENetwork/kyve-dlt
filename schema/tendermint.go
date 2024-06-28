package schema

import (
	"cloud.google.com/go/bigquery"
	"encoding/json"
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/google/uuid"
	"time"
)

type TendermintItem struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type TendermintRow struct {
	_dlt_raw_id       string
	_dlt_extracted_at string
	key               string
	value             string
	offset            string
}

func (t TendermintRow) ConvertToCSVLine() []string {
	return []string{
		uuid.New().String(),
		t._dlt_extracted_at,
		"{\"errors\":[], \"loader\": \"KYVE-DLT\"}", // airbyte meta_data
		t.key,
		t.value,
		t.offset,
	}
}

type Tendermint struct{}

func (t Tendermint) GetBigQuerySchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "_airbyte_raw_id", Type: bigquery.StringFieldType},
		{Name: "_airbyte_extracted_at", Type: bigquery.TimestampFieldType},
		{Name: "_airbyte_meta", Type: bigquery.JSONFieldType},
		{Name: "key", Type: bigquery.StringFieldType},
		{Name: "value", Type: bigquery.JSONFieldType},
		{Name: "offset", Type: bigquery.StringFieldType},
	}
}

func (t Tendermint) GetBigQueryTimePartitioning() *bigquery.TimePartitioning {
	return &bigquery.TimePartitioning{
		Field: "_airbyte_extracted_at",
		Type:  bigquery.DayPartitioningType,
	}
}

func (t Tendermint) GetBigQueryClustering() *bigquery.Clustering {
	return &bigquery.Clustering{Fields: []string{"_airbyte_extracted_at"}}
}

func (t Tendermint) GetCSVSchema() []string {
	return []string{
		"_airbyte_raw_id",
		"_airbyte_extracted_at",
		"_airbyte_meta",
		"key",
		"value",
		"offset",
	}
}

func (t Tendermint) GetPostgresCreateTableCommand(name string) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    _airbyte_raw_id varchar NOT NULL,
    _airbyte_extracted_at timestamp NOT NULL,
    _airbyte_meta varchar NOT NULL,
    "key" integer NOT NULL, 
    "value" varchar, 
    "offset" varchar NOT NULL, 
    PRIMARY KEY (key)
    )
    `, name)
}

func (t Tendermint) DownloadAndConvertBundle(bundle collector.Bundle) ([]DataRow, error) {

	bundleBuffer, err := downloadBundle(bundle)
	if err != nil {
		return nil, err
	}

	var items []TendermintItem
	err = json.Unmarshal(bundleBuffer.Bytes(), &items)
	if err != nil {
		return nil, err
	}

	columns := make([]DataRow, 0)
	for _, kyveItem := range items {
		utils.AwaitEnoughMemory("TODO", false)

		jsonValue, err := json.Marshal(kyveItem.Value)
		if err != nil {
			return nil, err
		}
		columns = append(columns, TendermintRow{
			_dlt_raw_id:       "",
			_dlt_extracted_at: time.Now().Format(time.RFC3339),
			value:             string(jsonValue),
			key:               kyveItem.Key,
			offset:            bundle.Id,
		})

	}

	return columns, nil
}
