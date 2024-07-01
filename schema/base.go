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

type BaseItem struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type BaseRow struct {
	_dlt_raw_id       string
	_dlt_extracted_at string
	key               string
	value             string
	bundle_id         int64
}

func (t BaseRow) ConvertToCSVLine() []string {
	return []string{
		uuid.New().String(),
		t._dlt_extracted_at,
		"{\"errors\":[], \"loader\": \"KYVE-DLT\"}", // airbyte meta_data
		t.key,
		t.value,
		strconv.FormatInt(t.bundle_id, 10),
	}
}

// TODO: Refactor to Base schema
type Base struct{}

func (t Base) GetBigQuerySchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "_dlt_raw_id", Type: bigquery.StringFieldType},
		{Name: "_dlt_extracted_at", Type: bigquery.TimestampFieldType},
		{Name: "key", Type: bigquery.StringFieldType},
		{Name: "value", Type: bigquery.JSONFieldType},
		{Name: "bundle_id", Type: bigquery.IntegerFieldType},
	}
}

func (t Base) GetBigQueryTimePartitioning() *bigquery.TimePartitioning {
	return &bigquery.TimePartitioning{
		Field: "_dlt_extracted_at",
		Type:  bigquery.DayPartitioningType,
	}
}

func (t Base) GetBigQueryClustering() *bigquery.Clustering {
	return &bigquery.Clustering{Fields: []string{"_dlt_extracted_at"}}
}

func (t Base) GetCSVSchema() []string {
	return []string{
		"_dlt_raw_id",
		"_dlt_extracted_at",
		"key",
		"value",
		"bundle_id",
	}
}

func (t Base) GetPostgresCreateTableCommand(name string) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    _dlt_raw_id varchar NOT NULL,
    _dlt_extracted_at timestamp NOT NULL,
    "key" varchar NOT NULL, 
    "value" varchar, 
    "bundle_id" integer NOT NULL, 
    PRIMARY KEY (key)
    )
    `, name)
}

func (t Base) DownloadAndConvertBundle(bundle collector.Bundle) ([]DataRow, error) {
	bundleBuffer, err := downloadBundle(bundle)
	if err != nil {
		return nil, err
	}

	var items []BaseItem
	err = json.Unmarshal(bundleBuffer.Bytes(), &items)
	if err != nil {
		return nil, err
	}

	bundleId, _ := strconv.ParseUint(bundle.Id, 10, 64)

	columns := make([]DataRow, 0)
	for _, kyveItem := range items {
		utils.AwaitEnoughMemory("TODO")

		jsonValue, err := json.Marshal(kyveItem.Value)
		if err != nil {
			return nil, err
		}
		columns = append(columns, BaseRow{
			_dlt_raw_id:       "",
			_dlt_extracted_at: time.Now().Format(time.RFC3339),
			value:             string(jsonValue),
			key:               kyveItem.Key,
			bundle_id:         int64(bundleId),
		})

	}

	return columns, nil
}
