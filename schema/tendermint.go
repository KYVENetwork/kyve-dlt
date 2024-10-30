package schema

import (
	"cloud.google.com/go/bigquery"
	"encoding/json"
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/google/uuid"
	"strconv"
)

type TendermintItem struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type TendermintRow struct {
	_dlt_raw_id       string
	_dlt_extracted_at string
	height            int64
	value             string
	bundle_id         int64
}

func (t TendermintRow) ConvertToCSVLine() []string {
	return []string{
		uuid.New().String(),
		t._dlt_extracted_at,
		strconv.FormatInt(t.height, 10),
		t.value,
		strconv.FormatInt(t.bundle_id, 10),
	}
}

type Tendermint struct{}

func (t Tendermint) GetBigQuerySchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "_dlt_raw_id", Type: bigquery.StringFieldType},
		{Name: "_dlt_extracted_at", Type: bigquery.TimestampFieldType},
		{Name: "height", Type: bigquery.IntegerFieldType},
		{Name: "value", Type: bigquery.JSONFieldType},
		{Name: "bundle_id", Type: bigquery.IntegerFieldType},
	}
}

func (t Tendermint) GetBigQueryTimePartitioning() *bigquery.TimePartitioning {
	return &bigquery.TimePartitioning{
		Field: "_dlt_extracted_at",
		Type:  bigquery.DayPartitioningType,
	}
}

func (t Tendermint) GetBigQueryClustering() *bigquery.Clustering {
	return &bigquery.Clustering{Fields: []string{"_dlt_extracted_at"}}
}

func (t Tendermint) GetCSVSchema() []string {
	return []string{
		"_dlt_raw_id",
		"_dlt_extracted_at",
		"height",
		"value",
		"bundle_id",
	}
}

func (t Tendermint) GetPostgresCreateTableCommand(name string) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    _dlt_raw_id varchar NOT NULL,
    _dlt_extracted_at timestamp NOT NULL,
    "height" integer NOT NULL, 
    "value" varchar, 
    "bundle_id" integer NOT NULL, 
    PRIMARY KEY (height)
    )
    `, name)
}

func (t Tendermint) DownloadAndConvertBundle(bundle collector.Bundle, extra ExtraData) (Result, error) {
	downloadResult, err := downloadBundle(bundle, extra)
	if err != nil {
		return Result{}, err
	}

	var items []TendermintItem
	err = json.Unmarshal(downloadResult.Data.Bytes(), &items)
	if err != nil {
		return Result{}, err
	}

	bundleId, _ := strconv.ParseUint(bundle.Id, 10, 64)

	columns := make([]DataRow, 0)
	for _, kyveItem := range items {
		utils.AwaitEnoughMemory(extra.Name)

		height, err := strconv.ParseUint(kyveItem.Key, 10, 64)
		if err != nil {
			panic(err)
		}

		jsonValue, err := json.Marshal(kyveItem.Value)
		if err != nil {
			return Result{}, err
		}
		columns = append(columns, TendermintRow{
			_dlt_raw_id:       "",
			_dlt_extracted_at: extra.ExtractedAt,
			value:             string(jsonValue),
			height:            int64(height),
			bundle_id:         int64(bundleId),
		})

	}

	return Result{
		Data:             columns,
		CompressedSize:   downloadResult.CompressedSize,
		UncompressedSize: downloadResult.UncompressedSize,
	}, nil
}
