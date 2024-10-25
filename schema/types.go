package schema

import (
	"bytes"
	"cloud.google.com/go/bigquery"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
)

type DataSource interface {
	DownloadAndConvertBundle(bundle collector.Bundle, extra ExtraData) (Result, error)
	GetCSVSchema() []string
	GetBigQuerySchema() bigquery.Schema
	GetBigQueryTimePartitioning() *bigquery.TimePartitioning
	GetBigQueryClustering() *bigquery.Clustering
	GetPostgresCreateTableCommand(string) string
}

type DataRow interface {
	ConvertToCSVLine() []string
}

type DownloadResult struct {
	Data             *bytes.Buffer
	CompressedSize   int64
	UncompressedSize int64
}

type Result struct {
	Data             []DataRow
	CompressedSize   int64
	UncompressedSize int64
}

type ExtraData struct {
	Name        string
	ExtractedAt string
}
