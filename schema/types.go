package schema

import (
	"KYVE-DLT/loader/collector"
	"cloud.google.com/go/bigquery"
)

type DataSource interface {
	DownloadAndConvertBundle(bundle collector.Bundle) ([]DataRow, error)
	GetCSVSchema() []string
	GetBigQuerySchema() bigquery.Schema
	GetBigQueryTimePartitioning() *bigquery.TimePartitioning
	GetBigQueryClustering() *bigquery.Clustering
	GetPostgresCreateTableCommand(string) string
}

type DataRow interface {
	ConvertToCSVLine() []string
}
