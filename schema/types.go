package schema

import (
	"cloud.google.com/go/bigquery"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
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
