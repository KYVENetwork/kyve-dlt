package destinations

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/google/uuid"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

type BigQueryConfig struct {
	ProjectId string
	DatasetId string
	TableId   string

	BucketWorkerCount   int
	BigQueryWorkerCount int
}

func NewBigQuery(config BigQueryConfig) BigQuery {
	return BigQuery{
		config:         config,
		dataRowChannel: nil,
	}
}

type BigQuery struct {
	config         BigQueryConfig
	dataRowChannel chan []schema.DataRow

	bucketChannel     chan string
	bucketWaitGroup   sync.WaitGroup
	bigQueryWaitGroup sync.WaitGroup

	schema schema.DataSource
}

func (b *BigQuery) StartProcess(schema schema.DataSource, dataRowChannel chan []schema.DataRow, waitGroup *sync.WaitGroup) {

	b.schema = schema
	b.dataRowChannel = dataRowChannel
	b.bucketChannel = make(chan string, 4)

	waitGroup.Add(1)

	// Uploads CSV files to Google Cloud Storage
	b.bucketWaitGroup.Add(b.config.BucketWorkerCount)
	for i := 1; i <= b.config.BucketWorkerCount; i++ {
		go b.bucketWorker(fmt.Sprintf("Bucket - %d", i))
	}

	// Import CSV files from Google Bucket to Table
	b.bigQueryWaitGroup.Add(b.config.BigQueryWorkerCount)
	for i := 1; i <= b.config.BigQueryWorkerCount; i++ {
		go b.bigqueryWorker(fmt.Sprintf("BigQuery - %d", i))
	}

	go func() {
		b.bucketWaitGroup.Wait()
		close(b.bucketChannel)

		b.bigQueryWaitGroup.Wait()

		waitGroup.Done()
	}()
}

func (b *BigQuery) bucketWorker(name string) {
	defer b.bucketWaitGroup.Done()

	for {
		itemRows, ok := <-b.dataRowChannel
		if !ok {
			fmt.Printf("(%s) Finished\n", name)
			return
		}

		// Create CSV
		csvBuffer := new(bytes.Buffer)
		csvWriter := csv.NewWriter(csvBuffer)

		csvWriter.Write(b.schema.GetCSVSchema())
		// write items
		for _, c := range itemRows {
			err := csvWriter.Write(c.ConvertToCSVLine())
			if err != nil {
				fmt.Println(err)
				return
			}
		}
		csvWriter.Flush()

		fileName := fmt.Sprintf("dlt/%s.csv.gz", uuid.New().String())

		utils.TryWithExponentialBackoff(func() error {
			return b.uploadCloudBucket("dbt_udf", fileName, csvBuffer)
		}, func(err error) {
			logger.Error().Str("err", err.Error()).Msg(fmt.Sprintf("(%s) error, retry in 5 seconds", name))
		})

		b.bucketChannel <- fileName

		logger.Debug().Msg(fmt.Sprintf("(%s) Uploaded %s - channel(csvFiles): %d, channel(uuid): %d\n", name, fileName, len(b.dataRowChannel), len(b.bucketChannel)))
	}
}

func (b *BigQuery) bigqueryWorker(name string) {
	defer b.bigQueryWaitGroup.Done()

	for {
		item, ok := <-b.bucketChannel
		if !ok {
			fmt.Printf("(%s) Finished\n", name)
			return
		}

		utils.TryWithExponentialBackoff(func() error {
			return b.importCSVExplicitSchema("gs://dbt_udf/" + item)
		}, func(err error) {
			logger.Error().Str("err", err.Error()).Msg(fmt.Sprintf("(%s) error, retry in 5 seconds", name))
		})

		logger.Debug().Msg(fmt.Sprintf("(%s) Imported %s - channel(uuid): %d\n", name, item, len(b.bucketChannel)))
	}
}

func (b *BigQuery) uploadCloudBucket(bucket, object string, buf io.Reader) error {

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*900)
	defer cancel()

	o := client.Bucket(bucket).Object(object)

	o = o.If(storage.Conditions{DoesNotExist: true})

	// Upload an object with storage.Writer.
	wc := o.NewWriter(ctx)
	wc.ContentEncoding = "gzip"

	gzipWriter := gzip.NewWriter(wc)
	if _, err = io.Copy(gzipWriter, buf); err != nil {
		return fmt.Errorf("io.Copy: %w", err)
	}
	// Flush the gzip writer to ensure all data is written
	err = gzipWriter.Flush()
	if err != nil {
		return fmt.Errorf("gzipFlush: %w", err)
	}
	gzipWriter.Close()

	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %w", err)
	}

	fmt.Sprintf("bundles: %d, csv: %d", len(b.bucketChannel))

	return nil
}

func (b *BigQuery) importCSVExplicitSchema(bucketFilePath string) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, b.config.ProjectId)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	gcsRef := bigquery.NewGCSReference(bucketFilePath)
	gcsRef.SkipLeadingRows = 1
	gcsRef.Schema = b.schema.GetBigQuerySchema()
	loader := client.Dataset(b.config.DatasetId).Table(b.config.TableId).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteAppend
	loader.TimePartitioning = b.schema.GetBigQueryTimePartitioning()
	loader.Clustering = b.schema.GetBigQueryClustering()

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil {
		return fmt.Errorf("job completed with error: %v", status.Err())
	}
	return nil
}