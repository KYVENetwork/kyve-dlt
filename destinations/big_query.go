package destinations

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

type BigQueryConfig struct {
	ProjectId  string
	DatasetId  string
	TableId    string
	BucketName string

	BucketWorkerCount   int
	BigQueryWorkerCount int
}

func NewBigQuery(config BigQueryConfig) BigQuery {
	return BigQuery{
		config:         config,
		dataRowChannel: nil,
		logger:         utils.DltLogger("BigQuery"),
	}
}

type BucketBusItem struct {
	FileName     string
	fromBundleId int64
	toBundleId   int64
}

type BigQuery struct {
	config         BigQueryConfig
	dataRowChannel chan DestinationBusItem

	bucketChannel     chan BucketBusItem
	bucketWaitGroup   sync.WaitGroup
	bigQueryWaitGroup sync.WaitGroup

	schema schema.DataSource

	logger zerolog.Logger
}

func (b *BigQuery) Close() {}

func (b *BigQuery) GetLatestBundleId() *int64 {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, b.config.ProjectId)
	if err != nil {
		b.logger.Error().Msg("failed to create BigQuery client")
		panic(err)
	}

	stmt := fmt.Sprintf("SELECT MAX(`bundle_id`) FROM `%s.%s`", b.config.DatasetId, b.config.TableId)
	query := client.Query(stmt)

	it, err := query.Read(ctx)
	if err != nil {
		// Check if the error is a NotFound error, which indicates that the table does not exist
		var apiErr *googleapi.Error
		if errors.As(err, &apiErr) && apiErr.Code == 404 {
			b.logger.Debug().Msg("BigQuery table does not exist yet")
			return nil
		}
		panic(err)
	}

	var latestBundleId int64 = 0
	for {
		var row []bigquery.Value
		err = it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			b.logger.Error().Str("err", err.Error()).Msg("BigQuery iterator failed")
			return nil
		}
		if row[0] != nil {
			latestBundleId = row[0].(int64)
		}
	}
	return &latestBundleId
}

func (b *BigQuery) Initialize(schema schema.DataSource, destinationChannel chan DestinationBusItem) {
	b.schema = schema
	b.dataRowChannel = destinationChannel
	b.bucketChannel = make(chan BucketBusItem, b.config.BucketWorkerCount)
}

func (b *BigQuery) StartProcess(waitGroup *sync.WaitGroup) {
	waitGroup.Add(1)

	// Uploads CSV files to Google Cloud Storage
	b.bucketWaitGroup.Add(b.config.BucketWorkerCount)
	for i := 1; i <= b.config.BucketWorkerCount; i++ {
		go b.bucketWorker(fmt.Sprintf("bucket-%d", i))
	}

	// Import CSV files from Google Bucket to Table
	b.bigQueryWaitGroup.Add(b.config.BigQueryWorkerCount)
	for i := 1; i <= b.config.BigQueryWorkerCount; i++ {
		go b.bigqueryWorker(fmt.Sprintf("big_query-%d", i))
	}

	go func() {
		b.bucketWaitGroup.Wait()
		close(b.bucketChannel)

		b.bigQueryWaitGroup.Wait()

		waitGroup.Done()
	}()
}

func (b *BigQuery) bucketWorker(workerId string) {
	defer b.bucketWaitGroup.Done()

	for {
		item, ok := <-b.dataRowChannel
		if !ok {
			b.logger.Info().Str("worker-id", workerId).Msg("Finished")
			return
		}

		// Create CSV
		csvBuffer := new(bytes.Buffer)
		csvWriter := csv.NewWriter(csvBuffer)

		csvWriter.Write(b.schema.GetCSVSchema())
		// write items
		for _, c := range item.Data {
			err := csvWriter.Write(c.ConvertToCSVLine())
			if err != nil {
				panic(err)
			}
		}
		csvWriter.Flush()

		fileName := fmt.Sprintf("dlt/%s/%s.csv.gz", time.Now().Format("2006-01-02"), uuid.New().String())

		utils.TryWithExponentialBackoff(func() error {
			return b.uploadCloudBucket(b.config.BucketName, fileName, csvBuffer)
		}, func(err error) {
			b.logger.Error().Str("worker-id", workerId).Str("err", err.Error()).Msg("error, retry in 5 seconds")
		})

		b.bucketChannel <- BucketBusItem{
			FileName:     fileName,
			fromBundleId: item.FromBundleId,
			toBundleId:   item.ToBundleId,
		}

		b.logger.Info().
			Str("worker-id", workerId).
			Str("fileName", fileName).
			Int64("fromBundleId", item.FromBundleId).
			Int64("toBundleId", item.ToBundleId).
			Msg(fmt.Sprintf("uploaded"))
	}
}

func (b *BigQuery) bigqueryWorker(workerId string) {
	defer b.bigQueryWaitGroup.Done()

	for {
		item, ok := <-b.bucketChannel
		if !ok {
			b.logger.Info().Str("worker-id", workerId).Msg("Finished")
			return
		}

		utils.TryWithExponentialBackoff(func() error {
			return b.importCSVExplicitSchema(fmt.Sprintf("gs://%s/%s", b.config.BucketName, item.FileName))
		}, func(err error) {
			b.logger.Error().Str("worker-id", workerId).Str("err", err.Error()).Msg("error, retry in 5 seconds")
		})

		b.logger.Info().
			Str("worker-id", workerId).
			Str("fileName", item.FileName).
			Int64("fromBundleId", item.fromBundleId).
			Int64("toBundleId", item.toBundleId).
			Msg("imported")
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
