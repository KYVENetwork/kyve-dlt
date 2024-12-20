package destinations

import (
	"database/sql"
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"strconv"
	"strings"
	"sync"
)

type PostgresConfig struct {
	ConnectionUrl string
	TableName     string

	PostgresWorkerCount int
	RowInsertLimit      int
}

func NewPostgres(config PostgresConfig) Postgres {
	return Postgres{
		config:         config,
		dataRowChannel: nil,
		logger:         utils.DltLogger("Postgres"),
	}
}

type Postgres struct {
	config         PostgresConfig
	dataRowChannel chan DestinationBusItem
	db             *sql.DB

	postgresWaitGroup sync.WaitGroup

	schema schema.DataSource

	logger zerolog.Logger
}

func (p *Postgres) Close() {
	if err := p.db.Close(); err != nil {
		panic(err)
	}
}

func (p *Postgres) GetLatestBundleId() *int64 {
	stmt := fmt.Sprintf("SELECT MAX(%s) FROM %s",
		"bundle_id",
		p.config.TableName,
	)

	var latestBundleId *int64
	err := p.db.QueryRow(stmt).Scan(&latestBundleId)
	if err != nil {
		panic(err)
	}

	return latestBundleId
}

func (p *Postgres) Initialize(schema schema.DataSource, destinationChannel chan DestinationBusItem) {
	p.schema = schema
	p.dataRowChannel = destinationChannel

	db, err := sql.Open("postgres", p.config.ConnectionUrl)
	if err != nil {
		panic(err)
	}

	p.db = db
	p.logger.Info().Msg("postgres connection established")

	if _, tableErr := p.db.Exec(p.schema.GetPostgresCreateTableCommand(p.config.TableName)); tableErr != nil {
		panic(tableErr)
	}
}

func (p *Postgres) StartProcess(waitGroup *sync.WaitGroup) {
	waitGroup.Add(1)
	p.postgresWaitGroup.Add(p.config.PostgresWorkerCount)
	for i := 1; i <= p.config.PostgresWorkerCount; i++ {
		go p.postgresWorker(fmt.Sprintf("postgres-%d", i))
	}

	go func() {
		p.postgresWaitGroup.Wait()
		waitGroup.Done()
		_ = p.db.Close()
	}()
}

func (p *Postgres) postgresWorker(workerId string) {
	defer p.postgresWaitGroup.Done()

	for {
		item, ok := <-p.dataRowChannel
		if !ok {
			p.logger.Debug().Str("worker-id", workerId).Msg("Finished")
			return
		}

		utils.TryWithExponentialBackoff(func() error {
			return p.bulkInsert(item.Data, p.config.RowInsertLimit)
		}, func(err error) {
			p.logger.Error().Str("worker-id", workerId).Str("err", err.Error()).Msg("PostgresWorker error, retry in 5 seconds")
		})

		p.logger.Info().
			Str("worker-id", workerId).
			Int64("fromBundleId", item.FromBundleId).
			Int64("toBundleId", item.ToBundleId).
			Int("rows", len(item.Data)).
			Msg("inserted")
	}
}

func (p *Postgres) bulkInsert(items []schema.DataRow, rowLimit int) error {
	columnNames := p.schema.GetCSVSchema()

	argsCounter := 1
	templateStrings := make([]string, 0, len(items))
	valueArgs := make([]interface{}, 0, len(items))
	selectedRows := 0
	for _, row := range items {
		selectedRows++
		s := make([]string, len(columnNames))
		for i := range s {
			s[i] = "$" + strconv.FormatInt(int64(argsCounter), 10)
			argsCounter += 1
		}
		templateString := fmt.Sprintf("(%s)", strings.Join(s, ", "))
		templateStrings = append(templateStrings, templateString)
		for _, field := range row.ConvertToCSVLine() {
			valueArgs = append(valueArgs, field)
		}

		if selectedRows == rowLimit {
			stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
				p.config.TableName,
				"\""+strings.Join(columnNames, "\", \"")+"\"",
				strings.Join(templateStrings, ", "),
			)
			_, err := p.db.Exec(stmt, valueArgs...)
			if err != nil {
				return err
			}
			argsCounter = 1
			templateStrings = make([]string, 0, len(items))
			valueArgs = make([]interface{}, 0, len(items))
			selectedRows = 0
		}
	}

	if selectedRows >= 1 {
		stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			p.config.TableName,
			"\""+strings.Join(columnNames, "\", \"")+"\"",
			strings.Join(templateStrings, ", "),
		)
		_, err := p.db.Exec(stmt, valueArgs...)
		if err != nil {
			return err
		}
	}
	return nil
}
