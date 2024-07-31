package destinations

import (
	"database/sql"
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/schema"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	_ "github.com/lib/pq"
	"strconv"
	"strings"
	"sync"
)

type PostgresConfig struct {
	ConnectionUrl string
	TableName     string

	PostgresWorkerCount int
}

func NewPostgres(config PostgresConfig) Postgres {
	return Postgres{
		config:         config,
		dataRowChannel: nil,
	}
}

type Postgres struct {
	config         PostgresConfig
	dataRowChannel chan []schema.DataRow
	db             *sql.DB

	postgresWaitGroup sync.WaitGroup

	schema schema.DataSource
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

func (p *Postgres) Initialize(schema schema.DataSource, dataRowChannel chan []schema.DataRow) {
	p.schema = schema
	p.dataRowChannel = dataRowChannel

	db, err := sql.Open("postgres", p.config.ConnectionUrl)
	if err != nil {
		panic(err)
	}

	p.db = db
	logger.Info().Msg("postgres connection established")

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
		items, ok := <-p.dataRowChannel
		if !ok {
			logger.Debug().Str("worker-id", workerId).Msg("Finished")
			return
		}
		_ = items

		utils.TryWithExponentialBackoff(func() error {
			return p.bulkInsert(items)
		}, func(err error) {
			logger.Error().Str("worker-id", workerId).Str("err", err.Error()).Msg("PostgresWorker error, retry in 5 seconds")
		})

		logger.Info().Str("worker-id", workerId).Msg(fmt.Sprintf("inserted %d rows. - channel(dataRow): %d", len(items), len(p.dataRowChannel)))
	}
}

func (p *Postgres) bulkInsert(items []schema.DataRow) error {
	columnNames := p.schema.GetCSVSchema()

	argsCounter := 1
	templateStrings := make([]string, 0, len(items))
	valueArgs := make([]interface{}, 0, len(items))
	for _, row := range items {
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
	}

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		p.config.TableName,
		"\""+strings.Join(columnNames, "\", \"")+"\"",
		strings.Join(templateStrings, ", "),
	)
	_, err := p.db.Exec(stmt, valueArgs...)

	return err
}
