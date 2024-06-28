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

func (p *Postgres) StartProcess(schema schema.DataSource, dataRowChannel chan []schema.DataRow, waitGroup *sync.WaitGroup) {
	p.schema = schema
	p.dataRowChannel = dataRowChannel
	waitGroup.Add(1)

	// Open DB
	db, err := sql.Open("postgres", "postgresql://localhost:5432/postgres?sslmode=disable")
	if err != nil {
		panic(err)
	}
	p.db = db
	fmt.Printf("Postgres connection established\n")

	if _, tableErr := p.db.Exec(p.schema.GetPostgresCreateTableCommand(p.config.TableName)); tableErr != nil {
		panic(tableErr)
	}

	p.postgresWaitGroup.Add(p.config.PostgresWorkerCount)
	for i := 1; i <= p.config.PostgresWorkerCount; i++ {
		go p.postgresWorker(fmt.Sprintf("Postgres - %d", i))
	}

	go func() {
		p.postgresWaitGroup.Wait()
		waitGroup.Done()
		_ = p.db.Close()
	}()
}

func (p *Postgres) postgresWorker(name string) {
	defer p.postgresWaitGroup.Done()

	for {
		items, ok := <-p.dataRowChannel
		if !ok {
			fmt.Printf("(%s) Finished\n", name)
			return
		}
		_ = items

		utils.TryWithExponentialBackoff(func() error {
			return p.bulkInsert(items)
		}, func(err error) {
			fmt.Printf("(%s) error: %s \nRetry in 5 seconds.\n", name, err.Error())
		})

		fmt.Printf("(%s) Inserted %d rows. - channel(dataRow): %d\n", name, len(items), len(p.dataRowChannel))
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
