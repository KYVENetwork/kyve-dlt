package utils

type Config struct {
	Sources      []Source      `yaml:"sources"`
	Destinations []Destination `yaml:"destinations"`
	Connections  []Connection  `yaml:"connections"`
	Loader       Loader        `yaml:"loader"`
	LogLevel     string        `yaml:"log_level"`
}

type Source struct {
	Name      string `yaml:"name"`
	PoolID    int    `yaml:"pool_id"`
	BatchSize int    `yaml:"batch_size"`
	Endpoint  string `yaml:"endpoint"`
	Schema    string `yaml:"schema"`
}

type Destination struct {
	Name              string `yaml:"name"`
	Type              string `yaml:"type"`
	ProjectID         string `yaml:"project_id,omitempty"`
	DatasetID         string `yaml:"dataset_id,omitempty"`
	TableID           string `yaml:"table_id,omitempty"`
	BucketName        string `yaml:"bucket_name,omitempty"`
	BucketWorkerCount int    `yaml:"bucket_worker_count,omitempty"`
	ConnectionURL     string `yaml:"connection_url,omitempty"`
	TableName         string `yaml:"table_name,omitempty"`
	WorkerCount       int    `yaml:"worker_count"`
}

type Connection struct {
	Name        string `yaml:"name"`
	Source      string `yaml:"source"`
	Destination string `yaml:"destination"`
	Cron        string `yaml:"cron"`
}

type Loader struct {
	ChannelSize    int `yaml:"channel_size"`
	CSVWorkerCount int `yaml:"csv_worker_count"`
	MaxRamGB       int `yaml:"max_ram_gb"`
}
