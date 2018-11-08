package config

var currentConfig *Config

type Config struct {
	Service Service `xml:"service"`
	Redis   Redis   `xml:"redis"`
	Mysql   Mysql   `xml:"mysql"`
	Kafka   Kafka   `xml:"kafka"`
}

type Service struct {
	MaxStoreMonths uint32 `xml:"max_store_months"`
	CustomerNum    uint32 `xml:"customer_num"`
	JobPoolSize    uint32 `xml:"job_pool_size"`
}

type Redis struct {
	Host string `xml:"host"`
	Port int    `xml:"port"`
	Auth string `xml:"auth"`
	Db   string `xml:"db"`
}

type Mysql struct {
	Host       string `xml:"host"`
	Port       int    `xml:"port"`
	User       string `xml:"user"`
	Password   string `xml:"password"`
	DbName     string `xml:"db_name"`
	DataSource string `xml:"-"`
}

type Kafka struct {
	Brokers            []string `xml:"broker"`
	ReceiveStateTopics []string `xml:"receive_state_topic"`
	SendAlarmTopic     string   `xml:"send_alarm_topic"`
}
