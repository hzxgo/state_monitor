package config

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/cihub/seelog"
)

func init() {
	if err := loadConfig(); err != nil {
		seelog.Errorf("load config err: %v", err)
		os.Exit(0)
		return
	}

	if err := checkConfig(); err != nil {
		seelog.Errorf("check config err: %v", err)
		os.Exit(0)
		return
	}
}

func GetConfig() *Config {
	return currentConfig
}

// ---------------------------------------------------------------------------------------------------------------------

func loadConfig() error {
	var cfg Config
	var configFile string

	env := strings.ToUpper(os.Getenv("ENV"))
	switch env {
	case "DEV":
		configFile = "./conf.d/dev_service.xml"
	case "TEST":
		configFile = "./conf.d/test_service.xml"
	case "BETA":
		configFile = "./conf.d/beta_service.xml"
	case "PRODUCT":
		configFile = "./conf.d/product_service.xml"
	default:
		configFile = "./conf.d/dev_service.xml"
	}

	if err := parseXml(configFile, &cfg); err != nil {
		return err
	}

	currentConfig = &cfg
	return nil
}

func checkConfig() error {

	// set mysql dataSource
	currentConfig.Mysql.DataSource = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=true",
		currentConfig.Mysql.User,
		currentConfig.Mysql.Password,
		currentConfig.Mysql.Host,
		currentConfig.Mysql.Port,
		currentConfig.Mysql.DbName)

	// customer > 0
	if currentConfig.Service.CustomerNum == 0 {
		currentConfig.Service.CustomerNum = 1
	}

	// job pool > 0
	if currentConfig.Service.JobPoolSize == 0 {
		currentConfig.Service.JobPoolSize = 1
	}

	return nil
}

func parseXml(filename string, v interface{}) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	if err = xml.Unmarshal(data, v); err != nil {
		return err
	}

	return nil
}
