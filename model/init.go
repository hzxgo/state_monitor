package model

import (
	"os"

	"state_monitor/config"
	"state_monitor/model/mysql"

	"github.com/cihub/seelog"
)

func init() {
	cfg := config.GetConfig()

	// init mysql
	if err := mysql.Init(cfg.Mysql.DataSource); err != nil {
		seelog.Criticalf("init mysql err %v", err)
		os.Exit(0)
		return
	}

	DefMonitorFields = map[string]string{
		"memory":    "20",
		"status":    "0",
		"exit_code": "2#3",
	}

	seelog.Infof("--------------------------------------------------")
	seelog.Infof("Service Name: %s", service_name)
	seelog.Infof("Service Version: %s", service_version)
	seelog.Infof("Service Release: %s", source_latest_push)
	seelog.Infof("--------------------------------------------------")
}
