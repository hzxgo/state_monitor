package model

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"state_monitor/config"
	"state_monitor/model/mysql"

	"github.com/cihub/seelog"
)

type ReportState struct {
	mysql.Model
}

// ---------------------------------------------------------------------------------------------------------------------

func NewReportState() *ReportState {
	return &ReportState{
		Model: mysql.Model{
			TableName: TABLE_REPORT_STATE_PRE + time.Now().Format("200601"),
		},
	}
}

func (this *ReportState) RollingBatchInsert(columns []string, params []interface{}) (int64, error) {
	this.TableName = TABLE_REPORT_STATE_PRE + time.Now().Format("200601")
	id, err := this.BatchInsert(columns, params)
	if err != nil {
		if strings.HasPrefix(err.Error(), "Error 1146") {
			if err = this.createTable(); err != nil {
				return 0, err
			}
			if id, err = this.BatchInsert(columns, params); err != nil {
				return 0, err
			}
		}

		// delete expired table
		maxRolls := config.GetConfig().Service.MaxStoreMonths
		if err = this.rollTables(maxRolls); err != nil {
			seelog.Errorf("delete expired report_alarm_x table err: %v", err)
		}
	}

	return id, nil
}

// ---------------------------------------------------------------------------------------------------------------------

func (this *ReportState) createTable() error {
	this.TableName = TABLE_REPORT_STATE_PRE + time.Now().Format("200601")
	cmd := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` ( "+
		"`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'id', "+
		"`job_id` bigint(20) DEFAULT '0' COMMENT '服务ID', "+
		"`service_name` varchar(127) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '服务名称', "+
		"`status` tinyint(1) DEFAULT '1' COMMENT '服务状态：0.异常、1.正常', "+
		"`env_type` tinyint(1) DEFAULT '0' COMMENT '环境类型：0.开发环境、1.测试环境、2.集成环境、3.预发环境、4.生产环境', "+
		"`start_time` bigint(20) DEFAULT '0' COMMENT '启动时间戳', "+
		"`stop_time` bigint(20) DEFAULT '0' COMMENT '结束时间戳', "+
		"`heart_time` bigint(20) DEFAULT '0' COMMENT '心跳时间戳', "+
		"`exit_code` tinyint(1) DEFAULT '0' COMMENT '服务退出状态：0.未退出、1.正常退出、2.异常退出、3.kill by admin', "+
		"`host` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '主机IP', "+
		"`process_id` int(11) DEFAULT '0' COMMENT '进程ID', "+
		"`memory` int(11) DEFAULT '0' COMMENT '占用内存百分比', "+
		"`load` int(11) DEFAULT '0' COMMENT '占用机器负载百分比', "+
		"`net_in` bigint(20) DEFAULT '0' COMMENT '网络流入量（程序内部计算网络传输，累加值）', "+
		"`net_out` bigint(20) DEFAULT '0' COMMENT '网络流出量（程序内部计算网络传输，累加值）', "+
		"`extend` text CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '扩展字段', "+
		"`is_alarm` tinyint(1) DEFAULT '0' COMMENT '报警标识：0.未报警、1.已报警', "+
		"`create_time` bigint(20) DEFAULT '0' COMMENT '创建时间戳', "+
		"PRIMARY KEY (`id`), "+
		"KEY `idx_job_id` (`job_id`) USING BTREE, "+
		"KEY `idx_service_name` (`service_name`) USING BTREE, "+
		"KEY `idx_status` (`status`) USING BTREE, "+
		"KEY `idx_memory` (`memory`) USING BTREE"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8;", this.TableName)

	_, err := this.GetDB().Query(cmd)
	if err != nil {
		return err
	}

	return nil
}

func (this *ReportState) rollTables(maxRolls uint32) error {
	if maxRolls == 0 {
		return nil
	}

	cmd := fmt.Sprintf("SHOW TABLES LIKE '%s%s'", TABLE_REPORT_STATE_PRE, "%")
	threshold, _ := strconv.Atoi(time.Now().AddDate(0, -int(maxRolls), 0).Format("200601"))

	if rows, err := this.GetDB().Query(cmd); err != nil {
		return err
	} else {
		for rows.Next() {
			var table string
			if err = rows.Scan(&table); err != nil {
				return err
			}

			month, err := strconv.Atoi(table[len(TABLE_REPORT_STATE_PRE):])
			if err == nil && month < threshold {
				if _, err = this.GetDB().Query("DROP TABLE " + table); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
