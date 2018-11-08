package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"state_monitor/model/mysql"
	"state_monitor/model/redis"
)

type StateMonitorPolicy struct {
	mysql.Model
	ID            int64
	JobID         int64
	ServiceName   string
	MonitorPolicy int
	Fields        mysql.NullString
}

// ---------------------------------------------------------------------------------------------------------------------

func NewStateMonitorPolicy() *StateMonitorPolicy {
	return &StateMonitorPolicy{
		Model: mysql.Model{
			TableName: TABLE_STATE_MONITOR_POLICY,
		},
	}
}

func (this *StateMonitorPolicy) DeleteCache(jobId int64, serviceName string) error {
	cacheKey := fmt.Sprintf("%s:%d#%s", RDS_REPORT_STATE_POLICY, jobId, serviceName)
	if err := redis.Del(cacheKey); err != nil {
		return err
	}
	return nil
}

func (this *StateMonitorPolicy) GetFieldsMap(jobId int64, serviceName string) (map[string]string, error) {

	// get values from cache
	cacheKey := fmt.Sprintf("%s:%d#%s", RDS_REPORT_STATE_POLICY, jobId, serviceName)
	ret, _ := redis.Hgetall(cacheKey)
	if len(ret) > 0 {
		return ret, nil
	}

	// get values from mysql
	var policy int
	exps := map[string]interface{}{
		"job_id=?":       jobId,
		"service_name=?": serviceName,
	}
	query := this.Select("monitor_policy, fields").Form(this.TableName)
	if row, err := this.SelectWhere(query, exps); err != nil {
		return nil, err
	} else {
		var fieldsNullString mysql.NullString
		if err = row.Scan(&policy, &fieldsNullString); err != nil {
			if err.Error() != mysql.ErrNoRows.Error() {
				return nil, err
			}
			ret = DefMonitorFields
		}
		if policy == 0 {
			ret = DefMonitorFields
		} else {
			if err := json.Unmarshal([]byte(fieldsNullString.String), &ret); err != nil {
				return nil, err
			}
		}
	}

	// set cache
	ret["monitor_policy"] = strconv.Itoa(policy)
	ret["timestamp"] = strconv.FormatInt(time.Now().Unix(), 10)
	redis.Hmset(cacheKey, ret)

	return ret, nil
}
