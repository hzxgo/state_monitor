package model

var (
	DefMonitorFields map[string]string
)

const (
	service_name       = "state_monitor_center"
	service_version    = "V0.0.1"
	source_latest_push = "2018-11-08"
)

// table name
const (
	TABLE_REPORT_STATE_PRE     = "report_state_"        // 上报状态表前缀
	TABLE_REPORT_ALARM_PRE     = "report_alarm_"        // 上报警告表前缀
	TABLE_STATE_MONITOR_POLICY = "state_monitor_policy" // 状态接听策略
)

// redis key
const (
	RDS_REPORT_STATE_POLICY = "monitor:state:policy" // 监控状态策略
)

// report_state state
const (
	REPORT_STATE_COM_STATUS_OK     = 1 // 通用字段：服务状态正常
	REPORT_STATE_COM_STATUS_FAILED = 0 // 通用字段：服务状态异常

	REPORT_STATE_COM_ENV_TYPE_DEV  = 0 // 通用字段：开发环境
	REPORT_STATE_COM_ENV_TYPE_TEST = 1 // 通用字段：测试环境
	REPORT_STATE_COM_ENV_TYPE_IDE  = 2 // 通用字段：集成环境
	REPORT_STATE_COM_ENV_TYPE_PRE  = 3 // 通用字段：预发环境
	REPORT_STATE_COM_ENV_TYPE_PRO  = 4 // 通用字段：生产环境

	REPORT_STATE_COM_EXIT_CODE_UNEXIT      = 0 // 通用字段：未退出
	REPORT_STATE_COM_EXIT_CODE_EXIT_OK     = 1 // 通用字段：正常退出
	REPORT_STATE_COM_EXIT_CODE_EXIT_FAILED = 2 // 通用字段：异常退出
	REPORT_STATE_COM_EXIT_CODE_EXIT_KILL   = 3 // 通用字段：kill by admin
)

// others
const (
	CHAN_CONSUMER_MSG_CAPS     = 100 // 消费者的消息通道容量
	BATCH_INSERT_CAPS          = 200 // 批量插入最大容量
	BATCH_INSERT_INTERVAL_TIME = 90  // 单位秒
)
