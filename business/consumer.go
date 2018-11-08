package business

type Consumer interface {
	Start() error
	Stop() error
}

type ReceiverStateMsg struct {
	JobID       int64  `json:"job_id"`
	ServiceName string `json:"service_name"`
	Status      int    `json:"status"`
	EnvType     int    `json:"env_type"`
	StartTime   int64  `json:"start_time"`
	StopTime    int64  `json:"stop_time"`
	HeartTime   int64  `json:"heart_time"`
	ExitCode    int    `json:"exit_code"`
	Host        string `json:"host"`
	ProcessID   int64  `json:"process_id"`
	Memory      int    `json:"memory"`
	Load        int    `json:"load"`
	NetIn       int64  `json:"net_in"`
	NetOut      int64  `json:"net_out"`
	Extend      string `json:"extend"`
	IsAlarm     bool   `json:"-"`
}

type alarmRequest struct {
	JobID       int64  `json:"job_id"`
	ServiceName string `json:"service_name"`
	HeartTime   int64  `json:"heart_time"`
	Content     string `json:"content"`
}
