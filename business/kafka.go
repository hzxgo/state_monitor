package business

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"state_monitor/config"
	"state_monitor/model"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/cihub/seelog"
)

type Kafka struct {
	l                  sync.Mutex                   // 锁
	consumer           *cluster.Consumer            // 消费者
	producer           sarama.AsyncProducer         // 生产者
	produceTopic       string                       // 生产者的主题
	chanConsumerMsg    chan *sarama.ConsumerMessage // 消费消息通道
	chanProducerValue  chan string                  // 生产消息的内容通道
	chanExit           chan struct{}                // 携程退出消息通道
	reportStateModel   *model.ReportState           // 上报状态模型
	monitorPolicyModel *model.StateMonitorPolicy    // 监控策略模型
}

type cache struct {
	l                 sync.Mutex    // 锁
	firstMsgTimestamp int64         // 第一条消息存入的时间戳
	values            []interface{} // 批量操作的对象值
}

var (
	msgCache *cache // 消费的消息不直接写MySQL，而是写入缓存，然后批量写入MySQL
)

func init() {
	msgCache = &cache{
		values: make([]interface{}, 0, model.BATCH_INSERT_CAPS),
	}
}

// ---------------------------------------------------------------------------------------------------------------------

func NewKafka(brokers, consumerTopics []string, producerTopic string) (*Kafka, error) {

	// create kafka produce
	produceConfig := sarama.NewConfig()
	produceConfig.Producer.RequiredAcks = sarama.WaitForAll
	produceConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	produceConfig.Producer.Return.Successes = true
	produceConfig.Producer.Return.Errors = true
	produceConfig.Version = sarama.V0_11_0_2
	producer, err := sarama.NewAsyncProducer(brokers, produceConfig)
	if err != nil {
		return nil, err
	}

	// create kafka consumer
	groupId := "state_monitor_center"
	consumerConfig := cluster.NewConfig()
	consumerConfig.Consumer.Return.Errors = true
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	consumer, err := cluster.NewConsumer(brokers, groupId, consumerTopics, consumerConfig)
	if err != nil {
		return nil, err
	}

	return &Kafka{
		consumer:           consumer,
		producer:           producer,
		produceTopic:       producerTopic,
		chanExit:           make(chan struct{}),
		chanConsumerMsg:    make(chan *sarama.ConsumerMessage, model.CHAN_CONSUMER_MSG_CAPS),
		chanProducerValue:  make(chan string, model.CHAN_CONSUMER_MSG_CAPS),
		reportStateModel:   model.NewReportState(),
		monitorPolicyModel: model.NewStateMonitorPolicy(),
	}, nil
}

func (this *Kafka) Start() error {

	// receiver msg from kafka
	go this.receiver()

	// consumer msg
	for i := 0; i < int(config.GetConfig().Service.JobPoolSize); i++ {
		go this.consumerMsg()
	}

	// alarm from state_monitor_center to alarm_monitor_center
	go this.alarm()

	return nil
}

func (this *Kafka) Stop() error {
	this.l.Lock()
	defer this.l.Unlock()

	close(this.chanExit)
	close(this.chanConsumerMsg)
	close(this.chanProducerValue)

	if err := this.consumer.Close(); err != nil {
		seelog.Errorf("close kafka consumer err: %v", err)
	}

	if err := this.producer.Close(); err != nil {
		seelog.Errorf("close kafka consumer err: %v", err)
	}

	// 将消息缓存清空输出至 MySQL
	if err := this.flushMsgCache(); err != nil {
		seelog.Errorf("flush msg cache to mysql error: %v", err)
	}

	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

func (this *Kafka) receiver() {
	var err error
	var msg *sarama.ConsumerMessage

	for {
		select {
		case <-this.chanExit:
			return
		case err = <-this.consumer.Errors():
			if err != nil {
				seelog.Errorf("consumer receiver err: %v", err)
			}
		case msg = <-this.consumer.Messages():
			this.chanConsumerMsg <- msg
			this.consumer.MarkOffset(msg, "")
		}
	}
}

func (this *Kafka) consumerMsg() {
	var err error
	var content string
	var stateObj ReceiverStateMsg
	var msg *sarama.ConsumerMessage
	var isNeed, isNotClosed bool

	for {
		select {
		case msg, isNotClosed = <-this.chanConsumerMsg:
			if !isNotClosed {
				return
			}
			if err = json.Unmarshal(msg.Value, &stateObj); err != nil {
				seelog.Errorf("json unmarshal failed. [T:%s P:%d O:%d M:%s], err: %v",
					msg.Topic, msg.Partition, msg.Offset, string(msg.Value), err)
				continue
			}
			if content, isNeed = this.isNeedAlarm(&stateObj); isNeed {
				obj := alarmRequest{
					JobID:       stateObj.JobID,
					ServiceName: stateObj.ServiceName,
					HeartTime:   time.Now().Unix(),
					Content: fmt.Sprintf("JobID: %d, ServiceName: %s, Msg: %s",
						stateObj.JobID, stateObj.ServiceName, content),
				}
				alarmPkg, _ := json.Marshal(obj)
				this.chanProducerValue <- string(alarmPkg)
			}

			// 存储消息
			stateObj.IsAlarm = isNeed
			if err = this.store(&stateObj); err != nil {
				seelog.Errorf("insert state err: %v", err)
				continue
			}

			seelog.Infof("consumer report_state msg ok [T:%s P:%d O:%d M:%s]",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		}
	}
}

func (this *Kafka) store(stateObj *ReceiverStateMsg) error {
	if stateObj == nil || stateObj.ServiceName == "" {
		return errors.New("params error, stateObj is null or serviceName is empty")
	}

	columns := []string{
		"job_id", "service_name", "`status`", "env_type", "start_time", "stop_time", "heart_time", "exit_code",
		"`host`", "process_id", "memory", "`load`", "net_in", "net_out", "extend", "is_alarm", "create_time",
	}
	value := []interface{}{
		stateObj.JobID, stateObj.ServiceName, stateObj.Status, stateObj.EnvType,
		stateObj.StartTime, stateObj.StopTime, stateObj.HeartTime, stateObj.ExitCode,
		stateObj.Host, stateObj.ProcessID, stateObj.Memory, stateObj.Load,
		stateObj.NetIn, stateObj.NetOut, stateObj.Extend, stateObj.IsAlarm, time.Now().Unix(),
	}

	msgCache.l.Lock()
	defer msgCache.l.Unlock()

	length := len(msgCache.values)
	diffTime := time.Now().Unix() - msgCache.firstMsgTimestamp - model.BATCH_INSERT_INTERVAL_TIME

	seelog.Warnf("diffTime: %d, firstMsgTimestamp: %d", diffTime, msgCache.firstMsgTimestamp)

	if length >= model.BATCH_INSERT_CAPS {
		seelog.Warnf("0000")
		_, err := this.reportStateModel.RollingBatchInsert(columns, msgCache.values)
		if err != nil {
			return err
		}

		// 复位 msgCache
		this.resetMsgCache()

		// 将消息写入缓存并记录时间
		msgCache.values = append(msgCache.values, value)
		if msgCache.firstMsgTimestamp == 0 {
			msgCache.firstMsgTimestamp = time.Now().Unix()
		}
	} else if diffTime > 0 && msgCache.firstMsgTimestamp > 0 {
		seelog.Warnf("1111")
		msgCache.values = append(msgCache.values, value)
		id, err := this.reportStateModel.RollingBatchInsert(columns, msgCache.values)
		if err != nil {
			return err
		}

		seelog.Warnf("insert lastid: %d", id)

		// 复位 msgCache
		this.resetMsgCache()
	} else {
		seelog.Warnf("2222")
		msgCache.values = append(msgCache.values, value)
		if msgCache.firstMsgTimestamp == 0 {
			msgCache.firstMsgTimestamp = time.Now().Unix()
		}
	}

	return nil
}

// 将缓存中的数据写入到 MySQL（该kafka退出前执行）
func (this *Kafka) flushMsgCache() error {
	msgCache.l.Lock()
	defer msgCache.l.Unlock()

	if len(msgCache.values) > 0 {
		columns := []string{
			"job_id", "service_name", "status", "env_type", "start_time", "stop_time", "heart_time",
			"exit_code", "host", "process_id", "memory", "load", "net_in", "net_out", "extend", "create_time",
		}
		_, err := this.reportStateModel.RollingBatchInsert(columns, msgCache.values)
		if err != nil {
			return err
		}

		// 复位 msgCache
		this.resetMsgCache()
	}

	return nil
}

// 复位消息缓存
func (this *Kafka) resetMsgCache() {
	msgCache.firstMsgTimestamp = 0
	msgCache.values = msgCache.values[0:0]
}

func (this *Kafka) isNeedAlarm(stateObj *ReceiverStateMsg) (string, bool) {
	if stateObj == nil || stateObj.ServiceName == "" {
		return "", false
	}

	m, err := this.monitorPolicyModel.GetFieldsMap(stateObj.JobID, stateObj.ServiceName)
	if err != nil {
		seelog.Errorf("get state monitor policy fields err: %v", err)
		return "", false
	}

	if v, ok := m["memory"]; ok {
		memory, _ := strconv.Atoi(v)
		if stateObj.Memory > memory {
			return fmt.Sprintf("memory usage is too high, usage: %d", stateObj.Memory), true
		}
	}

	if v, ok := m["status"]; ok {
		status, _ := strconv.Atoi(v)
		if stateObj.Status == model.REPORT_STATE_COM_STATUS_FAILED && stateObj.Status == status {
			return "service status exception", true
		}
	}

	if v, ok := m["exit_code"]; ok {
		s := strings.Split(v, "#")
		for _, value := range s {
			exitCode, _ := strconv.Atoi(value)
			if stateObj.ExitCode > model.REPORT_STATE_COM_EXIT_CODE_EXIT_OK && stateObj.ExitCode == exitCode {
				return "service exit exception", true
			}
		}
	}

	// service exit, delete redis cache
	if stateObj.ExitCode >= model.REPORT_STATE_COM_EXIT_CODE_EXIT_OK {
		this.monitorPolicyModel.DeleteCache(stateObj.JobID, stateObj.ServiceName)
	}

	return "", false
}

func (this *Kafka) alarm() {

	go func() {
		var value []byte
		var suc *sarama.ProducerMessage
		var fail *sarama.ProducerError

		for {
			select {
			case <-this.chanExit:
				return
			case suc = <-this.producer.Successes():
				value, _ = suc.Value.Encode()
				seelog.Infof("send alarm msg success. offset: %d, msgValue: %+v", suc.Offset, string(value))
			case fail = <-this.producer.Errors():
				seelog.Infof("send alarm msg failed: %s", fail.Err.Error())
			}
		}
	}()

	go func() {
		var value string
		var isNotClosed bool
		msg := &sarama.ProducerMessage{
			Topic: this.produceTopic,
			Key:   sarama.StringEncoder("state_monitor_center"),
		}

		for {
			select {
			case value, isNotClosed = <-this.chanProducerValue:
				if !isNotClosed {
					return
				}

				msg.Value = sarama.ByteEncoder(value)

				// send msg
				this.producer.Input() <- msg
			}
		}
	}()
}
