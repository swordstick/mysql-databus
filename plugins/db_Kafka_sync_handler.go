package plugins

import (
	"canal"
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"
)

type KafkaKey struct {
	Pos    mysql.Position
	Action string
}

type ProducerToKafkaHandler struct {
	Producer sarama.SyncProducer
	Topic    string
}

func NewKafkaSyncHandler(cfg *canal.Config) *ProducerToKafkaHandler {

	ptk := &ProducerToKafkaHandler{nil, ""}
	var producerobj sarama.SyncProducer
	var err error
	// brokers := []string{"192.168.59.103:9092"}
	// panic: kafka: invalid configuration (Producer.Return.Successes must be true to be used in a SyncProducer)
	// change config to nil @add
	producerobj, err = sarama.NewSyncProducer(cfg.Brokers, nil)
	if err != nil {
		// Should not reach here
		panic(err)
	}

	ptk.Producer = producerobj
	ptk.Topic = cfg.Topic

	return ptk
}

func (h *ProducerToKafkaHandler) Close() {
	h.Producer.Close()
}

func (h *ProducerToKafkaHandler) String() string {
	return "ProducerToKafkaHandler"
}

// func (h *ProducerToKafkaHandler) Do(e *canal.RowsEvent) error {

func (h *ProducerToKafkaHandler) Do(e interface{}, action string) error {

	var body []byte
	var err error

	if action == "DML" {
		value, ok := e.(*canal.RowsEvent)
		if !ok {
			log.Warning("DML type assertion is wrong")
			return fmt.Errorf("TYPE ASSERTION WRONG,%s", action)
		}
		value = canal.Base64RowsEvent(value)
		if body, err = json.Marshal(value); err != nil {
			panic(err.Error())
		}
	}

	if action == "DDL" {
		value, ok := e.(*canal.QueryEvent)
		if !ok {
			log.Warning("DDL type assertion is wrong")
			return fmt.Errorf("TYPE ASSERTION WRONG,%s", action)
		}
		if body, err = json.Marshal(value); err != nil {
			panic(err.Error())
		}
	}

	var head []byte
	var err1 error
	var pos mysql.Position

	pos = canal.MasterSave.Pos()
	key := KafkaKey{Pos: pos, Action: action}

	if head, err1 = json.Marshal(key); err1 != nil {
		panic(err1.Error())
	}

	if err := h.InputMessages(body, head); err != nil {
		panic(err.Error())
	}
	return nil
}

func (h *ProducerToKafkaHandler) InputMessages(body []byte, head []byte) error {

	msg := &sarama.ProducerMessage{
		Key:   sarama.ByteEncoder(head),
		Topic: h.Topic,
		Value: sarama.ByteEncoder(body),
	}
	partition, offset, err := h.Producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}
	log.Debugf("Message is stored in topic(%s),%d,%d \n", h.Topic, partition, offset)
	return err
}
