package canal

import (
	"encoding/json"
	"fmt"

	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"
)

type KafkaKey struct {
	Pos    mysql.Position
	Action string
}

type GetMpos interface {
	GetMasterPos() (string, uint32, error)
}

type Postooffset struct {
	Name       string
	Pos        uint64
	client     sarama.Client
	master     sarama.Consumer
	Poconsumer sarama.PartitionConsumer
	Topic      string
}

func NewPartitionConsumer(Topic string, brokers []string, iscpInit bool) (*Postooffset, error) {

	poffset := &Postooffset{Name: ""}
	// poffset := &Postooffset{Poconsumer: nil}

	poffset.Topic = Topic

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}
	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	if !iscpInit {
		poffset.client = client
		poffset.master = master
		return poffset, nil
	}

	consumer, err := master.ConsumePartition(Topic, 0, sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}

	poffset.client = client
	poffset.master = master
	poffset.Poconsumer = consumer

	return poffset, nil
}

func (p *Postooffset) Parse(Name string, Pos uint64, GetMposer GetMpos) (int64, error) {
	defer p.master.Close()

	p.Name = Name
	p.Pos = Pos

	MasterName, MasterPos, err := GetMposer.GetMasterPos()
	if err != nil {
		return -1, errors.Trace(err)
	}

	comp := p.ComparePos(MasterName, MasterPos)

	offset, err := p.GetNewOffset()
	if err != nil {
		return -1, err
	}

	time.Sleep(5 * time.Second)

	MasterName, MasterPos, err = GetMposer.GetMasterPos()
	if err != nil {
		return -1, errors.Trace(err)
	}

	comp1 := p.ComparePos(MasterName, MasterPos)

	if comp && comp1 {
		return offset, nil
	}

	offsetpos, err := p.SearchAsc()
	if err != nil {
		return 0, err
	}
	return offsetpos, nil
}

func (p *Postooffset) SearchAsc() (int64, error) {

	for {
		select {
		case err := <-p.Poconsumer.Errors():
			fmt.Println(err)
		case msg := <-p.Poconsumer.Messages():
			if myPos := parseJsonKey(msg.Key); myPos != nil {
				valuepos, ok := myPos.(KafkaKey)
				if !ok {
					log.Warning("type assertion is wrong")
					continue
				}
				if p.Name == valuepos.Pos.Name && uint32(p.Pos) == valuepos.Pos.Pos {
					log.Info("Get offset for ", valuepos.Pos.Name, valuepos.Pos.Pos, " is ", msg.Offset)
					return msg.Offset, nil
				}
			}
		}
	}
}

func (p *Postooffset) GetPosKeyWithKafkaOffset(Topic string, thisoffset int64) (string, uint32, error) {

	consumer, err := p.master.ConsumePartition(Topic, 0, thisoffset)
	if err != nil {
		log.Info("GetPosKeyWithKafkaOffset: ConsumePartition exit")
		return "", 0, err
	}

	p.Poconsumer = consumer

	select {
	case err := <-p.Poconsumer.Errors():
		log.Info("GetPosKeyWithKafkaOffset: Poconsumer.Errors exit")
		return "", 0, err
	case msg := <-p.Poconsumer.Messages():
		if myKey := parseJsonKey(msg.Key); myKey != nil {
			valuepos, ok := myKey.(KafkaKey)
			if !ok {
				log.Warning("type assertion is wrong")
				return "", 0, errors.New("invalid value type")
			}
			log.Infof("GetPosKeyWithKafkaOffset: Name: %s,Pos %d", valuepos.Pos.Name, valuepos.Pos.Pos)
			return valuepos.Pos.Name, valuepos.Pos.Pos, nil
		}
	}

	log.Info("GetPosKeyWithKafkaOffset: func body exit")
	return "", 0, errors.New("No useful")
}

func (p *Postooffset) ComparePos(MasterName string, MasterPos uint32) bool {

	if MasterName == p.Name && MasterPos == uint32(p.Pos) {
		return true
	}
	return false
}

func (p *Postooffset) GetNewOffset() (int64, error) {
	lastOffset, err := p.client.GetOffset(p.Topic, 0, sarama.OffsetNewest)
	if err != nil {
		return -1, err
	}
	return lastOffset, nil
}

func (p *Postooffset) GetOldOffset() (int64, error) {
	startOffset, err := p.client.GetOffset(p.Topic, 0, sarama.OffsetOldest)
	if err != nil {
		return -1, err
	}
	return startOffset, nil
}

func parseJsonKey(head []byte) interface{} {
	var pos KafkaKey
	if json.Unmarshal(head, &pos) != nil {
		log.Debug("Wrong Key")
		return nil
	}
	return pos
}
