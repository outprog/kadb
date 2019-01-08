package kadb

import (
	"os"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/inconshreveable/log15"
	"github.com/jinzhu/gorm"
)

var logger = log15.New(log15.Ctx{"module": "kadb"})

type Kadb struct {
	consumer  *cluster.Consumer
	db        *gorm.DB
	interrupt chan os.Signal

	msgChan    chan *sarama.ConsumerMessage
	msgDecoder func(key, value []byte) (interface{}, error)
}

func New(
	consumer *cluster.Consumer,
	db *gorm.DB,
	interrupt chan os.Signal,
	concurrencyLimit int,
) *Kadb {
	return &Kadb{
		consumer:  consumer,
		db:        db,
		interrupt: interrupt,

		msgChan: make(chan *sarama.ConsumerMessage, concurrencyLimit),
	}
}

func (k *Kadb) Run(msgDecoder func(key, value []byte) (interface{}, error)) {
	k.msgDecoder = msgDecoder

	go k.loopProcess()
	k.loopReceive()
}

func (k *Kadb) toDB(msg *sarama.ConsumerMessage) error {
	dMsg, err := k.msgDecoder(msg.Key, msg.Value)
	if err != nil {
		return err
	}
	k.db.Create(dMsg)
	return nil
}

func (k *Kadb) loopReceive() {
	defer k.consumer.Close()

	for {
		select {
		case msg, ok := <-k.consumer.Messages():
			if ok {
				// fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				k.msgChan <- msg
			}
		case <-k.interrupt:
			close(k.msgChan)
			logger.Info("consumer closed")
			return
		}
	}
}

func (k *Kadb) loopProcess() {
	for msg := range k.msgChan {
		if err := k.toDB(msg); err != nil {
			logger.Warn("process failed", "err", err)
		}
		k.consumer.MarkOffset(msg, "") // mark message as processed
	}
	k.db.Close()
	logger.Info("db closed")
	logger.Info("process over")
}
