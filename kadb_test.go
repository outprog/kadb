package kadb

import (
	"encoding/json"
	"os"
	"os/signal"
	"testing"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

func TestKadb(t *testing.T) {
	// init consumer
	config := cluster.NewConfig()
	consumer, err := cluster.NewConsumer([]string{"127.0.0.1:9092"}, "test-group", []string{"tokenlon.mm.order"}, config)
	if err != nil {
		panic(err)
	}

	// init db
	db, err := gorm.Open("mysql", "root@tcp(localhost:3306)/test")
	if err != nil {
		panic("failed to connect database")
	}

	// define schema
	// from kakfa
	type KaSource struct {
		SourceNode string `json:"sourceNode"`
		SessionId  string `json:"sessionId"`
	}
	// to sql ORM
	type KaSink struct {
		SourceNode string `gorm:"primary_key"`
		SessionId  string
	}
	// migration
	db.AutoMigrate(&KaSink{})

	// build kadb, decode topic msg to db
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	kdb := New(consumer, db, signals, 3)
	kdb.Run(func(key, value []byte) (interface{}, error) {

		// unmarshal source from kafka topic
		source := new(KaSource)
		if err := json.Unmarshal(value, &source); err != nil {
			return nil, err
		}

		// to flat struct, saved to db
		return &KaSink{
			SourceNode: source.SourceNode,
			SessionId:  source.SessionId,
		}, nil

	})
}
