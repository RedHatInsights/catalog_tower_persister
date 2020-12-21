package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"encoding/json"

	"github.com/mkanoor/catalog_tower_persister/internal/logger"

	log "github.com/sirupsen/logrus"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type MessagePayload struct {
	TenantID int64  `json:"tenant_id"`
	SourceID int64  `json:"source_id"`
	TaskURL  string `json:"task_url"`
	DataURL  string `json:"data_url"`
	Size     int64  `json:"size"`
}

func startKafkaListener(dbContext DatabaseContext, shutdown chan struct{}, wg *sync.WaitGroup) {

	topic := "platform.catalog.persister"
	defer log.Info("Kafka Listener exiting")
	defer wg.Done()
	ctx := context.Background()
	var counter int

	// --
	// Create Consumer instance
	// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewConsumer

	// Store the config
	cm := kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "catalog_tower_persisters",
	}
	//	"enable.partition.eof": true

	// Variable p holds the new Consumer instance.
	c, e := kafka.NewConsumer(&cm)

	// Check for errors in creating the Consumer
	if e != nil {
		if ke, ok := e.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				log.Errorf("Invalid args to configure kafka code %d %v", ec, e)
			default:
				log.Errorf("Error creating Kafka consure code %d %v", ec, e)
			}
		} else {
			// It's not a kafka.Error
			log.Errorf("Error creating Kafka consumer %v", e.Error())
		}

	} else {
		if e := c.Subscribe(topic, nil); e != nil {
			log.Errorf("Error subscribing to topic %v", e)

		} else {
			doTerm := false
			for !doTerm {
				select {
				case <-shutdown:
					doTerm = true
					break
				default:
					if ev := c.Poll(1000); ev == nil {
						continue
					} else {
						switch ev.(type) {

						case *kafka.Message:
							km := ev.(*kafka.Message)
							messageHeaders := make(map[string]string)
							var messagePayload MessagePayload
							for _, hdr := range km.Headers {
								fmt.Println("Key " + hdr.Key + " Value " + string(hdr.Value))
								switch hdr.Key {
								case "x-rh-identity", "x-rh-insights-request-id", "event_type":
									messageHeaders[hdr.Key] = string(hdr.Value)
								}
							}
							err := json.Unmarshal([]byte(string(km.Value)), &messagePayload)
							if err != nil {
								log.Errorf("Error parsing message" + err.Error())
							} else {
								log.Info("Received Kafka Message")
								log.Infof("#goroutines: %d", runtime.NumGoroutine())
								wg.Add(1)
								counter++
								nctx := logger.CtxWithLoggerID(ctx, counter)
								go startInventoryWorker(nctx, dbContext, messagePayload, messageHeaders, shutdown, wg)
							}

						case kafka.PartitionEOF:
							pe := ev.(kafka.PartitionEOF)
							doTerm = true
							log.Infof("Got to the end of partition %v on topic %v at offset %v\n",

								pe.Partition,
								string(*pe.Topic),
								pe.Offset)
							break

						case kafka.OffsetsCommitted:
							continue

						case kafka.Error:
							em := ev.(kafka.Error)
							log.Infof("Kafka error %v", em)

						default:
							log.Infof("Got an event that's not a Message, Error, or PartitionEOF %v", ev)

						}

					}
				}
			}
			log.Info("Closing Kafka Channel")
			c.Close()
		}
	}

}
