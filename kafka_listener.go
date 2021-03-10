package main

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"encoding/json"

	"github.com/RedHatInsights/catalog_tower_persister/config"
	"github.com/google/uuid"

	"github.com/sirupsen/logrus"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

//MessagePayload stores the information sent from the Catalog Inventory API Service
type MessagePayload struct {
	TenantID int64  `json:"tenant_id"`
	SourceID int64  `json:"source_id"`
	TaskURL  string `json:"task_url"`
	DataURL  string `json:"data_url"`
	Size     int64  `json:"size"`
}

func startKafkaListener(dbContext DatabaseContext, logger *logrus.Logger, shutdown chan struct{}, wg *sync.WaitGroup) {

	cfg := config.Get()
	defer logger.Info("Kafka Listener exiting")
	defer wg.Done()
	ctx := context.Background()

	// Store the config
	cm := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.KafkaBrokers, ","),
		"group.id":          cfg.KafkaGroupID,
	}

	c, err := kafka.NewConsumer(&cm)

	// Check for errors in creating the Consumer
	if err != nil {
		if ke, ok := err.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				logger.Errorf("Invalid args to configure kafka code %d %v", ec, err)
			default:
				logger.Errorf("Error creating Kafka consure code %d %v", ec, err)
			}
		} else {
			// It's not a kafka.Error
			logger.Errorf("Error creating Kafka consumer %v", err.Error())
		}

	} else {
		if err := c.Subscribe(cfg.KafkaTopic, nil); err != nil {
			logger.Errorf("Error subscribing to topic %v", err)

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

							processMessage(ctx, dbContext, logger, shutdown, wg, km)
						case kafka.PartitionEOF:
							pe := ev.(kafka.PartitionEOF)
							doTerm = true
							logger.Infof("Got to the end of partition %v on topic %v at offset %v\n",

								pe.Partition,
								string(*pe.Topic),
								pe.Offset)
							break

						case kafka.OffsetsCommitted:
							continue

						case kafka.Error:
							em := ev.(kafka.Error)
							logger.Infof("Kafka error %v", em)

						default:
							logger.Infof("Got an event that's not a Message, Error, or PartitionEOF %v", ev)

						}

					}
				}
			}
			logger.Info("Closing Kafka Channel")
			c.Close()
		}
	}

}

func processMessage(ctx context.Context, dbContext DatabaseContext, logger *logrus.Logger, shutdown chan struct{}, wg *sync.WaitGroup, km *kafka.Message) {
	messageHeaders := make(map[string]string)
	var messagePayload MessagePayload
	requestID := uuid.New().String()
	for _, hdr := range km.Headers {
		switch hdr.Key {
		case "x-rh-insights-request-id":
			messageHeaders[hdr.Key] = string(hdr.Value)
			requestID = string(hdr.Value)
		case "x-rh-identity", "event_type":
			messageHeaders[hdr.Key] = string(hdr.Value)
		}
	}
	logEntry := logger.WithFields(logrus.Fields{"request_id": requestID})

	err := json.Unmarshal([]byte(string(km.Value)), &messagePayload)
	if err != nil {
		logEntry.Errorf("Error parsing message" + err.Error())
	} else {
		logEntry.Info("Received Kafka Message")
		logEntry.Info(stats())
		wg.Add(1)
		ctx := context.Background()
		go startInventoryWorker(ctx, dbContext, logEntry, messagePayload, messageHeaders, shutdown, wg)
	}
}

// stats
func stats() string {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return fmt.Sprintf("Current Stats Alloc = %v MiB TotalAlloc = %v MiB Sys = %v MiB NumGC = %v NumGoroutine = %v",
		bToMb(ms.Alloc), bToMb(ms.TotalAlloc), bToMb(ms.Sys), ms.NumGC, runtime.NumGoroutine())

}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
