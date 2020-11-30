package main

import (
	"expvar"
	_ "expvar" // Register the expvar handlers
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"

	log "github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type DatabaseContext struct {
	DB *gorm.DB
}

func main() {
	go http.ListenAndServe(":7070", http.DefaultServeMux)
	logFileName := "/tmp/catalog_tower_persister" + strconv.Itoa(os.Getpid()) + ".log"
	logf, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("error opening log file: %v", err)
	}

	defer logf.Close()
	defer log.Info("Finished Catalog Worker")

	expvar.Publish("goroutines", expvar.Func(func() interface{} {
		return fmt.Sprintf("%d", runtime.NumGoroutine())
	}))
	log.SetOutput(logf)
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		panic("DATABASE_URL environment variable not set")
	}

	sigs := make(chan os.Signal, 1)
	shutdown := make(chan struct{})
	var workerGroup sync.WaitGroup
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	fmt.Println("Connected to database")

	dbContext := DatabaseContext{DB: db}

	workerGroup.Add(1)
	go startKafkaListener(dbContext, shutdown, &workerGroup)
	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		close(shutdown)
	}()
	workerGroup.Wait()
	fmt.Println("exiting")
}
