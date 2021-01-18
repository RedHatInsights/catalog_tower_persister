package main

import (
	"expvar"
	_ "expvar" // Register the expvar handlers
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	"github.com/RedHatInsights/catalog_tower_persister/config"
	"github.com/RedHatInsights/catalog_tower_persister/internal/logger"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type DatabaseContext struct {
	DB *gorm.DB
}

func main() {
	cfg := config.Get()
	log := logger.InitLogger()
	log.Info("Starting Catalog Tower Persister")
	httpServer := fmt.Sprintf("::%d", cfg.WebPort)
	go http.ListenAndServe(httpServer, http.DefaultServeMux)
	defer log.Info("Finished Catalog Worker")

	expvar.Publish("goroutines", expvar.Func(func() interface{} {
		return fmt.Sprintf("%d", runtime.NumGoroutine())
	}))

	dsn := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.DatabaseUsername,
		cfg.DatabasePassword,
		cfg.DatabaseHostname,
		cfg.DatabasePort,
		cfg.DatabaseName)
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
	go startKafkaListener(dbContext, log, shutdown, &workerGroup)
	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		close(shutdown)
	}()
	workerGroup.Wait()
	fmt.Println("exiting")
}
