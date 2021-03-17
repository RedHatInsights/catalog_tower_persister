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
	"sync/atomic"
	"syscall"

	"github.com/RedHatInsights/catalog_tower_persister/config"
	"github.com/RedHatInsights/catalog_tower_persister/internal/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

//DatabaseContext used to store the DB being used
type DatabaseContext struct {
	DB *gorm.DB
}

func main() {
	cfg := config.Get()
	log := logger.InitLogger()
	log.Info("Starting Catalog Tower Persister")
	defer log.Info("Finished Catalog Worker")

	isReady := &atomic.Value{}
	isReady.Store(false)

	go startPrometheus(cfg)
	go startProbes(cfg, isReady)

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
	go startKafkaListener(dbContext, log, shutdown, &workerGroup, isReady)
	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		close(shutdown)
	}()
	workerGroup.Wait()
	fmt.Println("exiting")
}

func startPrometheus(cfg *config.TowerPersisterConfig) {
	prometheusMux := http.NewServeMux()
	prometheusMux.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(fmt.Sprintf(":%d", cfg.MetricsPort), prometheusMux)
}

func startProbes(cfg *config.TowerPersisterConfig, isReady *atomic.Value) {
	probeMux := http.NewServeMux()

	probeMux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	probeMux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if !isReady.Load().(bool) {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	http.ListenAndServe(fmt.Sprintf(":%d", cfg.WebPort), probeMux)
}
