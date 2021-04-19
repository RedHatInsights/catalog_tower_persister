package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/RedHatInsights/catalog_tower_persister/internal/catalogtask"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/source"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/tenant"
	"github.com/RedHatInsights/catalog_tower_persister/internal/payload"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type defaultPersister struct {
	catalogTask catalogtask.CatalogTask
}

// Persister Interface needs to be able to process a Tar file and
// also update the Task in the cloud.redhat.com
type Persister interface {
	ProcessTar(ctx context.Context, logger *logrus.Entry, loader payload.Loader, client *http.Client, dbTransaction *gorm.DB, url string, shutdown chan struct{}) error
	TaskUpdater(logger *logrus.Entry, d map[string]interface{}, client *http.Client) error
}

// startPersisterWorker when a message is received from Kafka we start a
// Persister Worker.
func startPersisterWorker(ctx context.Context, db DatabaseContext, logger *logrus.Entry, message MessagePayload, headers map[string]string, shutdown chan struct{}, wg *sync.WaitGroup, p Persister) {
	defer logger.Info("Persister Worker finished")
	defer wg.Done()
	logger.Info("Persister Worker started")
	duration := 15 * time.Minute
	newCtx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	if p == nil {
		p = &defaultPersister{catalogTask: catalogtask.MakeCatalogTask(ctx, logger, message.TaskURL, headers)}
	}

	tenant, source, err := setup(logger, db, message.TenantID, message.SourceID)
	if err != nil {
		logger.Errorf("Error setting up tenant and source %v", err)
		err = updateTask(logger, "completed", "error", err.Error(), nil, p)
		if err != nil {
			logger.Errorf("Error updating task %v", err)
		}
		return
	}

	err = updateTask(logger, "running", "ok", fmt.Sprintf("Processing file size %d", message.Size), nil, p)
	if err != nil {
		logger.Errorf("Error updating task  to running state %v", err)
		return
	}

	dbTransaction := db.DB.Begin()
	bol := payload.MakeBillOfLading(logger, tenant, source, nil, dbTransaction)
	err = p.ProcessTar(newCtx, logger, bol, &http.Client{}, dbTransaction, message.DataURL, shutdown)
	if err != nil {
		logger.Errorf("Rolling back database changes %v", err)
		dbTransaction.Rollback()
		err = updateTask(logger, "completed", "error", err.Error(), nil, p)
		if err != nil {
			logger.Errorf("Error updating task %v", err)
		}
	} else {
		dbTransaction.Commit()
		logger.Info("Commited database changes")
		err = updateTask(logger, "completed", "ok", "Success", bol.GetStats(newCtx), p)
		if err != nil {
			logger.Errorf("Error updating task %v", err)
		}
	}
}

// setup ensures we have a Tenant and Source object
func setup(logger *logrus.Entry, db DatabaseContext, tenantID int64, sourceID int64) (*tenant.Tenant, *source.Source, error) {
	var err error
	tenant, err := findTenant(db, tenantID)
	if err != nil {
		logger.Errorf("Could not find tenant %v", err)
		return nil, nil, err
	}

	source, err := findSource(db, sourceID)
	if err != nil {
		logger.Errorf("Could not find source %v", err)
		return nil, nil, err
	}

	return tenant, source, nil
}

// findTenant finds a Tenant object from the Database. We get the TenantID from
// the Catalog Inventory API in the Kafka Message Payload
func findTenant(db DatabaseContext, tenantID int64) (*tenant.Tenant, error) {
	tenant := tenant.Tenant{}
	err := db.DB.First(&tenant, tenantID).Error
	if err != nil {
		return nil, fmt.Errorf("Error finding tenant: %v", err)
	}
	return &tenant, nil
}

// findSource finds a Source object from the Database. We get the SourceID from
// the Catalog Inventory API in the Kafka Message Payload
func findSource(db DatabaseContext, sourceID int64) (*source.Source, error) {
	source := source.Source{}
	err := db.DB.First(&source, sourceID).Error
	if err != nil {
		return nil, fmt.Errorf("Error finding source: %v", err)
	}
	return &source, nil
}

// updateTask updates the Task Object in the Catalog Inventory API by making
// a REST API call.
func updateTask(logger *logrus.Entry, state, status, msg string, stats map[string]interface{}, p Persister) error {
	data := map[string]interface{}{"status": status, "state": state, "message": msg}
	if stats != nil {
		data["output"] = map[string]interface{}{"stats": stats}
	}

	if status == "error" {
		data["output"] = map[string]interface{}{"errors": []string{msg}}
	}
	return p.TaskUpdater(logger, data, &http.Client{})
}

// TaskUpdater updates the Task object via REST API
func (dp *defaultPersister) TaskUpdater(logger *logrus.Entry, data map[string]interface{}, client *http.Client) error {
	err := dp.catalogTask.Update(data, client)
	if err != nil {
		logger.Errorf("Error updating catalog task %v", err)
		return err
	}
	return nil
}

// ProcessTar handles a Tar Payload and creates objects in the DB based on the
// files bundled in the compressed tar.
func (dp *defaultPersister) ProcessTar(ctx context.Context, logger *logrus.Entry, loader payload.Loader, client *http.Client, dbTransaction *gorm.DB, url string, shutdown chan struct{}) error {
	return payload.ProcessTar(ctx, logger, loader, client, dbTransaction, url, shutdown)
}
