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

type persisterContext struct {
	source           *source.Source
	tenant           *tenant.Tenant
	dbTransaction    *gorm.DB
	shutdownReceived bool
	bol              *payload.BillOfLading
	logger           *logrus.Entry
	catalogTask      catalogtask.CatalogTask
}

func startPersisterWorker(ctx context.Context, db DatabaseContext, logger *logrus.Entry, message MessagePayload, headers map[string]string, shutdown chan struct{}, wg *sync.WaitGroup) {
	defer logger.Info("Persister Worker finished")
	defer wg.Done()
	logger.Info("Persister Worker started")
	duration := 15 * time.Minute
	newCtx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	pc := persisterContext{logger: logger}
	pc.catalogTask = catalogtask.MakeCatalogTask(ctx, pc.logger, message.TaskURL, headers)

	err := pc.setup(db, message.TenantID, message.SourceID)
	if err != nil {
		pc.updateTask("completed", "error", err.Error(), nil)
		pc.logger.Errorf("Error setting up tenant and source %v", err)
		return
	}
	pc.updateTask("running", "ok", fmt.Sprintf("Processing file size %d", message.Size), nil)
	pc.dbTransaction = db.DB.Begin()
	pc.bol = payload.MakeBillOfLading(pc.logger, pc.tenant, pc.source, nil, pc.dbTransaction)
	err = payload.ProcessTar(newCtx, pc.logger, pc.bol, &http.Client{}, pc.dbTransaction, message.DataURL, shutdown)
	if err != nil {
		pc.logger.Errorf("Rolling back database changes %v", err)
		pc.dbTransaction.Rollback()
		pc.updateTask("completed", "error", err.Error(), nil)
	} else {
		pc.dbTransaction.Commit()
		pc.logger.Info("Commited database changes")
		pc.updateTask("completed", "ok", "Success", pc.bol.GetStats(newCtx))
	}
}

func (pc *persisterContext) setup(db DatabaseContext, tenantID int64, sourceID int64) error {
	var err error
	pc.tenant, err = pc.findTenant(db, tenantID)
	if err != nil {
		pc.logger.Errorf("Could not find tenant %v", err)
		return err
	}

	pc.source, err = pc.findSource(db, sourceID)
	if err != nil {
		pc.logger.Errorf("Could not find source %v", err)
		return err
	}

	return nil
}

func (pc *persisterContext) findTenant(db DatabaseContext, tenantID int64) (*tenant.Tenant, error) {
	tenant := tenant.Tenant{}
	err := db.DB.First(&tenant, tenantID).Error
	if err != nil {
		return nil, fmt.Errorf("Error finding tenant: %v", err)
	}
	return &tenant, nil
}

func (pc *persisterContext) findSource(db DatabaseContext, sourceID int64) (*source.Source, error) {
	source := source.Source{}
	err := db.DB.First(&source, sourceID).Error
	if err != nil {
		return nil, fmt.Errorf("Error finding source: %v", err)
	}

	return &source, nil
}

func (pc *persisterContext) updateTask(state, status, msg string, stats map[string]interface{}) error {
	data := map[string]interface{}{"status": status, "state": state, "message": msg}
	if stats != nil {
		data["output"] = map[string]interface{}{"stats": stats}
	}

	if status == "error" {
		data["output"] = map[string]interface{}{"errors": []string{msg}}
	}
	err := pc.catalogTask.Update(data, &http.Client{})
	if err != nil {
		pc.logger.Errorf("Error updating catalog task %v", err)
		return err
	}
	return nil
}
