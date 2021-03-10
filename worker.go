package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/RedHatInsights/catalog_tower_persister/internal/catalogtask"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/source"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/tenant"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type inventoryContext struct {
	source           *source.Source
	tenant           *tenant.Tenant
	dbTransaction    *gorm.DB
	shutdownReceived bool
	timeToWait       time.Duration
	pageContext      *PageContext
	logger           *logrus.Entry
	catalogTask      catalogtask.CatalogTask
}

func startInventoryWorker(ctx context.Context, db DatabaseContext, logger *logrus.Entry, message MessagePayload, headers map[string]string, shutdown chan struct{}, wg *sync.WaitGroup) {
	fmt.Println("Inventory Worker started")
	defer fmt.Println("Inventory Worker finished")
	defer logger.Info("Inventory worker terminating")
	defer wg.Done()
	logger.Info("Starting Inventory Worker")
	duration := 15 * time.Minute
	newCtx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	inv := inventoryContext{logger: logger}
	inv.timeToWait = 5 * time.Minute // Wait five minutes for a response
	inv.catalogTask = catalogtask.MakeCatalogTask(ctx, inv.logger, message.TaskURL, headers)

	err := inv.setup(db, message.TenantID, message.SourceID)
	if err != nil {
		inv.updateTask("completed", "error", err.Error(), nil)
		inv.logger.Errorf("Error setting up tenant and source %v", err)
		return
	}
	inv.updateTask("running", "ok", fmt.Sprintf("Processing file size %d", message.Size), nil)
	inv.dbTransaction = db.DB.Begin()
	inv.pageContext = MakePageContext(inv.logger, inv.tenant, inv.source, inv.dbTransaction)
	err = inv.process(newCtx, message.DataURL, shutdown)
	if err != nil {
		inv.logger.Errorf("Rolling back database changes %v", err)
		inv.dbTransaction.Rollback()
		inv.updateTask("completed", "error", err.Error(), nil)
	} else {
		inv.dbTransaction.Commit()
		inv.logger.Info("Commited database changes")
		inv.updateTask("completed", "ok", "Success", inv.pageContext.GetStats(newCtx))
		inv.pageContext.LogReports(newCtx)
	}
}

func (inv *inventoryContext) setup(db DatabaseContext, tenantID int64, sourceID int64) error {
	var err error
	inv.tenant, err = inv.findTenant(db, tenantID)
	if err != nil {
		inv.logger.Errorf("Could not find tenant %v", err)
		return err
	}

	inv.source, err = inv.findSource(db, sourceID)
	if err != nil {
		inv.logger.Errorf("Could not find source %v", err)
		return err
	}

	return nil
}

func (inv *inventoryContext) process(ctx context.Context, url string, shutdown chan struct{}) error {

	inv.logger.Infof("Fetching URL %s", url)

	resp, err := http.Get(url)
	if err != nil {
		inv.logger.Errorf("Error getting URL %s %v", url, err)
		return err
	}
	defer resp.Body.Close()

	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		inv.logger.Errorf("Error opening gzip %v", err)
		return err
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			inv.logger.Errorf("Error reading tar header %v", err)
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeReg:
			inv.logger.Infof("Contents of %s", hdr.Name)
			err = inv.pageContext.Process(ctx, hdr.Name, tr)
			if err != nil {
				inv.logger.Errorf("Error handling file %s %v", hdr.Name, err)
				return err
			}
		}

	}
	err = inv.postProcess(ctx)
	if err != nil {
		inv.logger.Errorf("Error post processing data %v", err)
		return err
	}
	return nil

}

func (inv *inventoryContext) postProcess(ctx context.Context) error {
	lh := LinkHandler{pageContext: inv.pageContext}
	err := lh.Process()
	if err != nil {
		inv.logger.Errorf("Error in linking objects %v", err)
		return err
	}
	dh := DeleteHandler{pageContext: inv.pageContext}
	err = dh.Process(ctx)
	if err != nil {
		inv.logger.Errorf("Error in linking objects %v", err)
		return err
	}
	return nil
}

func (inv *inventoryContext) findTenant(db DatabaseContext, tenantID int64) (*tenant.Tenant, error) {
	tenant := tenant.Tenant{}
	err := db.DB.First(&tenant, tenantID).Error
	if err != nil {
		return nil, fmt.Errorf("Error finding tenant: %v", err)
	}
	return &tenant, nil
}

func (inv *inventoryContext) findSource(db DatabaseContext, sourceID int64) (*source.Source, error) {
	source := source.Source{}
	err := db.DB.First(&source, sourceID).Error
	if err != nil {
		return nil, fmt.Errorf("Error finding source: %v", err)
	}

	return &source, nil
}

func (inv *inventoryContext) updateTask(state, status, msg string, stats map[string]interface{}) error {
	data := map[string]interface{}{"status": status, "state": state, "message": msg}
	if stats != nil {
		data["output"] = map[string]interface{}{"stats": stats}
	}

	if status == "error" {
		data["output"] = map[string]interface{}{"errors": []string{msg}}
	}
	err := inv.catalogTask.Update(data, &http.Client{})
	if err != nil {
		inv.logger.Errorf("Error updating catalog task %v", err)
		return err
	}
	return nil
}
