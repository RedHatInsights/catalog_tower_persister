package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/mkanoor/catalog_tower_persister/internal/logger"
	"github.com/mkanoor/catalog_tower_persister/internal/models/base"
	"github.com/mkanoor/catalog_tower_persister/internal/models/source"
	"github.com/mkanoor/catalog_tower_persister/internal/models/tenant"
	"github.com/mkanoor/catalog_tower_persister/internal/xrhidentity"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type RefreshStats struct {
	bytesReceived     int64
	refreshStartedAt  time.Time
	refreshFinishedAt time.Time
}

type InventoryContext struct {
	source             *source.Source
	tenant             *tenant.Tenant
	dbTransaction      *gorm.DB
	shutdownReceived   bool
	timeToWait         time.Duration
	refreshStats       RefreshStats
	incrementalRefresh bool
	lastRefreshTime    time.Time
	pageContext        *PageContext
	glog               logger.Logger
}

func startInventoryWorker(ctx context.Context, db DatabaseContext, message UploadRequest, shutdown chan struct{}, wg *sync.WaitGroup) {
	glog := logger.GetLogger(ctx)
	defer glog.Info("Inventory worker terminating")
	defer wg.Done()
	glog.Info("Starting Inventory Worker")
	duration := 15 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	xrh, err := xrhidentity.GetXRHIdentity(message.EncodedXRH)
	if err != nil {
		glog.Errorf("Error parsing XRH Identity %v", err)
		return
	}

	inv := InventoryContext{glog: glog}
	inv.timeToWait = 5 * time.Minute // Wait five minutes for a response
	inv.refreshStats.refreshStartedAt = time.Now().UTC()
	inv.refreshStats.bytesReceived = int64(message.Size)

	uid := "123456789"
	err = inv.setup(db, xrh.Identity.AccountNumber, uid)
	if err != nil {
		inv.glog.Errorf("Error setting up tenant and source %v", err)
		return
	}
	inv.dbTransaction = db.DB.Begin()
	inv.pageContext = MakePageContext(inv.glog, inv.tenant, inv.source, inv.dbTransaction)
	err = inv.process(ctx, message.URL, xrh.Identity.AccountNumber, uid, shutdown)
	inv.refreshStats.refreshFinishedAt = time.Now().UTC()
	if err != nil {
		inv.glog.Errorf("Rolling back database changes %v", err)
		inv.dbTransaction.Rollback()
		inv.updateSource(db, uid, "failed")
	} else {
		inv.dbTransaction.Commit()
		inv.glog.Info("Commited database changes")
		inv.updateSource(db, uid, "success")
	}
}

func (inv *InventoryContext) setup(db DatabaseContext, externalTenant string, uid string) error {
	var err error
	inv.tenant, err = inv.findOrCreateTenant(db, externalTenant)
	if err != nil {
		inv.glog.Errorf("Could not create tenant %v", err)
		return err
	}

	inv.source, err = inv.findOrCreateSource(db, uid)
	if err != nil {
		inv.glog.Errorf("Could not create source %v", err)
		return err
	}

	err = inv.singleRefresh(db)
	if err != nil {
		inv.glog.Errorf("Refresh failed %v", err)
		return err
	}
	return nil
}

func (inv *InventoryContext) process(ctx context.Context, url string, externalTenant string, uid string, shutdown chan struct{}) error {

	inv.glog.Infof("Fetching URL %s", url)

	resp, err := http.Get(url)
	if err != nil {
		inv.glog.Errorf("Error getting URL %s %v", url, err)
		return err
	}
	defer resp.Body.Close()

	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		inv.glog.Errorf("Error opening gzip %v", err)
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
			inv.glog.Errorf("Error reading tar header %v", err)
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeReg:
			inv.glog.Infof("Contents of %s", hdr.Name)
			err = inv.pageContext.Process(hdr.Name, tr)
			if err != nil {
				inv.glog.Errorf("Error handling file %s %v", hdr.Name, err)
				return err
			}
		}

	}
	err = inv.postProcess(ctx)
	if err != nil {
		inv.glog.Errorf("Error post processing data %v", err)
		return err
	}
	return nil

}

func (inv *InventoryContext) postProcess(ctx context.Context) error {
	lh := LinkHandler{PC: inv.pageContext}
	err := lh.Process()
	if err != nil {
		inv.glog.Errorf("Error in linking objects %v", err)
		return err
	}
	dh := DeleteHandler{PC: inv.pageContext}
	err = dh.Process()
	if err != nil {
		inv.glog.Errorf("Error in linking objects %v", err)
		return err
	}
	return nil
}

func (inv *InventoryContext) findOrCreateTenant(db DatabaseContext, v string) (*tenant.Tenant, error) {
	tenant := tenant.Tenant{ExternalTenant: v}
	if result := db.DB.Where("external_tenant = ?", v).First(&tenant); result.Error != nil {
		if result = db.DB.Create(&tenant); result.Error != nil {
			return nil, fmt.Errorf("Error creating tenant: %v" + result.Error.Error())
		}
	}
	return &tenant, nil
}

func (inv *InventoryContext) findOrCreateSource(db DatabaseContext, uid string) (*source.Source, error) {
	source := source.Source{UID: uid, Tenant: *inv.tenant}
	if result := db.DB.Where(&source).First(&source); result.Error != nil {
		if result = db.DB.Create(&source); result.Error != nil {
			return nil, fmt.Errorf("Error creating source: %v", result.Error.Error())
		}
	}

	return &source, nil
}

func (inv *InventoryContext) singleRefresh(db DatabaseContext) error {
	// Only one refresh for a source should be active
	// https://stackoverflow.com/questions/60331946/maintain-integrity-on-concurrent-updates-of-the-same-row/60335740#60335740
	if inv.source.RefreshState == "active" {
		return fmt.Errorf("A refresh is active for this source which was started at : %v", inv.source.RefreshStartedAt)
	}
	if inv.source.RefreshState == "success" && inv.source.LastSuccessfulRefreshAt.Valid {
		inv.incrementalRefresh = true
		inv.lastRefreshTime = inv.source.LastSuccessfulRefreshAt.Time
	}

	inv.source.RefreshStartedAt = sql.NullTime{Valid: true, Time: inv.refreshStats.refreshStartedAt}
	inv.source.RefreshFinishedAt = sql.NullTime{}
	inv.source.RefreshState = "active"
	//db.DB.Save(&inv.Source)
	result := db.DB.Clauses(clause.Locking{Strength: "UPDATE", Options: "NOWAIT"}).Find(&source.Source{Base: base.Base{ID: inv.source.ID}}).Updates(inv.source)
	if result.Error != nil {
		log.Errorf("Error locking source %d %v", inv.source.ID, result.Error)
		return result.Error
	}
	return nil
}

func (inv *InventoryContext) updateSource(db DatabaseContext, uid string, state string) error {
	source := source.Source{UID: uid, Tenant: *inv.tenant}
	result := db.DB.Where(&source).First(&source)
	if result.Error == nil {
		source.RefreshFinishedAt = sql.NullTime{Valid: true, Time: inv.refreshStats.refreshFinishedAt}
		source.BytesReceived = inv.refreshStats.bytesReceived
		source.RefreshState = state
		if state == "success" {
			source.LastSuccessfulRefreshAt = sql.NullTime{Valid: true, Time: inv.refreshStats.refreshStartedAt}
		}
		inv.glog.Infof("Source Info %v", source)
		db.DB.Save(&source)
	}
	return result.Error
}
