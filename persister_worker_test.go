package main

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/testhelper"
	"github.com/RedHatInsights/catalog_tower_persister/internal/payload"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

type FakePersister struct {
	loaderCalled      bool
	taskUpdaterCalled bool
	taskUpdaterError  error
	loaderError       error
}

func (fp *FakePersister) ProcessTar(ctx context.Context, logger *logrus.Entry, loader payload.Loader, client *http.Client, dbTransaction *gorm.DB, url string, shutdown chan struct{}) error {
	fp.loaderCalled = true
	return fp.loaderError
}

func (fp *FakePersister) TaskUpdater(logger *logrus.Entry, d map[string]interface{}, client *http.Client) error {
	fp.taskUpdaterCalled = true
	return fp.taskUpdaterError
}

func TestStartWorkerSuccess(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	ctx := context.TODO()
	headers := map[string]string{
		"x-rh-insights-request-id": "abc",
		"x-rh-identity":            "abc",
		"event_type":               "abc",
	}
	shutdown := make(chan struct{})
	tenantID := int64(888)
	sourceID := int64(777)
	tenantMock(mock, tenantID, nil)
	sourceMock(mock, sourceID, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	mp := MessagePayload{TenantID: tenantID,
		SourceID: sourceID,
		TaskURL:  "http://www.example.com",
		DataURL:  "http://www.example.com",
		Size:     int64(900)}
	fp := FakePersister{}

	dc := DatabaseContext{DB: gdb}
	startPersisterWorker(ctx, dc, testhelper.TestLogger(), mp, headers, shutdown, &wg, &fp)
	assert.Equal(t, fp.loaderCalled, true)
	assert.Equal(t, fp.taskUpdaterCalled, true)
}

func TestStartWorkerLoaderFailure(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	ctx := context.TODO()
	headers := map[string]string{
		"x-rh-insights-request-id": "abc",
		"x-rh-identity":            "abc",
		"event_type":               "abc",
	}
	shutdown := make(chan struct{})
	tenantID := int64(888)
	sourceID := int64(777)
	tenantMock(mock, tenantID, nil)
	sourceMock(mock, sourceID, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	mp := MessagePayload{TenantID: tenantID,
		SourceID: sourceID,
		TaskURL:  "http://www.example.com",
		DataURL:  "http://www.example.com",
		Size:     int64(900)}
	fp := FakePersister{loaderError: fmt.Errorf("Kaboom")}

	dc := DatabaseContext{DB: gdb}
	startPersisterWorker(ctx, dc, testhelper.TestLogger(), mp, headers, shutdown, &wg, &fp)

	assert.Equal(t, fp.loaderCalled, true)
	assert.Equal(t, fp.taskUpdaterCalled, true)
}

func TestStartWorkerTenantMissing(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	ctx := context.TODO()
	headers := map[string]string{
		"x-rh-insights-request-id": "abc",
		"x-rh-identity":            "abc",
		"event_type":               "abc",
	}
	shutdown := make(chan struct{})
	tenantID := int64(888)
	sourceID := int64(777)
	tenantMock(mock, tenantID, fmt.Errorf("Kaboom"))
	sourceMock(mock, sourceID, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	mp := MessagePayload{TenantID: tenantID,
		SourceID: sourceID,
		TaskURL:  "http://www.example.com",
		DataURL:  "http://www.example.com",
		Size:     int64(900)}
	fp := FakePersister{}

	dc := DatabaseContext{DB: gdb}
	startPersisterWorker(ctx, dc, testhelper.TestLogger(), mp, headers, shutdown, &wg, &fp)
	assert.Equal(t, fp.loaderCalled, false)
	assert.Equal(t, fp.taskUpdaterCalled, true)
}

func TestStartWorkerSourceMissing(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	ctx := context.TODO()
	headers := map[string]string{
		"x-rh-insights-request-id": "abc",
		"x-rh-identity":            "abc",
		"event_type":               "abc",
	}
	shutdown := make(chan struct{})
	tenantID := int64(888)
	sourceID := int64(777)
	tenantMock(mock, tenantID, nil)
	sourceMock(mock, sourceID, fmt.Errorf("Kaboom"))

	var wg sync.WaitGroup
	wg.Add(1)
	mp := MessagePayload{TenantID: tenantID,
		SourceID: sourceID,
		TaskURL:  "http://www.example.com",
		DataURL:  "http://www.example.com",
		Size:     int64(900)}
	fp := FakePersister{}

	dc := DatabaseContext{DB: gdb}
	startPersisterWorker(ctx, dc, testhelper.TestLogger(), mp, headers, shutdown, &wg, &fp)
	assert.Equal(t, fp.loaderCalled, false)
	assert.Equal(t, fp.taskUpdaterCalled, true)
}

var tenantColumns = []string{"id"}
var sourceColumns = []string{"id"}

func tenantMock(mock sqlmock.Sqlmock, id int64, err error) {
	tStr := `SELECT * FROM "tenants" WHERE "tenants"."id" = $1 ORDER BY "tenants"."id" LIMIT 1`
	if err == nil {
		tRows := sqlmock.NewRows(tenantColumns).AddRow(id)
		mock.ExpectQuery(regexp.QuoteMeta(tStr)).
			WithArgs(id).
			WillReturnRows(tRows)
	} else {
		mock.ExpectQuery(regexp.QuoteMeta(tStr)).
			WithArgs(id).
			WillReturnError(err)
	}
}

func sourceMock(mock sqlmock.Sqlmock, id int64, err error) {
	sStr := `SELECT * FROM "sources" WHERE "sources"."id" = $1 ORDER BY "sources"."id" LIMIT 1`
	if err == nil {
		sRows := sqlmock.NewRows(sourceColumns).AddRow(id)
		mock.ExpectQuery(regexp.QuoteMeta(sStr)).
			WithArgs(id).
			WillReturnRows(sRows)
	} else {
		mock.ExpectQuery(regexp.QuoteMeta(sStr)).
			WithArgs(id).
			WillReturnError(err)
	}
}
