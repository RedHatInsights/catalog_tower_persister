package serviceofferingnode

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/RedHatInsights/catalog_tower_persister/internal/logger"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/testhelper"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

var objectType = "workflow_job_template_node"

func makeDefaultAttrs(srcRef string, created string, jobtype string) map[string]interface{} {
	return map[string]interface{}{
		"created":               created,
		"modified":              "2020-01-08T10:22:59.423585Z",
		"id":                    json.Number(srcRef),
		"workflow_job_template": json.Number("12"),
		"unified_job_template":  json.Number("10"),
		"unified_job_type":      jobtype,
		"type":                  objectType,
		"inventory":             "/api/v2/inventories/2/",
	}
}

var columns = []string{"id", "tenant_id", "source_id", "source_ref", "name",
	"source_created_at", "created_at", "updated_at"}
var tenantID = int64(99)
var sourceID = int64(1)

func TestBadDateTime(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sonr := NewGORMRepository(gdb)
	attrs := makeDefaultAttrs("4", "gobbledegook", "job")
	son := ServiceOfferingNode{SourceID: sourceID, TenantID: tenantID}
	err := sonr.CreateOrUpdate(nctx, &son, attrs)
	checkErrors(t, err, mock, sonr, "Parsing time error", "parsing time")
}

func TestCreateMissingParams(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sonr := NewGORMRepository(gdb)
	son := ServiceOfferingNode{SourceID: sourceID, TenantID: tenantID}
	attrs := map[string]interface{}{
		"modified": "2020-01-08T10:22:59.423585Z",
		"id":       json.Number("4"),
		"name":     "demo",
		"type":     objectType,
	}
	err := sonr.CreateOrUpdate(nctx, &son, attrs)
	checkErrors(t, err, mock, sonr, "Expecting invalid attributes", "Missing Required Attribute created")
}

func TestCreateErrorLocatingRecord(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sonr := NewGORMRepository(gdb)
	srcRef := "4"
	attrs := makeDefaultAttrs(srcRef, "2020-01-08T10:22:59.423585Z", "job")
	son := ServiceOfferingNode{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_offering_nodes" WHERE "service_offering_nodes"."source_ref" = $1 AND "service_offering_nodes"."source_id" = $2 AND "service_offering_nodes"."archived_at" IS NULL ORDER BY "service_offering_nodes"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(fmt.Errorf("kaboom"))

	err := sonr.CreateOrUpdate(nctx, &son, attrs)
	checkErrors(t, err, mock, sonr, "Expecting create failure", "kaboom")
}

func TestCreateError(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sonr := NewGORMRepository(gdb)
	srcRef := "4"
	attrs := makeDefaultAttrs(srcRef, "2020-01-08T10:22:59.423585Z", "job")
	str := `SELECT * FROM "service_offering_nodes" WHERE "service_offering_nodes"."source_ref" = $1 AND "service_offering_nodes"."source_id" = $2 AND "service_offering_nodes"."archived_at" IS NULL ORDER BY "service_offering_nodes"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(gorm.ErrRecordNotFound)

	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "service_offering_nodes"`)).
		WithArgs(testhelper.AnyTime{}, testhelper.AnyTime{}, nil, srcRef, sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), tenantID, sourceID).
		WillReturnError(fmt.Errorf("kaboom"))

	son := ServiceOfferingNode{SourceID: sourceID, TenantID: tenantID}
	err := sonr.CreateOrUpdate(nctx, &son, attrs)
	checkErrors(t, err, mock, sonr, "Expecting create failure", "kaboom")
}

func TestCreateIgnoreObject(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sonr := NewGORMRepository(gdb)
	srcRef := "4"
	attrs := makeDefaultAttrs(srcRef, "2020-01-08T10:22:59.423585Z", "inventory_update")
	son := ServiceOfferingNode{SourceID: sourceID, TenantID: tenantID}
	err := sonr.CreateOrUpdate(nctx, &son, attrs)
	errMsg := "Ignoring non job template or workflow job template nodes"
	checkErrors(t, err, mock, sonr, "Expecting create failure", errMsg)
}

func TestCreate(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	//	extra := map[string]interface{}{"unified_job_type": "job"}
	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sonr := NewGORMRepository(gdb)
	srcRef := "4"
	attrs := makeDefaultAttrs(srcRef, "2020-01-08T10:22:59.423585Z", "job")
	newID := int64(78)
	son := ServiceOfferingNode{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_offering_nodes" WHERE "service_offering_nodes"."source_ref" = $1 AND "service_offering_nodes"."source_id" = $2 AND "service_offering_nodes"."archived_at" IS NULL ORDER BY "service_offering_nodes"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(gorm.ErrRecordNotFound)

	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "service_offering_nodes"`)).
		WithArgs(testhelper.AnyTime{}, testhelper.AnyTime{}, nil, srcRef, sqlmock.AnyArg(),
			nil, sqlmock.AnyArg(), sqlmock.AnyArg(), tenantID, sourceID).
		WillReturnRows(sqlmock.NewRows([]string{"service_offering_id", "root_service_offering_id", "service_inventory_id", "id"}).AddRow(5, 6, 7, newID))
	err := sonr.CreateOrUpdate(nctx, &son, attrs)
	assert.Nil(t, err, "CreateOrUpdate failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations")
	stats := sonr.Stats()
	assert.Equal(t, stats["adds"], 1)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 0)
	// TODO: Since the order of the returning is not guranteed in GORM we can't check the ID
	// Its most probably happening because they are using maps to store fields and the order of the
	// keys when retrieving a map is not guaranteed
	// assert.Equal(t, sc.ID, newID)

}

func TestCreateOrUpdateError(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	srcRef := "4"
	attrs := makeDefaultAttrs(srcRef, "2020-01-08T10:22:59.423585Z", "job")
	id := int64(1)
	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", time.Now(), time.Now(), time.Now())
	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sonr := NewGORMRepository(gdb)
	son := ServiceOfferingNode{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_offering_nodes" WHERE "service_offering_nodes"."source_ref" = $1 AND "service_offering_nodes"."source_id" = $2 AND "service_offering_nodes"."archived_at" IS NULL ORDER BY "service_offering_nodes"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)
	mock.ExpectExec("^UPDATE").WillReturnError(fmt.Errorf("kaboom"))

	err := sonr.CreateOrUpdate(nctx, &son, attrs)

	checkErrors(t, err, mock, sonr, "Expecting CreateUpdate Error", "kaboom")
}

func TestCreateOrUpdate(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "4"
	attrs := makeDefaultAttrs(srcRef, "2020-01-08T10:22:59.423585Z", "job")
	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", time.Now(), time.Now(), time.Now())
	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sonr := NewGORMRepository(gdb)
	son := ServiceOfferingNode{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_offering_nodes" WHERE "service_offering_nodes"."source_ref" = $1 AND "service_offering_nodes"."source_id" = $2 AND "service_offering_nodes"."archived_at" IS NULL ORDER BY "service_offering_nodes"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)
	mock.ExpectExec("^UPDATE").WillReturnResult(sqlmock.NewResult(100, 1))
	err := sonr.CreateOrUpdate(nctx, &son, attrs)

	assert.Nil(t, err, "CreateOrUpdate failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations")
	stats := sonr.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 1)
	assert.Equal(t, stats["deletes"], 0)

}

func TestDeleteUnwantedMissing(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	sourceRef := "2"

	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, sourceRef, "Test", time.Now(), time.Now(), time.Now())

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sonr := NewGORMRepository(gdb)
	son := ServiceOfferingNode{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_offering_nodes" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)
	sourceRefs := []string{sourceRef}
	err := sonr.DeleteUnwanted(nctx, &son, sourceRefs)

	assert.Nil(t, err, "DeleteUnwantedMissing failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for DeleteUnwanted")
	stats := sonr.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 0)
}

func TestDeleteUnwanted(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	sourceRef := "2"

	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, sourceRef, "Test", time.Now(), time.Now(), time.Now())

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sonr := NewGORMRepository(gdb)
	son := ServiceOfferingNode{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_offering_nodes" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)

	markAsArchived := `UPDATE "service_offering_nodes" SET "archived_at"=$1 WHERE "service_offering_nodes"."id" = $2 AND "service_offering_nodes"."archived_at" IS NULL`
	mock.ExpectExec(regexp.QuoteMeta(markAsArchived)).
		WithArgs(testhelper.AnyTime{}, sourceID).
		WillReturnResult(sqlmock.NewResult(100, 1))

	keep := "4"
	sourceRefs := []string{keep}
	err := sonr.DeleteUnwanted(nctx, &son, sourceRefs)
	assert.Nil(t, err, "DeleteUnwanted failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for DeleteUnwanted")
	stats := sonr.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 1)
}

func TestDeleteUnwantedError(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sonr := NewGORMRepository(gdb)
	son := ServiceOfferingNode{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_offering_nodes" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnError(fmt.Errorf("kaboom"))

	keep := "4"
	sourceRefs := []string{keep}
	err := sonr.DeleteUnwanted(nctx, &son, sourceRefs)
	checkErrors(t, err, mock, sonr, "DeleteUnwantedError", "kaboom")
}

func TestDeleteUnwantedErrorInDelete(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	sourceRef := "2"

	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, sourceRef, "Test", time.Now(), time.Now(), time.Now())

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sonr := NewGORMRepository(gdb)
	son := ServiceOfferingNode{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_offering_nodes" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)

	markAsArchived := `UPDATE "service_offering_nodes" SET "archived_at"=$1 WHERE "service_offering_nodes"."id" = $2 AND "service_offering_nodes"."archived_at" IS NULL`
	mock.ExpectExec(regexp.QuoteMeta(markAsArchived)).
		WithArgs(testhelper.AnyTime{}, sourceID).
		WillReturnError(fmt.Errorf("kaboom"))

	keep := "4"
	sourceRefs := []string{keep}
	err := sonr.DeleteUnwanted(nctx, &son, sourceRefs)
	checkErrors(t, err, mock, sonr, "DeleteUnwantedErrorInDelete", "kaboom")
}

func checkErrors(t *testing.T, err error, mock sqlmock.Sqlmock, sonr Repository, where string, errMessage string) {
	assert.NotNil(t, err, where)

	if !strings.Contains(err.Error(), errMessage) {
		t.Fatalf("Error message should have contained %s", errMessage)
	}

	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for %s", where)
	stats := sonr.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 0)
}
