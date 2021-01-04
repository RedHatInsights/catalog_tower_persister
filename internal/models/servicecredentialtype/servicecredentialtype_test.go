package servicecredentialtype

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

var objectType = "credential_type"

var defaultAttrs = map[string]interface{}{
	"created":     "2020-01-08T10:22:59.423567Z",
	"modified":    "2020-01-08T10:22:59.423585Z",
	"id":          json.Number("4"),
	"name":        "demo",
	"description": "openshift",
	"type":        objectType,
	"namespace":   "demo",
	"kind":        "test",
}

var columns = []string{"id", "created_at", "updated_at", "archived_at", "source_ref",
	"source_created_at", "last_seen_at", "name", "description", "kind",
	"namespace", "tenant_id", "source_id"}
var tenantID = int64(99)
var sourceID = int64(1)

func TestBadDateTime(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	srcRef := "4"
	attrs := map[string]interface{}{
		"created":     "gobbledegook",
		"modified":    "2020-01-08T10:22:59.423585Z",
		"id":          json.Number(srcRef),
		"name":        "demo",
		"description": "openshift",
		"type":        objectType,
		"namespace":   "demo",
		"kind":        "test",
	}
	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	err := scr.CreateOrUpdate(nctx, &sct, attrs)
	checkErrors(t, err, mock, scr, "Parsing time error", "parsing time")
}

func TestCreateMissingParams(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	attrs := map[string]interface{}{
		"created":   "2020-01-08T10:22:59.423567Z",
		"modified":  "2020-01-08T10:22:59.423585Z",
		"id":        json.Number("4"),
		"name":      "demo",
		"type":      objectType,
		"namespace": "demo",
		"kind":      "test",
	}
	err := scr.CreateOrUpdate(nctx, &sct, attrs)
	checkErrors(t, err, mock, scr, "Expecting invalid attributes", "Missing Required Attribute description")
}

func TestCreateErrorLocatingRecord(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	srcRef := "4"
	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_credential_types" WHERE "service_credential_types"."source_ref" = $1 AND "service_credential_types"."source_id" = $2 AND "service_credential_types"."archived_at" IS NULL ORDER BY "service_credential_types"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(fmt.Errorf("kaboom"))

	err := scr.CreateOrUpdate(nctx, &sct, defaultAttrs)
	checkErrors(t, err, mock, scr, "Expecting create failure", "kaboom")
}

func TestCreateError(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	srcRef := "4"
	str := `SELECT * FROM "service_credential_types" WHERE "service_credential_types"."source_ref" = $1 AND "service_credential_types"."source_id" = $2 AND "service_credential_types"."archived_at" IS NULL ORDER BY "service_credential_types"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(gorm.ErrRecordNotFound)
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "service_credential_types"`)).
		WithArgs(testhelper.AnyTime{}, testhelper.AnyTime{}, nil, srcRef, sqlmock.AnyArg(), sqlmock.AnyArg(), defaultAttrs["name"], defaultAttrs["description"], defaultAttrs["kind"], defaultAttrs["namespace"], tenantID, sourceID).
		WillReturnError(fmt.Errorf("kaboom"))

	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	err := scr.CreateOrUpdate(nctx, &sct, defaultAttrs)
	checkErrors(t, err, mock, scr, "Expecting create failure", "kaboom")
}

func TestCreate(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	srcRef := "4"
	newID := int64(78)
	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_credential_types" WHERE "service_credential_types"."source_ref" = $1 AND "service_credential_types"."source_id" = $2 AND "service_credential_types"."archived_at" IS NULL ORDER BY "service_credential_types"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(gorm.ErrRecordNotFound)
	insertStr := `INSERT INTO "service_credential_types" ("created_at","updated_at","archived_at","source_ref","source_created_at","last_seen_at","name","description","kind","namespace","tenant_id","source_id") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`

	mock.ExpectQuery(regexp.QuoteMeta(insertStr)).
		WithArgs(testhelper.AnyTime{}, testhelper.AnyTime{}, nil, srcRef, sqlmock.AnyArg(), sqlmock.AnyArg(), defaultAttrs["name"].(string), defaultAttrs["description"].(string), defaultAttrs["kind"].(string), defaultAttrs["namespace"].(string), tenantID, sourceID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(newID))
	err := scr.CreateOrUpdate(nctx, &sct, defaultAttrs)
	assert.Nil(t, err, "CreateOrUpdate failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations")
	stats := scr.Stats()
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
	id := int64(1)
	rows := sqlmock.NewRows(columns).
		AddRow(id, time.Now(), time.Now(), nil, srcRef, time.Now(), time.Now(), "test_name", "test_desc", "test_kind", "test_ns", tenantID, sourceID)
	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_credential_types" WHERE "service_credential_types"."source_ref" = $1 AND "service_credential_types"."source_id" = $2 AND "service_credential_types"."archived_at" IS NULL ORDER BY "service_credential_types"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)
	mock.ExpectExec("^UPDATE").WillReturnError(fmt.Errorf("kaboom"))

	err := scr.CreateOrUpdate(nctx, &sct, defaultAttrs)

	checkErrors(t, err, mock, scr, "Expecting CreateUpdate Error", "kaboom")
}

func TestCreateOrUpdate(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "4"
	rows := sqlmock.NewRows(columns).
		AddRow(id, time.Now(), time.Now(), nil, srcRef, time.Now(), time.Now(), "test_name", "test_desc", "test_kind", "test_ns", tenantID, sourceID)
	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_credential_types" WHERE "service_credential_types"."source_ref" = $1 AND "service_credential_types"."source_id" = $2 AND "service_credential_types"."archived_at" IS NULL ORDER BY "service_credential_types"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)
	mock.ExpectExec("^UPDATE").WillReturnResult(sqlmock.NewResult(100, 1))
	err := scr.CreateOrUpdate(nctx, &sct, defaultAttrs)

	assert.Nil(t, err, "CreateOrUpdate failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations")
	stats := scr.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 1)
	assert.Equal(t, stats["deletes"], 0)

}

func TestDeleteUnwantedMissing(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "2"

	rows := sqlmock.NewRows(columns).
		AddRow(id, time.Now(), time.Now(), nil, srcRef, time.Now(), time.Now(), "test_name", "test_desc", "test_kind", "test_ns", tenantID, sourceID)

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_credential_types" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)
	sourceRefs := []string{srcRef}
	err := scr.DeleteUnwanted(nctx, &sct, sourceRefs)

	assert.Nil(t, err, "DeleteUnwantedMissing failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for DeleteUnwanted")
	stats := scr.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 0)
}

func TestDeleteUnwanted(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "2"

	rows := sqlmock.NewRows(columns).
		AddRow(id, time.Now(), time.Now(), nil, srcRef, time.Now(), time.Now(), "test_name", "test_desc", "test_kind", "test_ns", tenantID, sourceID)

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_credential_types" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)

	markAsArchived := `UPDATE "service_credential_types" SET "archived_at"=$1 WHERE "service_credential_types"."id" = $2 AND "service_credential_types"."archived_at" IS NULL`
	mock.ExpectExec(regexp.QuoteMeta(markAsArchived)).
		WithArgs(testhelper.AnyTime{}, sourceID).
		WillReturnResult(sqlmock.NewResult(100, 1))

	keep := "4"
	sourceRefs := []string{keep}
	err := scr.DeleteUnwanted(nctx, &sct, sourceRefs)
	assert.Nil(t, err, "DeleteUnwanted failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for DeleteUnwanted")
	stats := scr.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 1)
}

func TestDeleteUnwantedError(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_credential_types" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnError(fmt.Errorf("kaboom"))

	keep := "4"
	sourceRefs := []string{keep}
	err := scr.DeleteUnwanted(nctx, &sct, sourceRefs)
	checkErrors(t, err, mock, scr, "DeleteUnwantedError", "kaboom")
}

func TestDeleteUnwantedErrorInDelete(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "2"

	rows := sqlmock.NewRows(columns).
		AddRow(id, time.Now(), time.Now(), nil, srcRef, time.Now(), time.Now(), "test_name", "test_desc", "test_kind", "test_ns", tenantID, sourceID)

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_credential_types" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)

	markAsArchived := `UPDATE "service_credential_types" SET "archived_at"=$1 WHERE "service_credential_types"."id" = $2 AND "service_credential_types"."archived_at" IS NULL`
	mock.ExpectExec(regexp.QuoteMeta(markAsArchived)).
		WithArgs(testhelper.AnyTime{}, sourceID).
		WillReturnError(fmt.Errorf("kaboom"))

	keep := "4"
	sourceRefs := []string{keep}
	err := scr.DeleteUnwanted(nctx, &sct, sourceRefs)
	checkErrors(t, err, mock, scr, "DeleteUnwantedErrorInDelete", "kaboom")
}

func checkErrors(t *testing.T, err error, mock sqlmock.Sqlmock, scr Repository, where string, errMessage string) {
	assert.NotNil(t, err, where)

	if !strings.Contains(err.Error(), errMessage) {
		t.Fatalf("Error message should have contained %s", errMessage)
	}

	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for %s", where)
	stats := scr.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 0)
}
