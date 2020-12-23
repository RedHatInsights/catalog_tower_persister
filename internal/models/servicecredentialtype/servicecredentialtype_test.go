package servicecredentialtype

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/RedHatInsights/catalog_tower_persister/internal/logger"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type AnyTime struct{}

// Match satisfies sqlmock.Argument interface
func (a AnyTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)
	return ok
}

func TestCreateMissingParams(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.Nilf(t, err, "error opening stub database %v", err)
	defer db.Close()
	gdb, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	assert.Nilf(t, err, "error opening gorm postgres database %v", err)

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sctr := NewGORMRepository(gdb)
	srcRef := "4"
	tenantID := int64(99)
	sourceID := int64(1)
	attrs := map[string]interface{}{
		"created":   "2020-01-08T10:22:59.423567Z",
		"id":        json.Number(srcRef),
		"modified":  "2020-01-08T10:22:59.423585Z",
		"namespace": "demo",
		"kind":      "test",
		"type":      "credential_type",
	}
	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	err = sctr.CreateOrUpdate(nctx, &sct, attrs)
	assert.NotNil(t, err, "Expecting invalid attributes")

	if !strings.Contains(err.Error(), "Missing Required Attribute name") {
		t.Fatalf("Error message should have contained missing name")
	}
	assert.Equal(t, sctr.NumberOfCreates(), 0)
	assert.Equal(t, sctr.NumberOfUpdates(), 0)
	assert.Equal(t, sctr.NumberOfDeletes(), 0)
}

func TestCreate(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.Nilf(t, err, "error opening stub database %v", err)
	defer db.Close()
	gdb, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	assert.Nilf(t, err, "error opening gorm postgres database %v", err)

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sctr := NewGORMRepository(gdb)
	srcRef := "4"
	tenantID := int64(99)
	sourceID := int64(1)
	newID := int64(87)
	attrs := map[string]interface{}{
		"created":     "2020-01-08T10:22:59.423567Z",
		"id":          json.Number(srcRef),
		"modified":    "2020-01-08T10:22:59.423585Z",
		"namespace":   "demo",
		"kind":        "test",
		"type":        "credential_type",
		"name":        "fred",
		"description": "desc",
	}
	sct := ServiceCredentialType{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_credential_types" WHERE "service_credential_types"."source_ref" = $1 AND "service_credential_types"."source_id" = $2 AND "service_credential_types"."archived_at" IS NULL ORDER BY "service_credential_types"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(gorm.ErrRecordNotFound)
	insertStr := `INSERT INTO "service_credential_types" ("created_at","updated_at","archived_at","source_ref","source_created_at","last_seen_at","name","description","kind","namespace","tenant_id","source_id") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`

	mock.ExpectQuery(regexp.QuoteMeta(insertStr)).
		WithArgs(AnyTime{}, AnyTime{}, nil, srcRef, sqlmock.AnyArg(), sqlmock.AnyArg(), attrs["name"].(string), attrs["description"].(string), attrs["kind"].(string), attrs["namespace"].(string), tenantID, sourceID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(newID))

	err = sctr.CreateOrUpdate(nctx, &sct, attrs)
	assert.Nil(t, err, "CreateOrUpdate failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations")
	assert.Equal(t, sctr.NumberOfCreates(), 1)
	assert.Equal(t, sct.ID, newID)
	assert.Equal(t, sctr.NumberOfUpdates(), 0)
	assert.Equal(t, sctr.NumberOfDeletes(), 0)

}

func TestCreateOrUpdate(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.Nilf(t, err, "error opening stub database %v", err)
	defer db.Close()
	gdb, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	assert.Nilf(t, err, "error opening gorm postgres database %v", err)

	existingID := int64(67)
	srcRef := "4"
	tenantID := int64(99)
	sourceID := int64(1)
	// Here we are creating rows in our mocked database.
	rows := sqlmock.NewRows([]string{"id", "created_at", "updated_at", "archived_at", "source_ref", "source_created_at", "last_seen_at", "name", "description", "kind", "namespace", "tenant_id", "source_id"}).
		AddRow(existingID, time.Now(), time.Now(), nil, srcRef, time.Now(), time.Now(), "test_name", "test_desc", "test_kind", "test_ns", tenantID, sourceID)

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sctr := NewGORMRepository(gdb)
	attrs := map[string]interface{}{
		"created":     "2020-01-08T10:22:59.423567Z",
		"id":          json.Number(srcRef),
		"modified":    "2020-01-08T10:22:59.423585Z",
		"namespace":   "demo",
		"kind":        "test",
		"type":        "credential_type",
		"name":        "fred",
		"description": "desc",
	}
	sct := ServiceCredentialType{SourceID: 1}
	str := `SELECT * FROM "service_credential_types" WHERE "service_credential_types"."source_ref" = $1 AND "service_credential_types"."source_id" = $2 AND "service_credential_types"."archived_at" IS NULL ORDER BY "service_credential_types"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)
	mock.ExpectExec("^UPDATE").WillReturnResult(sqlmock.NewResult(100, 1))
	err = sctr.CreateOrUpdate(nctx, &sct, attrs)

	assert.Nil(t, err, "CreateOrUpdate failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations")
	assert.Equal(t, sctr.NumberOfCreates(), 0)
	assert.Equal(t, sctr.NumberOfUpdates(), 1)
	assert.Equal(t, sctr.NumberOfDeletes(), 0)

}

func XTestDeleteUnwantedMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.Nilf(t, err, "error opening stub database %v", err)
	defer db.Close()
	gdb, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	assert.Nilf(t, err, "error opening gorm postgres database %v", err)
	id := 1
	sourceID := int64(1)
	tenantID := int64(99)
	sourceRef := "2"
	// Here we are creating rows in our mocked database.
	rows := sqlmock.NewRows([]string{"id", "tenant_id", "source_id", "source_ref", "name", "type_name", "description", "source_created_at", "created_at", "updated_at", "service_credential_type_id"}).
		AddRow(id, tenantID, sourceID, sourceRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), nil)

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sctr := NewGORMRepository(gdb)
	sct := ServiceCredentialType{SourceID: sourceID}
	str := `SELECT id, source_ref FROM "service_credential_types" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)
	sourceRefs := []string{sourceRef}
	err = sctr.DeleteUnwanted(nctx, &sct, sourceRefs)

	assert.Nil(t, err, "DeleteUnwanted failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for DeleteUnwanted")
	assert.Equal(t, sctr.NumberOfCreates(), 0)
	assert.Equal(t, sctr.NumberOfUpdates(), 0)
	assert.Equal(t, sctr.NumberOfDeletes(), 0)
}

func XTestDeleteUnwanted(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.Nilf(t, err, "error opening stub database %v", err)
	defer db.Close()
	gdb, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	assert.Nilf(t, err, "error opening gorm postgres database %v", err)
	id := 1
	sourceID := int64(1)
	tenantID := int64(99)
	sourceRef := "2"
	// Here we are creating rows in our mocked database.
	rows := sqlmock.NewRows([]string{"id", "tenant_id", "source_id", "source_ref", "name", "type_name", "description", "source_created_at", "created_at", "updated_at", "service_credential_type_id"}).
		AddRow(id, tenantID, sourceID, sourceRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), nil)

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	sctr := NewGORMRepository(gdb)
	sct := ServiceCredentialType{SourceID: sourceID}
	str := `SELECT id, source_ref FROM "service_credential_types" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)

	markAsArchived := `UPDATE "service_credential_types" SET "archived_at"=$1 WHERE "service_credential_types"."id" = $2 AND "service_credential_types"."archived_at" IS NULL`
	mock.ExpectExec(regexp.QuoteMeta(markAsArchived)).
		WithArgs(AnyTime{}, sourceID).
		WillReturnResult(sqlmock.NewResult(100, 1))

	keep := "4"
	sourceRefs := []string{keep}
	err = sctr.DeleteUnwanted(nctx, &sct, sourceRefs)
	assert.Nil(t, err, "DeleteUnwanted failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for DeleteUnwanted")
	assert.Equal(t, sctr.NumberOfCreates(), 0)
	assert.Equal(t, sctr.NumberOfUpdates(), 0)
	assert.Equal(t, sctr.NumberOfDeletes(), 1)
}
