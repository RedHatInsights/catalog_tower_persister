package servicecredential

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
	scr := NewGORMRepository(gdb)
	srcRef := "4"
	tenantID := int64(99)
	sourceID := int64(1)
	attrs := map[string]interface{}{
		"created":         "2020-01-08T10:22:59.423567Z",
		"credential_type": json.Number("14"),
		"id":              json.Number(srcRef),
		"modified":        "2020-01-08T10:22:59.423585Z",
		"name":            "demo",
		"type":            "credential",
	}
	sc := ServiceCredential{SourceID: sourceID, TenantID: tenantID}
	err = scr.CreateOrUpdate(nctx, &sc, attrs)
	assert.NotNil(t, err, "Expecting invalid attributes")

	if !strings.Contains(err.Error(), "Missing Required Attribute description") {
		t.Fatalf("Error message should have contained missing description")
	}
	assert.Equal(t, scr.NumberOfCreates(), 0)
	assert.Equal(t, scr.NumberOfUpdates(), 0)
	assert.Equal(t, scr.NumberOfDeletes(), 0)
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
	scr := NewGORMRepository(gdb)
	srcRef := "4"
	tenantID := int64(99)
	sourceID := int64(1)
	newID := int64(78)
	attrs := map[string]interface{}{
		"created":         "2020-01-08T10:22:59.423567Z",
		"credential_type": json.Number("14"),
		"description":     "desc",
		"id":              json.Number(srcRef),
		"modified":        "2020-01-08T10:22:59.423585Z",
		"name":            "demo",
		"type":            "credential",
	}
	sc := ServiceCredential{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_credentials" WHERE "service_credentials"."source_ref" = $1 AND "service_credentials"."source_id" = $2 AND "service_credentials"."archived_at" IS NULL ORDER BY "service_credentials"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(gorm.ErrRecordNotFound)
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "service_credentials"`)).
		WithArgs(AnyTime{}, AnyTime{}, nil, srcRef, sqlmock.AnyArg(), sqlmock.AnyArg(), "demo", sqlmock.AnyArg(), "desc", tenantID, 1).
		WillReturnRows(sqlmock.NewRows([]string{"service_credential_type_id", "id"}).AddRow(5, newID))
	err = scr.CreateOrUpdate(nctx, &sc, attrs)
	assert.Nil(t, err, "CreateOrUpdate failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations")
	assert.Equal(t, scr.NumberOfCreates(), 1)
	assert.Equal(t, scr.NumberOfUpdates(), 0)
	assert.Equal(t, scr.NumberOfDeletes(), 0)
	assert.Equal(t, sc.ID, newID)

}

func TestCreateOrUpdate(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.Nilf(t, err, "error opening stub database %v", err)
	defer db.Close()
	gdb, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	assert.Nilf(t, err, "error opening gorm postgres database %v", err)

	// Here we are creating rows in our mocked database.
	rows := sqlmock.NewRows([]string{"id", "tenant_id", "source_id", "source_ref", "name", "type_name", "description", "source_created_at", "created_at", "updated_at", "service_credential_type_id"}).
		AddRow(1, 1, 1, "2", "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), nil)
	srcRef := "4"
	sourceID := int64(1)
	tenantID := int64(99)
	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	attrs := map[string]interface{}{
		"created":         "2020-01-08T10:22:59.423567Z",
		"credential_type": json.Number("14"),
		"description":     "",
		"id":              json.Number(srcRef),
		"modified":        "2020-01-08T10:22:59.423585Z",
		"name":            "demo",
		"type":            "credential",
	}
	sc := ServiceCredential{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_credentials" WHERE "service_credentials"."source_ref" = $1 AND "service_credentials"."source_id" = $2 AND "service_credentials"."archived_at" IS NULL ORDER BY "service_credentials"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)
	mock.ExpectExec("^UPDATE").WillReturnResult(sqlmock.NewResult(100, 1))
	err = scr.CreateOrUpdate(nctx, &sc, attrs)

	assert.Nil(t, err, "CreateOrUpdate failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations")
	assert.Equal(t, scr.NumberOfCreates(), 0)
	assert.Equal(t, scr.NumberOfUpdates(), 1)
	assert.Equal(t, scr.NumberOfDeletes(), 0)

}

func TestDeleteUnwantedMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.Nilf(t, err, "error opening stub database %v", err)
	defer db.Close()
	gdb, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	assert.Nilf(t, err, "error opening gorm postgres database %v", err)
	id := 1
	sourceID := 1
	tenantID := 1
	sourceRef := "2"
	// Here we are creating rows in our mocked database.
	rows := sqlmock.NewRows([]string{"id", "tenant_id", "source_id", "source_ref", "name", "type_name", "description", "source_created_at", "created_at", "updated_at", "service_credential_type_id"}).
		AddRow(id, tenantID, sourceID, sourceRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), nil)

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	sc := ServiceCredential{SourceID: 1}
	str := `SELECT id, source_ref FROM "service_credentials" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)
	sourceRefs := []string{sourceRef}
	err = scr.DeleteUnwanted(nctx, &sc, sourceRefs)

	assert.Nil(t, err, "DeleteUnwanted failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for DeleteUnwanted")
	assert.Equal(t, scr.NumberOfCreates(), 0)
	assert.Equal(t, scr.NumberOfUpdates(), 0)
	assert.Equal(t, scr.NumberOfDeletes(), 0)
}

func TestDeleteUnwanted(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.Nilf(t, err, "error opening stub database %v", err)
	defer db.Close()
	gdb, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{})
	assert.Nilf(t, err, "error opening gorm postgres database %v", err)
	id := 1
	sourceID := 1
	tenantID := 1
	sourceRef := "2"
	// Here we are creating rows in our mocked database.
	rows := sqlmock.NewRows([]string{"id", "tenant_id", "source_id", "source_ref", "name", "type_name", "description", "source_created_at", "created_at", "updated_at", "service_credential_type_id"}).
		AddRow(id, tenantID, sourceID, sourceRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), nil)

	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	scr := NewGORMRepository(gdb)
	sc := ServiceCredential{SourceID: 1}
	str := `SELECT id, source_ref FROM "service_credentials" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)

	markAsArchived := `UPDATE "service_credentials" SET "archived_at"=$1 WHERE "service_credentials"."id" = $2 AND "service_credentials"."archived_at" IS NULL`
	mock.ExpectExec(regexp.QuoteMeta(markAsArchived)).
		WithArgs(AnyTime{}, sourceID).
		WillReturnResult(sqlmock.NewResult(100, 1))

	keep := "4"
	sourceRefs := []string{keep}
	err = scr.DeleteUnwanted(nctx, &sc, sourceRefs)
	assert.Nil(t, err, "DeleteUnwanted failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for DeleteUnwanted")
	assert.Equal(t, scr.NumberOfCreates(), 0)
	assert.Equal(t, scr.NumberOfUpdates(), 0)
	assert.Equal(t, scr.NumberOfDeletes(), 1)
}
