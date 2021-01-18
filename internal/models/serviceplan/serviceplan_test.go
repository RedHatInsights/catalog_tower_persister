package serviceplan

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/base"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/testhelper"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

var objectType = "survey_spec"

type MockConverter struct {
	data string
	err  error
}

var mockReader = strings.NewReader("hello")

var mockSpec = `{"page": 1, "fruits": ["apple", "peach"]}`

func (mc *MockConverter) Convert(ctx context.Context, logger *logrus.Entry, r io.Reader) ([]byte, error) {
	return []byte(mc.data), mc.err
}

var defaultAttrs = map[string]interface{}{
	"created":     "2020-01-08T10:22:59.423567Z",
	"modified":    "2020-01-08T10:22:59.423585Z",
	"id":          json.Number("4"),
	"name":        "demo",
	"description": "openshift",
	"type":        objectType,
}

var columns = []string{"id", "created_at", "updated_at", "archived_at", "source_ref",
	"source_created_at", "last_seen_at", "name", "description", "extra",
	"create_json_schema", "update_json_schema", "service_offering_id", "tenant_id", "source_id"}

var tenantID = int64(99)
var sourceID = int64(1)

var extra = map[string]interface{}{
	"kind":      "test",
	"type":      objectType,
	"variables": "",
}

func TestCreateMissingParams(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	scr := NewGORMRepository(gdb)
	sp := ServicePlan{SourceID: sourceID, TenantID: tenantID}
	attrs := map[string]interface{}{
		"created":  "2020-01-08T10:22:59.423567Z",
		"modified": "2020-01-08T10:22:59.423585Z",
		"id":       json.Number("4"),
		"name":     "demo",
		"type":     objectType,
	}
	mc := &MockConverter{data: mockSpec, err: nil}
	err := scr.CreateOrUpdate(ctx, testhelper.TestLogger(), &sp, mc, attrs, mockReader)
	checkErrors(t, err, mock, scr, "Expecting invalid attributes", "Missing Required Attribute description")
}

func TestCreateErrorLocatingRecord(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	scr := NewGORMRepository(gdb)
	srcRef := "4"
	sp := ServicePlan{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_plans" WHERE "service_plans"."source_ref" = $1 AND "service_plans"."source_id" = $2 AND "service_plans"."archived_at" IS NULL ORDER BY "service_plans"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(fmt.Errorf("kaboom"))

	mc := &MockConverter{data: mockSpec, err: nil}
	err := scr.CreateOrUpdate(ctx, testhelper.TestLogger(), &sp, mc, defaultAttrs, mockReader)
	checkErrors(t, err, mock, scr, "Expecting create failure", "kaboom")
}

func TestCreateError(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	scr := NewGORMRepository(gdb)
	srcRef := "4"
	str := `SELECT * FROM "service_plans" WHERE "service_plans"."source_ref" = $1 AND "service_plans"."source_id" = $2 AND "service_plans"."archived_at" IS NULL ORDER BY "service_plans"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(gorm.ErrRecordNotFound)
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "service_plans"`)).
		WithArgs(testhelper.AnyTime{}, testhelper.AnyTime{}, nil, srcRef, sqlmock.AnyArg(), sqlmock.AnyArg(), defaultAttrs["name"].(string), defaultAttrs["description"].(string), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), tenantID, sourceID).
		WillReturnError(fmt.Errorf("kaboom"))

	sp := ServicePlan{SourceID: sourceID, TenantID: tenantID}
	mc := &MockConverter{data: mockSpec, err: nil}
	err := scr.CreateOrUpdate(ctx, testhelper.TestLogger(), &sp, mc, defaultAttrs, mockReader)
	checkErrors(t, err, mock, scr, "Expecting create failure", "kaboom")
}

func TestCreateConverterError(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	scr := NewGORMRepository(gdb)
	sp := ServicePlan{SourceID: sourceID, TenantID: tenantID}
	mc := &MockConverter{data: mockSpec, err: fmt.Errorf("kaboom")}
	err := scr.CreateOrUpdate(ctx, testhelper.TestLogger(), &sp, mc, defaultAttrs, mockReader)
	checkErrors(t, err, mock, scr, "Expecting create failure", "kaboom")
}

func TestCreate(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	scr := NewGORMRepository(gdb)
	srcRef := "4"
	newID := int64(78)
	sp := ServicePlan{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_plans" WHERE "service_plans"."source_ref" = $1 AND "service_plans"."source_id" = $2 AND "service_plans"."archived_at" IS NULL ORDER BY "service_plans"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(gorm.ErrRecordNotFound)
	insertStr := `INSERT INTO "service_plans" ("created_at","updated_at","archived_at","source_ref","source_created_at","last_seen_at","name","description","extra","create_json_schema","update_json_schema","tenant_id","source_id") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`

	mock.ExpectQuery(regexp.QuoteMeta(insertStr)).
		WithArgs(testhelper.AnyTime{}, testhelper.AnyTime{}, nil, srcRef, sqlmock.AnyArg(), sqlmock.AnyArg(), defaultAttrs["name"].(string), defaultAttrs["description"].(string), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), tenantID, sourceID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "service_offering_id"}).AddRow(newID, 78))
	mc := &MockConverter{data: mockSpec, err: nil}
	err := scr.CreateOrUpdate(ctx, testhelper.TestLogger(), &sp, mc, defaultAttrs, mockReader)
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

	encodedExtra, err := json.Marshal(extra)
	if err != nil {
		t.Fatalf("Error encoding extra data")
	}
	srcRef := "4"
	id := int64(1)
	rows := sqlmock.NewRows(columns).
		AddRow(id, time.Now(), time.Now(), nil, srcRef, time.Now(), time.Now(), "test_name", "test_desc", encodedExtra, encodedExtra, encodedExtra, nil, tenantID, sourceID)
	ctx := context.TODO()
	scr := NewGORMRepository(gdb)
	sp := ServicePlan{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_plans" WHERE "service_plans"."source_ref" = $1 AND "service_plans"."source_id" = $2 AND "service_plans"."archived_at" IS NULL ORDER BY "service_plans"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)
	mock.ExpectExec("^UPDATE").WillReturnError(fmt.Errorf("kaboom"))

	mc := &MockConverter{data: mockSpec, err: nil}
	err = scr.CreateOrUpdate(ctx, testhelper.TestLogger(), &sp, mc, defaultAttrs, mockReader)

	checkErrors(t, err, mock, scr, "Expecting CreateUpdate Error", "kaboom")
}

func TestCreateOrUpdate(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "4"
	encodedExtra, err := json.Marshal(extra)
	if err != nil {
		t.Fatalf("Error encoding extra data")
	}
	rows := sqlmock.NewRows(columns).
		AddRow(id, time.Now(), time.Now(), nil, srcRef, time.Now(), time.Now(), "test_name", "test_desc", encodedExtra, encodedExtra, encodedExtra, nil, tenantID, sourceID)
	ctx := context.TODO()
	scr := NewGORMRepository(gdb)
	sp := ServicePlan{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_plans" WHERE "service_plans"."source_ref" = $1 AND "service_plans"."source_id" = $2 AND "service_plans"."archived_at" IS NULL ORDER BY "service_plans"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)
	mock.ExpectExec("^UPDATE").WillReturnResult(sqlmock.NewResult(100, 1))
	mc := &MockConverter{data: mockSpec, err: nil}
	err = scr.CreateOrUpdate(ctx, testhelper.TestLogger(), &sp, mc, defaultAttrs, mockReader)

	assert.Nil(t, err, "CreateOrUpdate failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations")
	stats := scr.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 1)
	assert.Equal(t, stats["deletes"], 0)

}

func TestDelete(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	srcRef := "2"
	ctx := context.TODO()
	scr := NewGORMRepository(gdb)
	sp := ServicePlan{SourceID: sourceID, TenantID: tenantID, Tower: base.Tower{SourceRef: srcRef}}
	markAsArchived := `UPDATE "service_plans" SET "archived_at"=$1 WHERE (source_ref = $2 AND source_id = $3) AND "service_plans"."archived_at" IS NULL`
	mock.ExpectExec(regexp.QuoteMeta(markAsArchived)).
		WithArgs(testhelper.AnyTime{}, srcRef, sourceID).
		WillReturnResult(sqlmock.NewResult(100, 1))
	err := scr.Delete(ctx, testhelper.TestLogger(), &sp)
	assert.Nil(t, err, "Delete failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for Delete")
	stats := scr.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 1)
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
