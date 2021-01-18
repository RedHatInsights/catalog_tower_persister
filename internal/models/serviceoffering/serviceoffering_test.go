package serviceoffering

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
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceplan"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/testhelper"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

type MockConverter struct {
	data string
	err  error
}

var mockReader = strings.NewReader("hello")

var mockSpec = `{"page": 1, "fruits": ["apple", "peach"]}`

func (mc *MockConverter) Convert(ctx context.Context, logger *logrus.Entry, r io.Reader) ([]byte, error) {
	return []byte(mc.data), mc.err
}

type MockServicePlanRepository struct {
	deletesCalled int
	err           error
}

func (mspr *MockServicePlanRepository) Delete(ctx context.Context, logger *logrus.Entry, sp *serviceplan.ServicePlan) error {
	if mspr.err == nil {
		mspr.deletesCalled++
	}
	return mspr.err
}

func (mspr *MockServicePlanRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sp *serviceplan.ServicePlan, converter serviceplan.DDFConverter, attrs map[string]interface{}, r io.Reader) error {
	return nil
}

func (mspr *MockServicePlanRepository) Stats() map[string]int {
	return map[string]int{"add": 0}
}

var objectType = "job_template"

func makeDefaultAttrs(id string, survey bool) map[string]interface{} {
	return map[string]interface{}{"created": "2020-01-08T10:22:59.423567Z",
		"modified":                "2020-01-08T10:22:59.423585Z",
		"id":                      json.Number(id),
		"inventory":               "/api/v2/inventories/1/",
		"name":                    "demo",
		"description":             "openshift",
		"type":                    objectType,
		"ask_inventory_on_launch": true,
		"ask_variables_on_launch": false,
		"survey_enabled":          survey,
		"ask_tags_on_launch":      true,
	}
}

var columns = []string{"id", "tenant_id", "source_id", "source_ref", "name", "type_name",
	"description", "source_created_at", "created_at", "updated_at",
	"extra"}
var tenantID = int64(99)
var sourceID = int64(1)

func TestBadDateTime(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	srcRef := "4"
	attrs := map[string]interface{}{
		"created":                 "gobbledegook",
		"modified":                "2020-01-08T10:22:59.423585Z",
		"id":                      json.Number(srcRef),
		"name":                    "demo",
		"description":             "openshift",
		"type":                    objectType,
		"ask_inventory_on_launch": true,
		"ask_variables_on_launch": false,
		"survey_enabled":          true,
	}
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	err := sor.CreateOrUpdate(ctx, testhelper.TestLogger(), &so, attrs, &MockServicePlanRepository{})
	checkErrors(t, err, mock, sor, "Parsing time error", "parsing time")
}

func TestCreateMissingParams(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	attrs := map[string]interface{}{
		"created":                 "2020-01-08T10:22:59.423567Z",
		"modified":                "2020-01-08T10:22:59.423585Z",
		"id":                      json.Number("4"),
		"name":                    "demo",
		"type":                    objectType,
		"ask_inventory_on_launch": true,
		"ask_variables_on_launch": false,
		"survey_enabled":          true,
	}
	err := sor.CreateOrUpdate(ctx, testhelper.TestLogger(), &so, attrs, &MockServicePlanRepository{})
	checkErrors(t, err, mock, sor, "Expecting invalid attributes", "Missing Required Attribute description")
}

func TestCreateErrorLocatingRecord(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	srcRef := "4"
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_offerings" WHERE "service_offerings"."source_ref" = $1 AND "service_offerings"."source_id" = $2 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(fmt.Errorf("kaboom"))
	err := sor.CreateOrUpdate(ctx, testhelper.TestLogger(), &so, makeDefaultAttrs(srcRef, true), &MockServicePlanRepository{})
	checkErrors(t, err, mock, sor, "Expecting create failure", "kaboom")
}

func TestCreateError(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	srcRef := "4"
	defaultAttrs := makeDefaultAttrs(srcRef, true)
	str := `SELECT * FROM "service_offerings" WHERE "service_offerings"."source_ref" = $1 AND "service_offerings"."source_id" = $2 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(gorm.ErrRecordNotFound)
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "service_offerings"`)).
		WithArgs(testhelper.AnyTime{}, testhelper.AnyTime{}, nil, srcRef, sqlmock.AnyArg(), sqlmock.AnyArg(), defaultAttrs["name"], defaultAttrs["description"], sqlmock.AnyArg(), tenantID, sourceID).
		WillReturnError(fmt.Errorf("kaboom"))
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	err := sor.CreateOrUpdate(ctx, testhelper.TestLogger(), &so, makeDefaultAttrs(srcRef, true), &MockServicePlanRepository{})
	checkErrors(t, err, mock, sor, "Expecting create failure", "kaboom")
}

func TestCreate(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	srcRef := "4"
	newID := int64(78)
	defaultAttrs := makeDefaultAttrs(srcRef, true)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_offerings" WHERE "service_offerings"."source_ref" = $1 AND "service_offerings"."source_id" = $2 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`

	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnError(gorm.ErrRecordNotFound)
	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO "service_offerings"`)).
		WithArgs(testhelper.AnyTime{}, testhelper.AnyTime{}, nil, srcRef, sqlmock.AnyArg(), sqlmock.AnyArg(), defaultAttrs["name"], defaultAttrs["description"], sqlmock.AnyArg(), tenantID, sourceID).
		WillReturnRows(sqlmock.NewRows([]string{"service_offering_icon_id", "id", "service_inventory_id"}).AddRow(5, newID, 6))
	err := sor.CreateOrUpdate(ctx, testhelper.TestLogger(), &so, defaultAttrs, &MockServicePlanRepository{})
	assert.Nil(t, err, "CreateOrUpdate failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations")
	stats := sor.Stats()
	assert.Equal(t, stats["adds"], 1)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 0)
	// TODO: Since the order of the returning is not guranteed in GORM we can't check the ID
	// Its most probably happening because they are using maps to store fields and the order of the
	// keys when retrieving a map is not guaranteed
	// assert.Equal(t, sc.ID, newID)

}

func TestCreateOrUpdateExtraError(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	encodedExtra := []byte("gobbledegook")
	srcRef := "4"
	id := int64(1)
	defaultAttrs := makeDefaultAttrs(srcRef, true)
	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)
	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_offerings" WHERE "service_offerings"."source_ref" = $1 AND "service_offerings"."source_id" = $2 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)
	err := sor.CreateOrUpdate(ctx, testhelper.TestLogger(), &so, defaultAttrs, &MockServicePlanRepository{})
	errMsg := `invalid character 'g' looking for beginning of value`
	checkErrors(t, err, mock, sor, "TestCreateOrUpdateExtraError", errMsg)
}

func TestCreateOrUpdateError(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	extra := map[string]interface{}{
		"ask_inventory_on_launch": true,
		"ask_variables_on_launch": false,
		"survey_enabled":          true,
		"type":                    "job_template"}
	encodedExtra, err := json.Marshal(extra)
	if err != nil {
		t.Fatalf("Error encoding extra data")
	}
	srcRef := "4"
	id := int64(1)
	defaultAttrs := makeDefaultAttrs(srcRef, true)
	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)
	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_offerings" WHERE "service_offerings"."source_ref" = $1 AND "service_offerings"."source_id" = $2 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)
	mock.ExpectExec("^UPDATE").WillReturnError(fmt.Errorf("kaboom"))

	err = sor.CreateOrUpdate(ctx, testhelper.TestLogger(), &so, defaultAttrs, &MockServicePlanRepository{})

	checkErrors(t, err, mock, sor, "Expecting CreateUpdate Error", "kaboom")
}

func TestCreateOrUpdate(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "4"

	defaultAttrs := makeDefaultAttrs(srcRef, false)
	extra := map[string]interface{}{
		"ask_inventory_on_launch": true,
		"ask_variables_on_launch": false,
		"survey_enabled":          true,
		"type":                    "job_template"}

	encodedExtra, err := json.Marshal(extra)
	if err != nil {
		t.Fatalf("Error encoding extra data")
	}
	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)
	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_offerings" WHERE "service_offerings"."source_ref" = $1 AND "service_offerings"."source_id" = $2 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)
	mock.ExpectExec("^UPDATE").WillReturnResult(sqlmock.NewResult(100, 1))
	err = sor.CreateOrUpdate(ctx, testhelper.TestLogger(), &so, defaultAttrs, &MockServicePlanRepository{})

	assert.Nil(t, err, "CreateOrUpdate failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations")
	stats := sor.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 1)
	assert.Equal(t, stats["deletes"], 0)

}

func TestCreateOrUpdateSurveyDisabled(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "4"

	defaultAttrs := makeDefaultAttrs(srcRef, false)
	extra := map[string]interface{}{
		"ask_inventory_on_launch": true,
		"ask_variables_on_launch": false,
		"survey_enabled":          true,
		"type":                    "job_template"}

	encodedExtra, err := json.Marshal(extra)
	if err != nil {
		t.Fatalf("Error encoding extra data")
	}
	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)
	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_offerings" WHERE "service_offerings"."source_ref" = $1 AND "service_offerings"."source_id" = $2 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)
	mock.ExpectExec("^UPDATE").WillReturnResult(sqlmock.NewResult(100, 1))
	mspr := &MockServicePlanRepository{}
	err = sor.CreateOrUpdate(ctx, testhelper.TestLogger(), &so, defaultAttrs, mspr)

	assert.Nil(t, err, "CreateOrUpdate failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations")
	stats := sor.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 1)
	assert.Equal(t, stats["deletes"], 0)
	assert.Equal(t, mspr.deletesCalled, 1)

}

func TestCreateOrUpdateSurveyDisabledError(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "4"

	defaultAttrs := makeDefaultAttrs(srcRef, false)
	extra := map[string]interface{}{
		"ask_inventory_on_launch": true,
		"ask_variables_on_launch": false,
		"survey_enabled":          true,
		"type":                    "job_template"}

	encodedExtra, err := json.Marshal(extra)
	if err != nil {
		t.Fatalf("Error encoding extra data")
	}
	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)
	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT * FROM "service_offerings" WHERE "service_offerings"."source_ref" = $1 AND "service_offerings"."source_id" = $2 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows)

	mspr := &MockServicePlanRepository{err: fmt.Errorf("kaboom")}
	err = sor.CreateOrUpdate(ctx, testhelper.TestLogger(), &so, defaultAttrs, mspr)

	checkErrors(t, err, mock, sor, "TestCreateOrUpdateSurveyDisabledError", "kaboom")
	assert.NotNil(t, err, "TestCreateOrUpdateSurveyDisabledError failed")
	assert.Equal(t, mspr.deletesCalled, 0)
}

func TestDeleteUnwantedMissing(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "2"

	extra := map[string]interface{}{
		"ask_inventory_on_launch": true,
		"ask_variables_on_launch": false,
		"survey_enabled":          true,
		"type":                    "job_template"}
	encodedExtra, err := json.Marshal(extra)
	if err != nil {
		t.Fatalf("Error encoding extra data")
	}
	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)

	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_offerings" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)
	sourceRefs := []string{srcRef}
	err = sor.DeleteUnwanted(ctx, testhelper.TestLogger(), &so, sourceRefs, &MockServicePlanRepository{})

	assert.Nil(t, err, "DeleteUnwantedMissing failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for DeleteUnwanted")
	stats := sor.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 0)
}

func TestDeleteUnwantedMissingServicePlan(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "2"

	extra := map[string]interface{}{
		"ask_inventory_on_launch": true,
		"ask_variables_on_launch": false,
		"survey_enabled":          true,
		"type":                    "job_template"}
	encodedExtra, err := json.Marshal(extra)
	if err != nil {
		t.Fatalf("Error encoding extra data")
	}
	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)

	// Can't use the same rows in multiple calls
	rows2 := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)
	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_offerings" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)

	fetchStr := `SELECT * FROM "service_offerings" WHERE "service_offerings"."source_ref" = $1 AND "service_offerings"."source_id" = $2 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(fetchStr)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows2)

	markAsArchived := `UPDATE "service_offerings" SET "archived_at"=$1 WHERE "service_offerings"."id" = $2 AND "service_offerings"."archived_at" IS NULL`
	mock.ExpectExec(regexp.QuoteMeta(markAsArchived)).
		WithArgs(testhelper.AnyTime{}, id).
		WillReturnResult(sqlmock.NewResult(100, 1))

	keep := "4"
	sourceRefs := []string{keep}
	mspr := &MockServicePlanRepository{err: fmt.Errorf("kaboom")}
	err = sor.DeleteUnwanted(ctx, testhelper.TestLogger(), &so, sourceRefs, mspr)
	assert.NotNil(t, err, "TestDeleteUnwantedMissingServicePlan failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for DeleteUnwanted")
	stats := sor.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 1)
}

func TestDeleteUnwanted(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "2"

	extra := map[string]interface{}{
		"ask_inventory_on_launch": true,
		"ask_variables_on_launch": false,
		"survey_enabled":          true,
		"type":                    "job_template"}
	encodedExtra, err := json.Marshal(extra)
	if err != nil {
		t.Fatalf("Error encoding extra data")
	}
	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)

	// Can't use the same rows in multiple calls
	rows2 := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)
	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_offerings" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)

	fetchStr := `SELECT * FROM "service_offerings" WHERE "service_offerings"."source_ref" = $1 AND "service_offerings"."source_id" = $2 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(fetchStr)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows2)

	markAsArchived := `UPDATE "service_offerings" SET "archived_at"=$1 WHERE "service_offerings"."id" = $2 AND "service_offerings"."archived_at" IS NULL`
	mock.ExpectExec(regexp.QuoteMeta(markAsArchived)).
		WithArgs(testhelper.AnyTime{}, id).
		WillReturnResult(sqlmock.NewResult(100, 1))

	keep := "4"
	sourceRefs := []string{keep}
	err = sor.DeleteUnwanted(ctx, testhelper.TestLogger(), &so, sourceRefs, &MockServicePlanRepository{})
	assert.Nil(t, err, "DeleteUnwanted failed")
	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for DeleteUnwanted")
	stats := sor.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 1)
}

func TestDeleteUnwantedError(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_offerings" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnError(fmt.Errorf("kaboom"))

	keep := "4"
	sourceRefs := []string{keep}
	err := sor.DeleteUnwanted(ctx, testhelper.TestLogger(), &so, sourceRefs, &MockServicePlanRepository{})
	checkErrors(t, err, mock, sor, "DeleteUnwantedError", "kaboom")
}

func TestDeleteUnwantedErrorInDelete(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "2"

	extra := map[string]interface{}{
		"ask_inventory_on_launch": true,
		"ask_variables_on_launch": false,
		"survey_enabled":          true,
		"type":                    "job_template"}
	encodedExtra, err := json.Marshal(extra)
	if err != nil {
		t.Fatalf("Error encoding extra data")
	}
	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)

	// Can't use the same rows in multiple calls
	rows2 := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)

	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_offerings" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)

	fetchStr := `SELECT * FROM "service_offerings" WHERE "service_offerings"."source_ref" = $1 AND "service_offerings"."source_id" = $2 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(fetchStr)).
		WithArgs(srcRef, sourceID).
		WillReturnRows(rows2)

	markAsArchived := `UPDATE "service_offerings" SET "archived_at"=$1 WHERE "service_offerings"."id" = $2 AND "service_offerings"."archived_at" IS NULL`
	mock.ExpectExec(regexp.QuoteMeta(markAsArchived)).
		WithArgs(testhelper.AnyTime{}, id).
		WillReturnError(fmt.Errorf("kaboom"))

	keep := "4"
	sourceRefs := []string{keep}
	err = sor.DeleteUnwanted(ctx, testhelper.TestLogger(), &so, sourceRefs, &MockServicePlanRepository{})
	checkErrors(t, err, mock, sor, "DeleteUnwantedErrorInDelete", "kaboom")
}

func TestDeleteUnwantedMissingInstance(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	id := int64(1)
	srcRef := "2"

	extra := map[string]interface{}{
		"ask_inventory_on_launch": true,
		"ask_variables_on_launch": false,
		"survey_enabled":          true,
		"type":                    "job_template"}
	encodedExtra, err := json.Marshal(extra)
	if err != nil {
		t.Fatalf("Error encoding extra data")
	}
	rows := sqlmock.NewRows(columns).
		AddRow(id, tenantID, sourceID, srcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), encodedExtra)

	ctx := context.TODO()
	sor := NewGORMRepository(gdb)
	so := ServiceOffering{SourceID: sourceID, TenantID: tenantID}
	str := `SELECT id, source_ref FROM "service_offerings" WHERE source_id = $1 AND archived_at IS NULL`
	mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sourceID).
		WillReturnRows(rows)

	fetchStr := `SELECT * FROM "service_offerings" WHERE "service_offerings"."source_ref" = $1 AND "service_offerings"."source_id" = $2 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`
	mock.ExpectQuery(regexp.QuoteMeta(fetchStr)).
		WithArgs(srcRef, sourceID).
		WillReturnError(fmt.Errorf("kaboom"))

	keep := "4"
	sourceRefs := []string{keep}
	err = sor.DeleteUnwanted(ctx, testhelper.TestLogger(), &so, sourceRefs, &MockServicePlanRepository{})
	checkErrors(t, err, mock, sor, "TestDeleteUnwantedMissingInstance", "kaboom")
}

func checkErrors(t *testing.T, err error, mock sqlmock.Sqlmock, sor Repository, where string, errMessage string) {
	assert.NotNil(t, err, where)

	if !strings.Contains(err.Error(), errMessage) {
		t.Fatalf("Error message should have contained %s", errMessage)
	}

	assert.NoError(t, mock.ExpectationsWereMet(), "There were unfulfilled expectations for %s", where)
	stats := sor.Stats()
	assert.Equal(t, stats["adds"], 0)
	assert.Equal(t, stats["updates"], 0)
	assert.Equal(t, stats["deletes"], 0)
}
