package payload

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/testhelper"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

type linkCommon struct {
	data  string
	url   string
	where string
	gdb   *gorm.DB
	mock  sqlmock.Sqlmock
	t     *testing.T
}

var testServiceInventoryData = `{
   "count": 2,
   "next": "something",
   "previous": null,
   "results": [
      {
        "id": 73,
	"ID": 730,
	"type": "job_template",
	"ServiceInventorySourceRef": "55"
      }
   ]
   }`

var testServicePlanData = `{
   "count": 2,
   "next": "something",
   "previous": null,
   "results": [
      {
        "id": 73,
	"ID": 730,
	"type": "job_template",
	"SurveyEnabled": true,
	"SourceRef": "85"
      }
   ]
   }`

var testServicePlanWorkflowData = `{
   "count": 2,
   "next": "something",
   "previous": null,
   "results": [
      {
        "id": 73,
	"ID": 730,
	"type": "workflow_job_template",
	"SurveyEnabled": true,
	"SourceRef": "85"
      }
   ]
   }`
var testCredentialData = `{
   "count": 2,
   "next": "something",
   "previous": null,
   "results": [
      {
        "id": 73,
	"ID": 730,
	"type": "credential",
	"ServiceCredentialTypeSourceRef": "885"
      }
   ]
   }`

var testWorkflowNodeData = `{
   "count": 2,
   "next": "something",
   "previous": null,
   "results": [
      {
        "id": 73,
	"ID": 730,
	"SourceRef": "889",
	"type": "workflow_job_template_node",
	"ServiceOfferingSourceRef": "777",
	"RootServiceOfferingSourceRef": "111",
	"UnifiedJobType": "inventory"
      }
   ]
   }`

var serviceOfferingColumns = []string{"id", "tenant_id", "source_id", "source_ref", "name", "type_name",
	"description", "source_created_at", "created_at", "updated_at",
	"extra"}

var servicePlanColumns = []string{"id", "created_at", "updated_at", "archived_at", "source_ref",
	"source_created_at", "last_seen_at", "name", "description", "extra",
	"create_json_schema", "update_json_schema", "service_offering_id", "tenant_id", "source_id"}
var serviceCredentialTypeColumns = []string{"id", "created_at", "updated_at", "archived_at", "source_ref",
	"source_created_at", "last_seen_at", "name", "description", "kind",
	"namespace", "tenant_id", "source_id"}
var serviceOfferingNodeColumns = []string{"id", "tenant_id", "source_id", "source_ref", "name",
	"source_created_at", "created_at", "updated_at"}
var serviceInventoryColumns = []string{"id", "created_at", "updated_at", "archived_at", "source_ref",
	"source_created_at", "last_seen_at", "name", "description", "extra",
	"tenant_id", "source_id"}
var serviceCredentialColumns = []string{"id", "tenant_id", "source_id", "source_ref", "name", "type_name",
	"description", "source_created_at", "created_at", "updated_at",
	"service_credential_type_id"}
var tenantID = testTenant.ID
var sourceID = testSource.ID

type serviceInventoryTest struct {
	serviceInventorySrcRef string
	serviceInventoryID     int64
	serviceOfferingID      int64
	serviceOfferingSrcRef  string
}

func TestServiceInventoryLink(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	sit := serviceInventoryTest{serviceInventorySrcRef: "55",
		serviceInventoryID:    int64(567),
		serviceOfferingID:     int64(730),
		serviceOfferingSrcRef: "986"}

	lc := linkCommon{data: testServiceInventoryData, url: "/api/v2/job_templates/",
		where: "testServiceInventoryLink", gdb: gdb,
		mock: mock, t: t}

	setInventoryMocks(&lc, &sit, nil, nil, nil)
	checkSuccess(&lc)
}

func TestServiceInventoryLinkError1(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	errMessage := "Blow up during find"
	sit := serviceInventoryTest{serviceInventorySrcRef: "55",
		serviceInventoryID:    int64(567),
		serviceOfferingID:     int64(730),
		serviceOfferingSrcRef: "986"}

	lc := linkCommon{data: testServiceInventoryData, url: "/api/v2/job_templates/",
		where: "TestServiceInventoryLinkError1", gdb: gdb,
		mock: mock, t: t}

	setInventoryMocks(&lc, &sit, fmt.Errorf(errMessage), nil, nil)
	checkErrors(&lc, errMessage)
}

func TestServiceInventoryLinkError2(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	errMessage := "Blow up during find"
	sit := serviceInventoryTest{serviceInventorySrcRef: "55",
		serviceInventoryID:    int64(567),
		serviceOfferingID:     int64(730),
		serviceOfferingSrcRef: "986"}

	lc := linkCommon{data: testServiceInventoryData, url: "/api/v2/job_templates/",
		where: "TestServiceInventoryLinkError2", gdb: gdb,
		mock: mock, t: t}

	setInventoryMocks(&lc, &sit, nil, fmt.Errorf(errMessage), nil)
	checkErrors(&lc, errMessage)
}

func TestServiceInventoryLinkError3(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	errMessage := "Blow up during save"
	sit := serviceInventoryTest{serviceInventorySrcRef: "55",
		serviceInventoryID:    int64(567),
		serviceOfferingID:     int64(730),
		serviceOfferingSrcRef: "986"}

	lc := linkCommon{data: testServiceInventoryData, url: "/api/v2/job_templates/",
		where: "TestServiceInventoryLinkError3", gdb: gdb,
		mock: mock, t: t}

	setInventoryMocks(&lc, &sit, nil, nil, fmt.Errorf(errMessage))
	checkErrors(&lc, errMessage)
}

func setInventoryMocks(lc *linkCommon, sit *serviceInventoryTest, err1, err2, errSave error) {
	str := `SELECT * FROM "service_inventories" WHERE (source_ref= $1 AND tenant_id = $2 AND source_id = $3) AND "service_inventories"."archived_at" IS NULL ORDER BY "service_inventories"."id" LIMIT 1`
	if err1 != nil {
		lc.mock.ExpectQuery(regexp.QuoteMeta(str)).
			WithArgs(sit.serviceInventorySrcRef, tenantID, sourceID).
			WillReturnError(err1)
		return
	}
	rows := sqlmock.NewRows(serviceInventoryColumns).
		AddRow(sit.serviceInventoryID, time.Now(), time.Now(), nil, sit.serviceInventorySrcRef, time.Now(), time.Now(), "test_name", "test_desc", nil, tenantID, sourceID)
	lc.mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sit.serviceInventorySrcRef, tenantID, sourceID).
		WillReturnRows(rows)

	soStr := `SELECT * FROM "service_offerings" WHERE ID = $1 AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`
	if err2 != nil {
		lc.mock.ExpectQuery(regexp.QuoteMeta(soStr)).
			WithArgs(sit.serviceOfferingID).
			WillReturnError(err2)
		return
	}
	soRows := sqlmock.NewRows(serviceOfferingColumns).
		AddRow(sit.serviceOfferingID, tenantID, sourceID, sit.serviceOfferingSrcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), nil)
	lc.mock.ExpectQuery(regexp.QuoteMeta(soStr)).
		WithArgs(sit.serviceOfferingID).
		WillReturnRows(soRows)

	if errSave != nil {
		lc.mock.ExpectExec("^UPDATE").WillReturnError(errSave)
	} else {
		lc.mock.ExpectExec("^UPDATE").WillReturnResult(sqlmock.NewResult(100, 1))
	}
}

type servicePlanTest struct {
	servicePlanSrcRef     string
	servicePlanID         int64
	serviceOfferingSrcRef string
	serviceOfferingID     int64
}

func TestServicePlanWorkflowLink(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	spt := servicePlanTest{servicePlanSrcRef: "85",
		servicePlanID:         int64(567),
		serviceOfferingID:     int64(730),
		serviceOfferingSrcRef: "85"}
	lc := linkCommon{data: testServicePlanWorkflowData, url: "/api/v2/workflow_job_templates/",
		where: "TestServicePlanWorkflowLink", gdb: gdb,
		mock: mock, t: t}

	setServicePlanMocks(&lc, &spt, nil, nil, nil)
	checkSuccess(&lc)
}

func TestServicePlanWorkflowLinkError1(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	errMessage := "Blow up during find"
	spt := servicePlanTest{servicePlanSrcRef: "85",
		servicePlanID:         int64(567),
		serviceOfferingID:     int64(730),
		serviceOfferingSrcRef: "85"}
	lc := linkCommon{data: testServicePlanWorkflowData, url: "/api/v2/workflow_job_templates/",
		where: "TestServicePlanWorkflowLinkError1", gdb: gdb,
		mock: mock, t: t}

	setServicePlanMocks(&lc, &spt, fmt.Errorf(errMessage), nil, nil)
	checkErrors(&lc, errMessage)
}

func TestServicePlanWorkflowLinkError2(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	errMessage := "Blow up during find"
	spt := servicePlanTest{servicePlanSrcRef: "85",
		servicePlanID:         int64(567),
		serviceOfferingID:     int64(730),
		serviceOfferingSrcRef: "85"}
	lc := linkCommon{data: testServicePlanWorkflowData, url: "/api/v2/workflow_job_templates/",
		where: "TestServicePlanWorkflowLinkError2", gdb: gdb,
		mock: mock, t: t}

	setServicePlanMocks(&lc, &spt, nil, fmt.Errorf(errMessage), nil)
	checkErrors(&lc, errMessage)
}

func TestServicePlanWorkflowLinkError3(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	errMessage := "Blow up during save"
	spt := servicePlanTest{servicePlanSrcRef: "85",
		servicePlanID:         int64(567),
		serviceOfferingID:     int64(730),
		serviceOfferingSrcRef: "85"}
	lc := linkCommon{data: testServicePlanWorkflowData, url: "/api/v2/workflow_job_templates/",
		where: "TestServicePlanWorkflowLinkError3", gdb: gdb,
		mock: mock, t: t}

	setServicePlanMocks(&lc, &spt, nil, nil, fmt.Errorf(errMessage))
	checkErrors(&lc, errMessage)
}

func setServicePlanMocks(lc *linkCommon, spt *servicePlanTest, err1, err2, errSave error) {
	str := `SELECT * FROM "service_plans" WHERE (source_ref= $1 AND tenant_id = $2 AND source_id = $3) AND "service_plans"."archived_at" IS NULL ORDER BY "service_plans"."id" LIMIT 1`
	if err1 != nil {
		lc.mock.ExpectQuery(regexp.QuoteMeta(str)).
			WithArgs(spt.servicePlanSrcRef, tenantID, sourceID).
			WillReturnError(err1)
		return
	}
	rows := sqlmock.NewRows(servicePlanColumns).
		AddRow(spt.servicePlanID, time.Now(), time.Now(), nil, spt.servicePlanSrcRef, time.Now(), time.Now(), "test_name", "test_desc", nil, nil, nil, nil, tenantID, sourceID)
	lc.mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(spt.servicePlanSrcRef, tenantID, sourceID).
		WillReturnRows(rows)

	soStr := `SELECT * FROM "service_offerings" WHERE (source_ref= $1 AND tenant_id = $2 AND source_id = $3) AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`
	if err2 != nil {
		lc.mock.ExpectQuery(regexp.QuoteMeta(soStr)).
			WithArgs(spt.serviceOfferingSrcRef, tenantID, sourceID).
			WillReturnError(err2)
		return
	}
	soRows := sqlmock.NewRows(serviceOfferingColumns).
		AddRow(spt.serviceOfferingID, tenantID, sourceID, spt.serviceOfferingSrcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), nil)
	lc.mock.ExpectQuery(regexp.QuoteMeta(soStr)).
		WithArgs(spt.serviceOfferingSrcRef, tenantID, sourceID).
		WillReturnRows(soRows)

	if errSave != nil {
		lc.mock.ExpectExec("^UPDATE").WillReturnError(errSave)
	} else {
		lc.mock.ExpectExec("^UPDATE").WillReturnResult(sqlmock.NewResult(100, 1))
	}
}

func TestServicePlanJobTemplateLink(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	spt := servicePlanTest{servicePlanSrcRef: "85",
		servicePlanID:         int64(567),
		serviceOfferingID:     int64(730),
		serviceOfferingSrcRef: "85"}
	lc := linkCommon{data: testServicePlanData, url: "/api/v2/job_templates/",
		where: "TestServicePlanJobTemplateLink", gdb: gdb,
		mock: mock, t: t}

	setServicePlanMocks(&lc, &spt, nil, nil, nil)
	checkSuccess(&lc)
}

func TestServicePlanJobTemplateLinkError1(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	errMessage := "Blow up during find"
	spt := servicePlanTest{servicePlanSrcRef: "85",
		servicePlanID:         int64(567),
		serviceOfferingID:     int64(730),
		serviceOfferingSrcRef: "85"}
	lc := linkCommon{data: testServicePlanData, url: "/api/v2/job_templates/",
		where: "TestServicePlanJobTemplateLinkError1", gdb: gdb,
		mock: mock, t: t}

	setServicePlanMocks(&lc, &spt, fmt.Errorf(errMessage), nil, nil)
	checkErrors(&lc, errMessage)
}

func TestServicePlanJobTemplateLinkError2(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	errMessage := "Blow up during second find"
	spt := servicePlanTest{servicePlanSrcRef: "85",
		servicePlanID:         int64(567),
		serviceOfferingID:     int64(730),
		serviceOfferingSrcRef: "85"}
	lc := linkCommon{data: testServicePlanData, url: "/api/v2/job_templates/",
		where: "TestServicePlanJobTemplateLinkError2", gdb: gdb,
		mock: mock, t: t}

	setServicePlanMocks(&lc, &spt, nil, fmt.Errorf(errMessage), nil)
	checkErrors(&lc, errMessage)
}

func TestServicePlanJobTemplateLinkError3(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()

	errMessage := "Blow up during save"
	spt := servicePlanTest{servicePlanSrcRef: "85",
		servicePlanID:         int64(567),
		serviceOfferingID:     int64(730),
		serviceOfferingSrcRef: "85"}
	lc := linkCommon{data: testServicePlanData, url: "/api/v2/job_templates/",
		where: "TestServicePlanJobTemplateLinkError3", gdb: gdb,
		mock: mock, t: t}

	setServicePlanMocks(&lc, &spt, nil, nil, fmt.Errorf(errMessage))
	checkErrors(&lc, errMessage)
}

type serviceCredentialTest struct {
	credentialTypeSrcRef string
	credentialTypeID     int64
	credentialSrcRef     string
	credentialID         int64
}

func TestCredentialTypeLink(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	sct := serviceCredentialTest{credentialTypeSrcRef: "885",
		credentialTypeID: int64(567),
		credentialID:     int64(730),
		credentialSrcRef: "585"}
	lc := linkCommon{data: testCredentialData, url: "/api/v2/credentials/",
		where: "TestCredentialTypeLink", gdb: gdb,
		mock: mock, t: t}

	setCredentialMocks(&lc, &sct, nil, nil, nil)
	checkSuccess(&lc)
}

func TestCredentialTypeLinkError1(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	errMessage := "Blow up during first find"
	sct := serviceCredentialTest{credentialTypeSrcRef: "885",
		credentialTypeID: int64(567),
		credentialID:     int64(730),
		credentialSrcRef: "585"}
	lc := linkCommon{data: testCredentialData, url: "/api/v2/credentials/",
		where: "TestCredentialTypeLinkError1", gdb: gdb,
		mock: mock, t: t}

	setCredentialMocks(&lc, &sct, fmt.Errorf(errMessage), nil, nil)
	checkErrors(&lc, errMessage)
}

func TestCredentialTypeLinkError2(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	errMessage := "Blow up during second find"
	sct := serviceCredentialTest{credentialTypeSrcRef: "885",
		credentialTypeID: int64(567),
		credentialID:     int64(730),
		credentialSrcRef: "585"}
	lc := linkCommon{data: testCredentialData, url: "/api/v2/credentials/",
		where: "TestCredentialTypeLinkError2", gdb: gdb,
		mock: mock, t: t}

	setCredentialMocks(&lc, &sct, nil, fmt.Errorf(errMessage), nil)
	checkErrors(&lc, errMessage)
}

func TestCredentialTypeLinkError3(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	errMessage := "Blow up during save"
	sct := serviceCredentialTest{credentialTypeSrcRef: "885",
		credentialTypeID: int64(567),
		credentialID:     int64(730),
		credentialSrcRef: "585"}
	lc := linkCommon{data: testCredentialData, url: "/api/v2/credentials/",
		where: "TestCredentialTypeLinkError3", gdb: gdb,
		mock: mock, t: t}

	setCredentialMocks(&lc, &sct, nil, nil, fmt.Errorf(errMessage))
	checkErrors(&lc, errMessage)
}

func setCredentialMocks(lc *linkCommon, sct *serviceCredentialTest, err1, err2, errSave error) {
	str := `SELECT * FROM "service_credential_types" WHERE (source_ref= $1 AND tenant_id = $2 AND source_id = $3) AND "service_credential_types"."archived_at" IS NULL ORDER BY "service_credential_types"."id" LIMIT 1`
	if err1 != nil {
		lc.mock.ExpectQuery(regexp.QuoteMeta(str)).
			WithArgs(sct.credentialTypeSrcRef, tenantID, sourceID).
			WillReturnError(err1)
		return
	}
	rows := sqlmock.NewRows(serviceCredentialTypeColumns).
		AddRow(sct.credentialTypeID, time.Now(), time.Now(), nil, sct.credentialTypeSrcRef, time.Now(), time.Now(), "test_name", "test_desc", "test_kind", "test_ns", tenantID, sourceID)
	lc.mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(sct.credentialTypeSrcRef, tenantID, sourceID).
		WillReturnRows(rows)

	credentialStr := `SELECT * FROM "service_credentials" WHERE ID = $1 AND "service_credentials"."archived_at" IS NULL ORDER BY "service_credentials"."id" LIMIT 1`
	if err2 != nil {
		lc.mock.ExpectQuery(regexp.QuoteMeta(credentialStr)).
			WithArgs(sct.credentialID).
			WillReturnError(err2)
		return
	}
	credentialRows := sqlmock.NewRows(serviceCredentialColumns).
		AddRow(sct.credentialID, tenantID, sourceID, sct.credentialSrcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), nil)
	lc.mock.ExpectQuery(regexp.QuoteMeta(credentialStr)).
		WithArgs(sct.credentialID).
		WillReturnRows(credentialRows)

	if errSave != nil {
		lc.mock.ExpectExec("^UPDATE").WillReturnError(errSave)
	} else {
		lc.mock.ExpectExec("^UPDATE").WillReturnResult(sqlmock.NewResult(100, 1))
	}
}

type serviceNodeTest struct {
	sonSrcRef    string
	sonID        int64
	parentSrcRef string
	parentID     int64
	rootSrcRef   string
	rootID       int64
}

func TestServiceNodeLink(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	snt := serviceNodeTest{sonSrcRef: "889",
		sonID:        int64(567),
		parentSrcRef: "777",
		parentID:     int64(568),
		rootSrcRef:   "111",
		rootID:       int64(668)}
	lc := linkCommon{data: testWorkflowNodeData, url: "/api/v2/workflow_job_template_node/",
		where: "TestServiceNodeLink", gdb: gdb,
		mock: mock, t: t}
	setServiceNodeMocks(&lc, &snt, nil, nil, nil, nil)
	checkSuccess(&lc)
}

func TestServiceNodeLinkError1(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	errMessage := "Blow up during first find"
	snt := serviceNodeTest{sonSrcRef: "889",
		sonID:        int64(567),
		parentSrcRef: "777",
		parentID:     int64(568),
		rootSrcRef:   "111",
		rootID:       int64(668)}
	lc := linkCommon{data: testWorkflowNodeData, url: "/api/v2/workflow_job_template_node/",
		where: "TestServiceNodeLinkError1", gdb: gdb,
		mock: mock, t: t}
	setServiceNodeMocks(&lc, &snt, fmt.Errorf(errMessage), nil, nil, nil)
	checkErrors(&lc, errMessage)
}

func TestServiceNodeLinkError2(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	errMessage := "Blow up during second find"
	snt := serviceNodeTest{sonSrcRef: "889",
		sonID:        int64(567),
		parentSrcRef: "777",
		parentID:     int64(568),
		rootSrcRef:   "111",
		rootID:       int64(668)}
	lc := linkCommon{data: testWorkflowNodeData, url: "/api/v2/workflow_job_template_node/",
		where: "TestServiceNodeLinkError2", gdb: gdb,
		mock: mock, t: t}
	setServiceNodeMocks(&lc, &snt, nil, fmt.Errorf(errMessage), nil, nil)
	checkErrors(&lc, errMessage)
}

func TestServiceNodeLinkError3(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	errMessage := "Blow up during third find"
	snt := serviceNodeTest{sonSrcRef: "889",
		sonID:        int64(567),
		parentSrcRef: "777",
		parentID:     int64(568),
		rootSrcRef:   "111",
		rootID:       int64(668)}
	lc := linkCommon{data: testWorkflowNodeData, url: "/api/v2/workflow_job_template_node/",
		where: "TestServiceNodeLinkError3", gdb: gdb,
		mock: mock, t: t}
	setServiceNodeMocks(&lc, &snt, nil, nil, fmt.Errorf(errMessage), nil)
	checkErrors(&lc, errMessage)
}

func TestServiceNodeLinkError4(t *testing.T) {
	gdb, mock, teardown := testhelper.MockDBSetup(t)
	defer teardown()
	errMessage := "Blow up during save"
	snt := serviceNodeTest{sonSrcRef: "889",
		sonID:        int64(567),
		parentSrcRef: "777",
		parentID:     int64(568),
		rootSrcRef:   "111",
		rootID:       int64(668)}
	lc := linkCommon{data: testWorkflowNodeData, url: "/api/v2/workflow_job_template_node/",
		where: "TestServiceNodeLinkError4", gdb: gdb,
		mock: mock, t: t}
	setServiceNodeMocks(&lc, &snt, nil, nil, nil, fmt.Errorf(errMessage))
	checkErrors(&lc, errMessage)
}

func setServiceNodeMocks(lc *linkCommon, snt *serviceNodeTest, err1, err2, err3, errSave error) {
	str := `SELECT * FROM "service_offering_nodes" WHERE (source_ref= $1 AND tenant_id = $2 AND source_id = $3) AND "service_offering_nodes"."archived_at" IS NULL ORDER BY "service_offering_nodes"."id" LIMIT 1`
	if err1 != nil {
		lc.mock.ExpectQuery(regexp.QuoteMeta(str)).
			WithArgs(snt.sonSrcRef, tenantID, sourceID).
			WillReturnError(err1)
		return
	}
	rows := sqlmock.NewRows(serviceOfferingNodeColumns).
		AddRow(snt.sonID, tenantID, sourceID, snt.sonSrcRef, "Test", time.Now(), time.Now(), time.Now())
	lc.mock.ExpectQuery(regexp.QuoteMeta(str)).
		WithArgs(snt.sonSrcRef, tenantID, sourceID).
		WillReturnRows(rows)

	soStr := `SELECT * FROM "service_offerings" WHERE (source_ref= $1 AND tenant_id = $2 AND source_id = $3) AND "service_offerings"."archived_at" IS NULL ORDER BY "service_offerings"."id" LIMIT 1`
	if err2 != nil {
		lc.mock.ExpectQuery(regexp.QuoteMeta(soStr)).
			WithArgs(snt.parentSrcRef, tenantID, sourceID).
			WillReturnError(err2)
		return
	}
	parent := sqlmock.NewRows(serviceOfferingColumns).
		AddRow(snt.parentID, tenantID, sourceID, snt.parentSrcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), nil)
	lc.mock.ExpectQuery(regexp.QuoteMeta(soStr)).
		WithArgs(snt.parentSrcRef, tenantID, sourceID).
		WillReturnRows(parent)

	if err3 != nil {
		lc.mock.ExpectQuery(regexp.QuoteMeta(soStr)).
			WithArgs(snt.rootSrcRef, tenantID, sourceID).
			WillReturnError(err3)
		return
	}
	root := sqlmock.NewRows(serviceOfferingColumns).
		AddRow(snt.rootID, tenantID, sourceID, snt.rootSrcRef, "Test", "", "Test Description", time.Now(), time.Now(), time.Now(), nil)
	lc.mock.ExpectQuery(regexp.QuoteMeta(soStr)).
		WithArgs(snt.rootSrcRef, tenantID, sourceID).
		WillReturnRows(root)

	if errSave != nil {
		lc.mock.ExpectExec("^UPDATE").WillReturnError(errSave)
	} else {
		lc.mock.ExpectExec("^UPDATE").WillReturnResult(sqlmock.NewResult(100, 1))
	}
}

func checkSuccess(lc *linkCommon) {
	ctx := context.TODO()
	repos := dummyObjectRepos(nil, nil)
	bol := MakeBillOfLading(testhelper.TestLogger(), &testTenant, &testSource, repos, lc.gdb)
	err := bol.ProcessPage(ctx, lc.url, strings.NewReader(lc.data))
	assert.Nil(lc.t, err, lc.url)
	err = bol.ProcessLinks(ctx, lc.gdb)
	assert.Nil(lc.t, err, lc.url)
}

func checkErrors(lc *linkCommon, errMessage string) {
	ctx := context.TODO()
	repos := dummyObjectRepos(nil, nil)
	bol := MakeBillOfLading(testhelper.TestLogger(), &testTenant, &testSource, repos, lc.gdb)
	err := bol.ProcessPage(ctx, lc.url, strings.NewReader(lc.data))
	assert.Nil(lc.t, err, lc.url)
	err = bol.ProcessLinks(ctx, lc.gdb)
	assert.NotNil(lc.t, err, lc.where)

	if !strings.Contains(err.Error(), errMessage) {
		lc.t.Fatalf("Error message should have contained %s", errMessage)
	}

	assert.NoError(lc.t, lc.mock.ExpectationsWereMet(), "There were unfulfilled expectations for %s", lc.where)
}
