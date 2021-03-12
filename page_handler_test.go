package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/mocks"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/source"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/tenant"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/testhelper"
	"github.com/stretchr/testify/assert"
)

func dummyObjectRepos(addError error, deleteError error) *objectRepos {
	return &objectRepos{
		servicecredentialrepo:     &mocks.MockServiceCredentialRepository{AddError: addError, DeleteError: deleteError},
		servicecredentialtyperepo: &mocks.MockServiceCredentialTypeRepository{AddError: addError, DeleteError: deleteError},
		serviceinventoryrepo:      &mocks.MockServiceInventoryRepository{AddError: addError, DeleteError: deleteError},
		serviceplanrepo:           &mocks.MockServicePlanRepository{AddError: addError, DeleteError: deleteError},
		serviceofferingrepo:       &mocks.MockServiceOfferingRepository{AddError: addError, DeleteError: deleteError},
		serviceofferingnoderepo:   &mocks.MockServiceOfferingNodeRepository{AddError: addError, DeleteError: deleteError},
	}
}

var testTenant = tenant.Tenant{ID: int64(999)}
var testSource = source.Source{ID: int64(989)}

func createPayload(objType string) string {
	dataFormat := `{
           "count": 2,
           "next": "/api/v2/someobject/?page=2",
           "previous": null,
           "results": [
            {
               "id": 73,
	       "type": "%s"
	    },
            {
               "id": 78,
	       "type": "%s"
            }
	   ]
        }`
	return fmt.Sprintf(dataFormat, objType, objType)
}

var onlyIDs = `{
     "count": 2,
     "next": "/api/v2/somobject/?page=2",
     "previous": null,
     "results": [
        {"id": 73},
	{"id": 78}
     ]
     }`

var surveySpec = `{
    "name": "",
    "description": "",
    "spec": []
    }`

var singleJobTemplate = `{
    "id": 73,
    "type": "job_template"
    }`

var specTests = []struct {
	url  string
	data string
}{
	{"/api/v2/job_templates/", createPayload("job_template")},
	{"/api/v2/job_templates/73", singleJobTemplate},
	{"/api/v2/credentials/", createPayload("credential")},
	{"/api/v2/credential_types/", createPayload("credential_type")},
	{"/api/v2/inventories/", createPayload("inventory")},
	{"/api/v2/workflow_job_templates/", createPayload("workflow_job_template")},
	{"/api/v2/workflow_job_template_nodes/", createPayload("workflow_job_template_node")},
	{"/api/v2/job_templates/73/survey_spec/page1.json", surveySpec},
}

func TestAdds(t *testing.T) {
	for _, tt := range specTests {
		pc := MakePageContext(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(nil, nil))
		err := pc.Process(context.TODO(), tt.url, strings.NewReader(tt.data))
		assert.Nil(t, err, tt.url)
	}
}

func TestErrors(t *testing.T) {
	kaboom := fmt.Errorf("Kaboom")
	for _, tt := range specTests {
		pc := MakePageContext(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(kaboom, nil))
		err := pc.Process(context.TODO(), tt.url, strings.NewReader(tt.data))
		assert.NotNil(t, err, tt.url)
		if !strings.Contains(err.Error(), "Kaboom") {
			t.Fatalf("Error message should have contained kaboom")
		}
	}

}

func TestBadUrl(t *testing.T) {
	pc := MakePageContext(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(nil, nil))
	err := pc.Process(context.TODO(), "/bogus", strings.NewReader(""))
	assert.NotNil(t, err, "/bogus")
	if !strings.Contains(err.Error(), "Could not get object type from url") {
		t.Fatalf("Error message should have contained %s", "Could not get object type from url")
	}
}

func TestBadType(t *testing.T) {
	pc := MakePageContext(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(nil, nil))
	err := pc.Process(context.TODO(), "/api/v2/job_templates/", strings.NewReader(createPayload("bad")))
	assert.NotNil(t, err, "/api/v2/job_templates/")
	if !strings.Contains(err.Error(), "Invalid Object type found bad") {
		t.Fatalf("Error message should have contained %s", "Invalid Object type found bad")
	}
}

func TestLogReports(t *testing.T) {
	ctx := context.TODO()
	pc := MakePageContext(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(nil, nil))
	err := pc.Process(ctx, "/api/v2/inventories/", strings.NewReader(createPayload("inventory")))
	pc.LogReports(ctx)
	assert.Nil(t, err, "/api/v2/inventories/")
}

func TestIDs(t *testing.T) {
	pc := MakePageContext(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(nil, nil))
	err := pc.Process(context.TODO(), "/api/v2/job_templates/id/page1.json", strings.NewReader(onlyIDs))
	assert.Nil(t, err, "/api/v2/job_templates/id/page1.json")
}

func TestGetStats(t *testing.T) {
	ctx := context.TODO()
	pc := MakePageContext(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(nil, nil))
	err := pc.Process(ctx, "/api/v2/inventories/", strings.NewReader(createPayload("inventory")))
	assert.Nil(t, err, "/api/v2/inventories/")
	stats := pc.GetStats(ctx)["inventories"]
	v, ok := stats.(map[string]int)
	if !ok {
		t.Errorf("Stats is not an interface")
	}
	assert.Equal(t, v["adds"], 2, "inventories")
	assert.Equal(t, v["deletes"], 0, "inventories")
	assert.Equal(t, v["updates"], 0, "inventories")
}
