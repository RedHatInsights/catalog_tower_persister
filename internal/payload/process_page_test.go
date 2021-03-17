package payload

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/testhelper"
	"github.com/stretchr/testify/assert"
)

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
		bol := MakeBillOfLading(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(nil, nil), nil)
		err := bol.ProcessPage(context.TODO(), tt.url, strings.NewReader(tt.data))
		assert.Nil(t, err, tt.url)
	}
}

func TestErrors(t *testing.T) {
	kaboom := fmt.Errorf("Kaboom")
	for _, tt := range specTests {
		bol := MakeBillOfLading(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(kaboom, nil), nil)
		err := bol.ProcessPage(context.TODO(), tt.url, strings.NewReader(tt.data))
		assert.NotNil(t, err, tt.url)
		if !strings.Contains(err.Error(), "Kaboom") {
			t.Fatalf("Error message should have contained kaboom")
		}
	}

}

func TestBadUrl(t *testing.T) {
	bol := MakeBillOfLading(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(nil, nil), nil)
	err := bol.ProcessPage(context.TODO(), "/bogus", strings.NewReader(""))
	assert.NotNil(t, err, "/bogus")
	if !strings.Contains(err.Error(), "Could not get object type from url") {
		t.Fatalf("Error message should have contained %s", "Could not get object type from url")
	}
}

func TestBadType(t *testing.T) {
	bol := MakeBillOfLading(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(nil, nil), nil)
	err := bol.ProcessPage(context.TODO(), "/api/v2/job_templates/", strings.NewReader(createPayload("bad")))
	assert.NotNil(t, err, "/api/v2/job_templates/")
	if !strings.Contains(err.Error(), "Invalid Object type found bad") {
		t.Fatalf("Error message should have contained %s", "Invalid Object type found bad")
	}
}

func TestIDs(t *testing.T) {
	bol := MakeBillOfLading(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(nil, nil), nil)
	err := bol.ProcessPage(context.TODO(), "/api/v2/job_templates/id/page1.json", strings.NewReader(onlyIDs))
	assert.Nil(t, err, "/api/v2/job_templates/id/page1.json")
}
