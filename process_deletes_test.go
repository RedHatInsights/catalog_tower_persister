package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/testhelper"
	"github.com/stretchr/testify/assert"
)

var deleteSpecTests = []struct {
	url  string
	data string
}{
	{"/api/v2/job_templates/", createPayload("job_template")},
	{"/api/v2/credentials/", createPayload("credential")},
	{"/api/v2/credential_types/", createPayload("credential_type")},
	{"/api/v2/inventories/", createPayload("inventory")},
	{"/api/v2/workflow_job_templates/", createPayload("workflow_job_template")},
}

func TestDeletes(t *testing.T) {
	for _, tt := range deleteSpecTests {
		ctx := context.TODO()
		repos := dummyObjectRepos(nil, nil)
		pc := MakePageContext(testhelper.TestLogger(), &testTenant, &testSource, repos)
		err := pc.Process(ctx, tt.url, strings.NewReader(tt.data))
		assert.Nil(t, err, tt.url)
		err = pc.ProcessDeletes(ctx)
		assert.Nil(t, err, tt.url)
	}
}

func TestDeleteErrors(t *testing.T) {
	kaboom := fmt.Errorf("Kaboom")
	for _, tt := range deleteSpecTests {
		ctx := context.TODO()
		repos := dummyObjectRepos(nil, kaboom)
		pc := MakePageContext(testhelper.TestLogger(), &testTenant, &testSource, repos)
		err := pc.Process(ctx, tt.url, strings.NewReader(tt.data))
		assert.Nil(t, err, tt.url)
		err = pc.ProcessDeletes(ctx)
		assert.NotNil(t, err, tt.url)
		if !strings.Contains(err.Error(), "Kaboom") {
			t.Fatalf("Error message should have contained kaboom")
		}
	}
}
