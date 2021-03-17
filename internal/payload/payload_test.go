package payload

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

func dummyObjectRepos(addError error, deleteError error) *ObjectRepos {
	return &ObjectRepos{
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

func TestLogReports(t *testing.T) {
	ctx := context.TODO()
	bol := MakeBillOfLading(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(nil, nil), nil)
	err := bol.ProcessPage(ctx, "/api/v2/inventories/", strings.NewReader(createPayload("inventory")))
	bol.logReports(ctx)
	assert.Nil(t, err, "/api/v2/inventories/")
}

func TestGetStats(t *testing.T) {
	ctx := context.TODO()
	bol := MakeBillOfLading(testhelper.TestLogger(), &testTenant, &testSource, dummyObjectRepos(nil, nil), nil)
	err := bol.ProcessPage(ctx, "/api/v2/inventories/", strings.NewReader(createPayload("inventory")))
	assert.Nil(t, err, "/api/v2/inventories/")
	stats := bol.GetStats(ctx)["inventories"]
	v, ok := stats.(map[string]int)
	if !ok {
		t.Errorf("Stats is not an interface")
	}
	assert.Equal(t, v["adds"], 2, "inventories")
	assert.Equal(t, v["deletes"], 0, "inventories")
	assert.Equal(t, v["updates"], 0, "inventories")
}
