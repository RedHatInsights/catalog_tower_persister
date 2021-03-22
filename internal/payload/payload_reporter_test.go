package payload

import (
	"context"
	"strings"
	"testing"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/testhelper"
	"github.com/stretchr/testify/assert"
)

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
