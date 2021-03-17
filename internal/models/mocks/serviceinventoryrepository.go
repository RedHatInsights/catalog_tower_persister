package mocks

import (
	"context"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/sirupsen/logrus"
)

//MockServiceInventoryRepository used for testing
type MockServiceInventoryRepository struct {
	DeletesCalled int
	AddsCalled    int
	UpdatesCalled int
	AddError      error
	DeleteError   error
}

//DeleteUnwanted objects given a list of objects to keep
func (msir *MockServiceInventoryRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, si *serviceinventory.ServiceInventory, keepSourceRefs []string) error {
	if msir.DeleteError == nil {
		msir.DeletesCalled++
	}
	return msir.DeleteError
}

//CreateOrUpdate an object
func (msir *MockServiceInventoryRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, si *serviceinventory.ServiceInventory, attrs map[string]interface{}) error {
	if msir.AddError == nil {
		msir.AddsCalled++
	}
	return msir.AddError
}

//Stats get the number of adds/updates/deletes
func (msir *MockServiceInventoryRepository) Stats() map[string]int {
	return map[string]int{"adds": msir.AddsCalled, "deletes": msir.DeletesCalled, "updates": msir.UpdatesCalled}
}
