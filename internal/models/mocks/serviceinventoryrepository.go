package mocks

import (
	"context"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/sirupsen/logrus"
)

type MockServiceInventoryRepository struct {
	DeletesCalled int
	AddsCalled    int
	UpdatesCalled int
	Error         error
}

func (msir *MockServiceInventoryRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, si *serviceinventory.ServiceInventory, keepSourceRefs []string) error {
	if msir.Error == nil {
		msir.DeletesCalled++
	}
	return msir.Error
}

func (msir *MockServiceInventoryRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, si *serviceinventory.ServiceInventory, attrs map[string]interface{}) error {
	if msir.Error == nil {
		msir.AddsCalled++
	}
	return msir.Error
}

func (msir *MockServiceInventoryRepository) Stats() map[string]int {
	return map[string]int{"adds": msir.AddsCalled, "deletes": msir.DeletesCalled, "updates": msir.UpdatesCalled}
}
