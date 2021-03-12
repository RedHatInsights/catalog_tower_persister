package mocks

import (
	"context"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceofferingnode"
	"github.com/sirupsen/logrus"
)

type MockServiceOfferingNodeRepository struct {
	DeletesCalled int
	AddsCalled    int
	UpdatesCalled int
	AddError      error
	DeleteError   error
}

func (msonr *MockServiceOfferingNodeRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, son *serviceofferingnode.ServiceOfferingNode, keepSourceRefs []string) error {
	if msonr.DeleteError == nil {
		msonr.DeletesCalled++
	}
	return msonr.DeleteError
}

func (msonr *MockServiceOfferingNodeRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, son *serviceofferingnode.ServiceOfferingNode, attrs map[string]interface{}) error {
	if msonr.AddError == nil {
		msonr.AddsCalled++
	}
	return msonr.AddError
}

func (msonr *MockServiceOfferingNodeRepository) Stats() map[string]int {
	return map[string]int{"adds": msonr.AddsCalled, "deletes": msonr.DeletesCalled, "updates": msonr.UpdatesCalled}
}
