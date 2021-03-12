package mocks

import (
	"context"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceoffering"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceplan"
	"github.com/sirupsen/logrus"
)

type MockServiceOfferingRepository struct {
	DeletesCalled int
	AddsCalled    int
	UpdatesCalled int
	AddError      error
	DeleteError   error
}

func (msor *MockServiceOfferingRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, so *serviceoffering.ServiceOffering, keepSourceRefs []string, spr serviceplan.Repository) error {
	if msor.DeleteError == nil {
		msor.DeletesCalled++
	}
	return msor.DeleteError
}

func (msor *MockServiceOfferingRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, so *serviceoffering.ServiceOffering, attrs map[string]interface{}, spr serviceplan.Repository) error {
	if msor.AddError == nil {
		msor.AddsCalled++
	}
	return msor.AddError
}

func (msor *MockServiceOfferingRepository) Stats() map[string]int {
	return map[string]int{"adds": msor.AddsCalled, "deletes": msor.DeletesCalled, "updates": msor.UpdatesCalled}
}
