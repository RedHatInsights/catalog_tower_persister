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
	Error         error
}

func (msor *MockServiceOfferingRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, so *serviceoffering.ServiceOffering, keepSourceRefs []string, spr serviceplan.Repository) error {
	if msor.Error == nil {
		msor.DeletesCalled++
	}
	return msor.Error
}

func (msor *MockServiceOfferingRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, so *serviceoffering.ServiceOffering, attrs map[string]interface{}, spr serviceplan.Repository) error {
	if msor.Error == nil {
		msor.AddsCalled++
	}
	return msor.Error
}

func (msor *MockServiceOfferingRepository) Stats() map[string]int {
	return map[string]int{"adds": msor.AddsCalled, "deletes": msor.DeletesCalled, "updates": msor.UpdatesCalled}
}
