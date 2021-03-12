package mocks

import (
	"context"
	"io"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceplan"
	"github.com/sirupsen/logrus"
)

type MockServicePlanRepository struct {
	DeletesCalled int
	AddsCalled    int
	UpdatesCalled int
	Error         error
}

func (mspr *MockServicePlanRepository) Delete(ctx context.Context, logger *logrus.Entry, sp *serviceplan.ServicePlan) error {
	if mspr.Error == nil {
		mspr.DeletesCalled++
	}
	return mspr.Error
}

func (mspr *MockServicePlanRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sp *serviceplan.ServicePlan, converter serviceplan.DDFConverter, attrs map[string]interface{}, r io.Reader) error {
	if mspr.Error == nil {
		mspr.AddsCalled++
	}
	return mspr.Error
}

func (mspr *MockServicePlanRepository) Stats() map[string]int {
	return map[string]int{"adds": mspr.AddsCalled, "deletes": mspr.DeletesCalled, "updates": mspr.UpdatesCalled}
}
