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
	AddError      error
	DeleteError   error
}

func (mspr *MockServicePlanRepository) Delete(ctx context.Context, logger *logrus.Entry, sp *serviceplan.ServicePlan) error {
	if mspr.DeleteError == nil {
		mspr.DeletesCalled++
	}
	return mspr.DeleteError
}

func (mspr *MockServicePlanRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sp *serviceplan.ServicePlan, converter serviceplan.DDFConverter, attrs map[string]interface{}, r io.Reader) error {
	if mspr.AddError == nil {
		mspr.AddsCalled++
	}
	return mspr.AddError
}

func (mspr *MockServicePlanRepository) Stats() map[string]int {
	return map[string]int{"adds": mspr.AddsCalled, "deletes": mspr.DeletesCalled, "updates": mspr.UpdatesCalled}
}
