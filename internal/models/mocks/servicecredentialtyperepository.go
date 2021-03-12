package mocks

import (
	"context"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/sirupsen/logrus"
)

type MockServiceCredentialTypeRepository struct {
	DeletesCalled int
	AddsCalled    int
	UpdatesCalled int
	Error         error
}

func (msctr *MockServiceCredentialTypeRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, sct *servicecredentialtype.ServiceCredentialType, keepSourceRefs []string) error {
	if msctr.Error == nil {
		msctr.DeletesCalled++
	}
	return msctr.Error
}

func (msctr *MockServiceCredentialTypeRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sct *servicecredentialtype.ServiceCredentialType, attrs map[string]interface{}) error {
	if msctr.Error == nil {
		msctr.AddsCalled++
	}
	return msctr.Error
}

func (msctr *MockServiceCredentialTypeRepository) Stats() map[string]int {
	return map[string]int{"adds": msctr.AddsCalled, "deletes": msctr.DeletesCalled, "updates": msctr.UpdatesCalled}
}
