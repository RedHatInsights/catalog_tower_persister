package mocks

import (
	"context"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/sirupsen/logrus"
)

//MockServiceCredentialTypeRepository used for tests
type MockServiceCredentialTypeRepository struct {
	DeletesCalled int
	AddsCalled    int
	UpdatesCalled int
	AddError      error
	DeleteError   error
}

//DeleteUnwanted objects given a list of objects to keep
func (msctr *MockServiceCredentialTypeRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, sct *servicecredentialtype.ServiceCredentialType, keepSourceRefs []string) error {
	if msctr.DeleteError == nil {
		msctr.DeletesCalled++
	}
	return msctr.DeleteError
}

//CreateOrUpdate an object
func (msctr *MockServiceCredentialTypeRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sct *servicecredentialtype.ServiceCredentialType, attrs map[string]interface{}) error {
	if msctr.AddError == nil {
		msctr.AddsCalled++
	}
	return msctr.AddError
}

//Stats get the count for adds/updates/deletes
func (msctr *MockServiceCredentialTypeRepository) Stats() map[string]int {
	return map[string]int{"adds": msctr.AddsCalled, "deletes": msctr.DeletesCalled, "updates": msctr.UpdatesCalled}
}
