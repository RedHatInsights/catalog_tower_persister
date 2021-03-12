package mocks

import (
	"context"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredential"
	"github.com/sirupsen/logrus"
)

type MockServiceCredentialRepository struct {
	DeletesCalled int
	AddsCalled    int
	UpdatesCalled int
	DeleteError   error
	AddError      error
}

func (mscr *MockServiceCredentialRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, sc *servicecredential.ServiceCredential, keepSourceRefs []string) error {
	if mscr.DeleteError == nil {
		mscr.DeletesCalled++
	}
	return mscr.DeleteError
}

func (mscr *MockServiceCredentialRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sc *servicecredential.ServiceCredential, attrs map[string]interface{}) error {
	if mscr.AddError == nil {
		mscr.AddsCalled++
	}
	return mscr.AddError
}

func (mscr *MockServiceCredentialRepository) Stats() map[string]int {
	return map[string]int{"adds": mscr.AddsCalled, "deletes": mscr.DeletesCalled, "updates": mscr.UpdatesCalled}
}
