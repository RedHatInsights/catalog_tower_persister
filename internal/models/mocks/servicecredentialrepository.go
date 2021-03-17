package mocks

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredential"
	"github.com/sirupsen/logrus"
)

// MockServiceCredentialRepository used for testing
type MockServiceCredentialRepository struct {
	DeletesCalled int
	AddsCalled    int
	UpdatesCalled int
	DeleteError   error
	AddError      error
}

//DeleteUnwanted deleted unwanted objects given a list of objects to keep
func (mscr *MockServiceCredentialRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, sc *servicecredential.ServiceCredential, keepSourceRefs []string) error {
	if mscr.DeleteError == nil {
		mscr.DeletesCalled++
	}
	return mscr.DeleteError
}

//CreateOrUpdate an object
func (mscr *MockServiceCredentialRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sc *servicecredential.ServiceCredential, attrs map[string]interface{}) error {
	if mscr.AddError == nil {
		for k, v := range attrs {
			switch c := v.(type) {
			case string:
				setServiceCredentialString(sc, k, c)
			case int:
				setServiceCredentialInt(sc, k, int64(c))
			case json.Number:
				i, _ := strconv.ParseInt(c.String(), 10, 64)
				setServiceCredentialInt(sc, k, int64(i))
			case bool:
				setServiceCredentialBool(sc, k, c)
			}
		}
		mscr.AddsCalled++
	}
	return mscr.AddError
}

//Stats get the adds/updates/deletes
func (mscr *MockServiceCredentialRepository) Stats() map[string]int {
	return map[string]int{"adds": mscr.AddsCalled, "deletes": mscr.DeletesCalled, "updates": mscr.UpdatesCalled}
}

func setServiceCredentialString(sc *servicecredential.ServiceCredential, field string, value string) {
	v := reflect.ValueOf(sc).Elem().FieldByName(field)
	if v.IsValid() {
		v.SetString(value)
	}
}

func setServiceCredentialInt(sc *servicecredential.ServiceCredential, field string, value int64) {
	v := reflect.ValueOf(sc).Elem().FieldByName(field)
	if v.IsValid() {
		v.SetInt(value)
	}
}

func setServiceCredentialBool(sc *servicecredential.ServiceCredential, field string, value bool) {
	v := reflect.ValueOf(sc).Elem().FieldByName(field)
	if v.IsValid() {
		v.SetBool(value)
	}
}
