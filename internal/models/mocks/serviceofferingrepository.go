package mocks

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceoffering"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceplan"
	"github.com/sirupsen/logrus"
)

//MockServiceOfferingRepository for testing
type MockServiceOfferingRepository struct {
	DeletesCalled int
	AddsCalled    int
	UpdatesCalled int
	AddError      error
	DeleteError   error
}

//DeleteUnwanted objects given a list of objects to keep
func (msor *MockServiceOfferingRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, so *serviceoffering.ServiceOffering, keepSourceRefs []string, spr serviceplan.Repository) error {
	if msor.DeleteError == nil {
		msor.DeletesCalled++
	}
	return msor.DeleteError
}

//CreateOrUpdate object
func (msor *MockServiceOfferingRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, so *serviceoffering.ServiceOffering, attrs map[string]interface{}, spr serviceplan.Repository) error {
	if msor.AddError == nil {
		for k, v := range attrs {
			switch c := v.(type) {
			case string:
				setServiceOfferingString(so, k, c)
			case int:
				setServiceOfferingInt(so, k, int64(c))
			case json.Number:
				i, _ := strconv.ParseInt(c.String(), 10, 64)
				setServiceOfferingInt(so, k, int64(i))
			case bool:
				setServiceOfferingBool(so, k, c)
			}
		}
		msor.AddsCalled++
	}
	return msor.AddError
}

//Stats get the number of adds/updates/deletes
func (msor *MockServiceOfferingRepository) Stats() map[string]int {
	return map[string]int{"adds": msor.AddsCalled, "deletes": msor.DeletesCalled, "updates": msor.UpdatesCalled}
}

func setServiceOfferingString(so *serviceoffering.ServiceOffering, field string, value string) {
	v := reflect.ValueOf(so).Elem().FieldByName(field)
	if v.IsValid() {
		v.SetString(value)
	}
}

func setServiceOfferingInt(so *serviceoffering.ServiceOffering, field string, value int64) {
	v := reflect.ValueOf(so).Elem().FieldByName(field)
	if v.IsValid() {
		v.SetInt(value)
	}
}

func setServiceOfferingBool(so *serviceoffering.ServiceOffering, field string, value bool) {
	v := reflect.ValueOf(so).Elem().FieldByName(field)
	if v.IsValid() {
		v.SetBool(value)
	}
}
