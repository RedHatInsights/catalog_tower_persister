package mocks

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceofferingnode"
	"github.com/sirupsen/logrus"
)

//MockServiceOfferingNodeRepository for testing
type MockServiceOfferingNodeRepository struct {
	DeletesCalled int
	AddsCalled    int
	UpdatesCalled int
	AddError      error
	DeleteError   error
}

//DeleteUnwanted objects given a list of objects to keep
func (msonr *MockServiceOfferingNodeRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, son *serviceofferingnode.ServiceOfferingNode, keepSourceRefs []string) error {
	if msonr.DeleteError == nil {
		msonr.DeletesCalled++
	}
	return msonr.DeleteError
}

//CreateOrUpdate object
func (msonr *MockServiceOfferingNodeRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, son *serviceofferingnode.ServiceOfferingNode, attrs map[string]interface{}) error {
	if msonr.AddError == nil {
		for k, v := range attrs {
			switch c := v.(type) {
			case string:
				setServiceOfferingNodeString(son, k, c)
			case int:
				setServiceOfferingNodeInt(son, k, int64(c))
			case json.Number:
				i, _ := strconv.ParseInt(c.String(), 10, 64)
				setServiceOfferingNodeInt(son, k, int64(i))
			case bool:
				setServiceOfferingNodeBool(son, k, c)
			}
		}
		msonr.AddsCalled++
	}
	return msonr.AddError
}

//Stats get the number of adds/updates/deletes
func (msonr *MockServiceOfferingNodeRepository) Stats() map[string]int {
	return map[string]int{"adds": msonr.AddsCalled, "deletes": msonr.DeletesCalled, "updates": msonr.UpdatesCalled}
}

func setServiceOfferingNodeString(son *serviceofferingnode.ServiceOfferingNode, field string, value string) {
	v := reflect.ValueOf(son).Elem().FieldByName(field)
	if v.IsValid() {
		v.SetString(value)
	}
}

func setServiceOfferingNodeInt(son *serviceofferingnode.ServiceOfferingNode, field string, value int64) {
	v := reflect.ValueOf(son).Elem().FieldByName(field)
	if v.IsValid() {
		v.SetInt(value)
	}
}

func setServiceOfferingNodeBool(son *serviceofferingnode.ServiceOfferingNode, field string, value bool) {
	v := reflect.ValueOf(son).Elem().FieldByName(field)
	if v.IsValid() {
		v.SetBool(value)
	}
}
