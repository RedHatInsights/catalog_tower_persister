package payload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/mocks"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/source"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/tenant"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/testhelper"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func dummyObjectRepos(addError error, deleteError error) *ObjectRepos {
	return &ObjectRepos{
		servicecredentialrepo:     &mocks.MockServiceCredentialRepository{AddError: addError, DeleteError: deleteError},
		servicecredentialtyperepo: &mocks.MockServiceCredentialTypeRepository{AddError: addError, DeleteError: deleteError},
		serviceinventoryrepo:      &mocks.MockServiceInventoryRepository{AddError: addError, DeleteError: deleteError},
		serviceplanrepo:           &mocks.MockServicePlanRepository{AddError: addError, DeleteError: deleteError},
		serviceofferingrepo:       &mocks.MockServiceOfferingRepository{AddError: addError, DeleteError: deleteError},
		serviceofferingnoderepo:   &mocks.MockServiceOfferingNodeRepository{AddError: addError, DeleteError: deleteError},
	}
}

type mockLoader struct {
	pageError     error
	linkerError   error
	deleteError   error
	pageCount     int
	linkerCalled  bool
	deletesCalled bool
}

func (ml *mockLoader) ProcessPage(ctx context.Context, name string, r io.Reader) error {
	ml.pageCount++
	return ml.pageError
}

func (ml *mockLoader) ProcessLinks(ctx context.Context, dbTransaction *gorm.DB) error {
	ml.linkerCalled = true
	return ml.linkerError
}

func (ml *mockLoader) ProcessDeletes(ctx context.Context) error {
	ml.deletesCalled = true
	return ml.deleteError
}

func (ml *mockLoader) GetStats(ctx context.Context) map[string]interface{} {
	x := map[string]interface{}{
		"test": 1,
	}
	return x
}

type fakeTransport struct {
	body   io.ReadCloser
	status int
	T      *testing.T
}

func (f *fakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp := &http.Response{
		StatusCode: f.status,
		Status:     http.StatusText(f.status),
		Body:       f.body,
		Header: http.Header{
			"Content-Type": {"application/x-gtar"},
		},
	}
	return resp, nil
}

func fakeClient(t *testing.T, body io.ReadCloser, status int) *http.Client {
	return &http.Client{
		Transport: &fakeTransport{body: body, status: status, T: t},
	}
}

var testTenant = tenant.Tenant{ID: int64(999)}
var testSource = source.Source{ID: int64(989)}

func createPayload(objType string) string {
	dataFormat := `{
           "count": 2,
           "next": "/api/v2/someobject/?page=2",
           "previous": null,
           "results": [
            {
               "id": 73,
	       "type": "%s"
	    },
            {
               "id": 78,
	       "type": "%s"
            }
	   ]
        }`
	return fmt.Sprintf(dataFormat, objType, objType)
}

func TestProcessTarSuccess(t *testing.T) {
	ctx := context.TODO()
	shutdown := make(chan struct{})
	url := "https://www.example.com/data.tar"
	ml := mockLoader{}
	f, err := os.Open("testdata/sample.tgz")
	if err != nil {
		t.Errorf("Error opening file %s %v", "testdata/sample.tgz", err)
	}
	defer f.Close()
	fc := fakeClient(t, f, http.StatusOK)
	err = ProcessTar(ctx, testhelper.TestLogger(), &ml, fc, nil, url, shutdown)

	assert.Nil(t, err, "Should have parsed payload")
	assert.Equal(t, ml.pageCount, 14, "14 Pages should be processed")
	assert.True(t, ml.linkerCalled, true, "Linker should get called")
	assert.True(t, ml.deletesCalled, true, "Deletes should get called")
}

func TestProcessTarNotFound(t *testing.T) {
	ctx := context.TODO()
	shutdown := make(chan struct{})
	url := "https://www.example.com/data.tar"
	ml := mockLoader{}
	f, err := os.Open("testdata/sample.tgz")
	if err != nil {
		t.Errorf("Error opening file %s %v", "testdata/sample.tgz", err)
	}
	defer f.Close()
	fc := fakeClient(t, f, http.StatusNotFound)
	err = ProcessTar(ctx, testhelper.TestLogger(), &ml, fc, nil, url, shutdown)

	assert.NotNil(t, err, "Should not have parsed payload")
	assert.Equal(t, ml.pageCount, 0, "0 Pages should be processed")
}

func TestProcessBadTarFailure(t *testing.T) {
	ctx := context.TODO()
	shutdown := make(chan struct{})
	url := "https://www.example.com/data.tar"
	ml := mockLoader{}
	body := ioutil.NopCloser(bytes.NewBufferString("ABSBSB"))
	fc := fakeClient(t, body, http.StatusOK)
	err := ProcessTar(ctx, testhelper.TestLogger(), &ml, fc, nil, url, shutdown)

	assert.NotNil(t, err, "Should not have parsed payload")
	assert.Equal(t, ml.pageCount, 0, "0 Pages should be processed")
}

var errorCases = []struct {
	errMessage string
	ml         mockLoader
}{
	{"Kaboom in Page Handler", mockLoader{pageError: fmt.Errorf("Kaboom in Page Handler")}},
	{"Kaboom in Linker", mockLoader{linkerError: fmt.Errorf("Kaboom in Linker")}},
	{"Kaboom in Deletes", mockLoader{deleteError: fmt.Errorf("Kaboom in Deletes")}},
}

func TestProcessFailures(t *testing.T) {
	for _, tt := range errorCases {
		ctx := context.TODO()
		shutdown := make(chan struct{})
		url := "https://www.example.com/data.tar"
		f, err := os.Open("testdata/sample.tgz")
		if err != nil {
			t.Errorf("Error opening file %s %v", "testdata/sample.tgz", err)
		}
		defer f.Close()
		fc := fakeClient(t, f, http.StatusOK)
		err = ProcessTar(ctx, testhelper.TestLogger(), &tt.ml, fc, nil, url, shutdown)

		assert.NotNil(t, err, "Shouldn't have parsed payload")
		if !strings.Contains(err.Error(), tt.errMessage) {
			t.Fatalf("Error message should have contained %s", tt.errMessage)
		}
	}
}
