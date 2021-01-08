package catalogtask

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/RedHatInsights/catalog_tower_persister/internal/logger"
	"github.com/stretchr/testify/assert"
)

type fakeTransport struct {
	body          []string
	status        int
	requestNumber int
	T             *testing.T
}

func (f *fakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp := &http.Response{
		StatusCode: f.status,
		Status:     http.StatusText(f.status),
		Body:       ioutil.NopCloser(bytes.NewBufferString(f.body[f.requestNumber])),
		Header: http.Header{
			"Content-Type": {"application/json"},
		},
	}
	f.requestNumber++
	return resp, nil
}

func fakeClient(t *testing.T, body []string, status int) *http.Client {
	return &http.Client{
		Transport: &fakeTransport{body: body, status: status, T: t},
	}
}

var data = map[string]interface{}{
	"name": "Fred Flintstone",
	"age":  45,
}

var url = "http://www.example.com"
var headers = map[string]string{
	xRHIdentity:          "abc",
	xRHInsightsRequestID: "id",
}

func TestUpdateSuccess(t *testing.T) {
	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	ctask := MakeCatalogTask(nctx, url, headers)
	body := []string{"Created"}
	fc := fakeClient(t, body, http.StatusNoContent)
	err := ctask.Update(data, fc)
	assert.Nil(t, err, "Update failed")
}

func TestUpdateMissingHeaders(t *testing.T) {
	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	headers := map[string]string{
		"abc": "id",
	}
	ctask := MakeCatalogTask(nctx, url, headers)
	body := []string{"Created"}
	fc := fakeClient(t, body, http.StatusNoContent)
	err := ctask.Update(data, fc)
	assert.NotNil(t, err, "Update should have failed")

	if !strings.Contains(err.Error(), "X_RH_IDENTITY") {
		t.Fatalf("Error message should have contained X_RH_IDENTITY")
	}
}

func TestUpdateBadStatus(t *testing.T) {
	ctx := context.TODO()
	nctx := logger.CtxWithLoggerID(ctx, "12345")
	ctask := MakeCatalogTask(nctx, url, headers)
	body := []string{"Error Body"}
	fc := fakeClient(t, body, http.StatusBadRequest)
	err := ctask.Update(data, fc)
	assert.NotNil(t, err, "Update should have failed")
	errMsg := "Invalid HTTP Status code from post 400"
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("Error message should have contained %s", errMsg)
	}
}
