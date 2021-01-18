package catalogtask

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/sirupsen/logrus"
)

// CatalogTask interface to update the Task object in cloud.redhat.com
type CatalogTask interface {
	Update(data map[string]interface{}, client *http.Client) error
}

type defaultCatalogTask struct {
	url     string
	ctx     context.Context
	logger  *logrus.Entry
	headers map[string]string
}

const xRHIdentity = "x-rh-identity"
const xRHInsightsRequestID = "x-rh-insights-request-id"

// MakeCatalogTask creates a new Catalog Task object
func MakeCatalogTask(ctx context.Context, logger *logrus.Entry, url string, headers map[string]string) CatalogTask {
	return &defaultCatalogTask{ctx: ctx, url: url, logger: logger, headers: headers}
}

// Update the Task object in the cloud
func (ct *defaultCatalogTask) Update(data map[string]interface{}, client *http.Client) error {
	payload, err := json.Marshal(data)

	if err != nil {
		ct.logger.Errorf("Error Marshaling Payload %v", err)
		return err
	}
	//client := &http.Client{}
	req, err := http.NewRequest(http.MethodPatch, ct.url, bytes.NewBuffer(payload))
	if err != nil {
		ct.logger.Errorf("Error creating a new request %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if _, ok := ct.headers[xRHIdentity]; !ok {
		err = fmt.Errorf("X_RH_IDENTITY is not set message headers")
		ct.logger.Errorf("%v", err)
		return err
	}
	req.Header.Set(xRHIdentity, ct.headers[xRHIdentity])
	if val, ok := ct.headers[xRHInsightsRequestID]; ok {
		req.Header.Set(xRHInsightsRequestID, val)
	}
	resp, err := client.Do(req)
	if err != nil {
		ct.logger.Errorf("Error processing request %v", err)
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		ct.logger.Errorf("Error reading body %v", err)
		return err
	}
	if resp.StatusCode != http.StatusNoContent {
		err = fmt.Errorf("Invalid HTTP Status code from post %d", resp.StatusCode)
		ct.logger.Errorf("Error %v", err)
		return err
	}
	ct.logger.Infof("Task Update Statue Code %d", resp.StatusCode)

	ct.logger.Infof("Response from Patch %s", string(body))
	return nil
}
