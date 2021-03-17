package payload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredential"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceoffering"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceofferingnode"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceplan"
	"github.com/RedHatInsights/catalog_tower_persister/internal/spec2ddf"
)

// pageResponse stores the response from the Ansible Tower API call which
// has been bundled into the tar file.
type pageResponse map[string]interface{}

var surveySpecRe = regexp.MustCompile(`api\/v2\/(job_templates|workflow_job_templates)\/(.*)\/survey_spec/page1.json`)

var objTypeRe = regexp.MustCompile(`\/api\/v2\/(.*)\/`)

// ProcessPage handles one file at a time from the tar file
func (bol *BillOfLading) ProcessPage(ctx context.Context, url string, r io.Reader) error {
	objectType, err := getObjectType(url)
	if err != nil {
		bol.logger.Errorf("%v", err)
		return err
	}
	// Survey Spec have a different format and are never returned as a list
	// They don't have ID's so we need to handle them separately, also we dont
	// want to read the file twice,
	if objectType == "survey_spec" {
		obj := make(map[string]interface{})
		return bol.addObject(ctx, obj, url, r)
	}
	var pr pageResponse
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	err = decoder.Decode(&pr)
	if err != nil {
		bol.logger.Errorf("Error decoding message body %s %v", url, err)
		return err
	}

	if isListResults(pr) {
		ids := strings.Contains(url, "/id")
		bol.logger.Infof("Received %s objects idObject %v", pr["count"].(json.Number).String(), ids)
		if val, ok := pr["results"]; ok {
			for _, obj := range val.([]interface{}) {
				if ids {
					err = bol.addIDList(ctx, obj.(map[string]interface{}), objectType)
				} else {
					err = bol.addObject(ctx, obj.(map[string]interface{}), url, nil)
					if err != nil {
						bol.logger.Errorf("Error adding object %s", objectType)
						bol.logger.Errorf("Error %v", err)
						return err
					}
				}
			}
		}
	} else {
		err = bol.addObject(ctx, pr, url, nil)
		if err != nil {
			bol.logger.Errorf("Error adding object %s", url)
			bol.logger.Errorf("Error %v", err)
			return err
		}
	}

	return nil
}

// isListResults checks if the response is a single object or a list of objects
func isListResults(pr pageResponse) bool {
	if _, ok1 := pr["results"]; ok1 {
		if _, ok2 := pr["count"]; ok2 {
			if _, ok3 := pr["next"]; ok3 {
				if _, ok4 := pr["previous"]; ok4 {
					return true
				}
			}
		}
	}
	return false
}

// idExists check if an id exists in the list
func idExists(ids []string, id string) bool {
	for _, str := range ids {
		if str == id {
			return true
		}
	}
	return false
}

// addIDList stores the ID of the current object based on its type so we can link can together
func (bol *BillOfLading) addIDList(ctx context.Context, obj map[string]interface{}, objType string) error {
	id := obj["id"].(json.Number).String()
	switch objType {
	case "job_template", "job_templates":
		if !idExists(bol.jobTemplateSourceRefs, id) {
			bol.jobTemplateSourceRefs = append(bol.jobTemplateSourceRefs, id)
		}
	case "workflow_job_template", "workflow_job_templates":
		if !idExists(bol.jobTemplateSourceRefs, id) {
			bol.jobTemplateSourceRefs = append(bol.jobTemplateSourceRefs, id)
		}
	case "inventory", "inventories":
		if !idExists(bol.inventorySourceRefs, id) {
			bol.inventorySourceRefs = append(bol.inventorySourceRefs, id)
		}
	case "credential", "credentials":
		if !idExists(bol.credentialSourceRefs, id) {
			bol.credentialSourceRefs = append(bol.credentialSourceRefs, id)
		}
	case "credential_type", "credential_types":
		if !idExists(bol.credentialTypeSourceRefs, id) {
			bol.credentialTypeSourceRefs = append(bol.credentialTypeSourceRefs, id)
		}
	case "workflow_job_template_node", "workflow_job_template_nodes":
		if !idExists(bol.workflowNodeSourceRefs, id) {
			bol.workflowNodeSourceRefs = append(bol.workflowNodeSourceRefs, id)
		}
	case "survey_spec":
	default:
		bol.logger.Errorf("Invalid Object type found %s", objType)
		return fmt.Errorf("Invalid Object type found %s", objType)
	}
	return nil
}

// addObject add an object into the Database
func (bol *BillOfLading) addObject(ctx context.Context, obj map[string]interface{}, url string, r io.Reader) error {
	var err error
	if _, ok := obj["type"]; !ok {
		// api/v2/job_templates/10/survey_spec
		s := surveySpecRe.FindStringSubmatch(url)
		if len(s) > 1 {
			obj["id"] = json.Number(s[2])
			obj["type"] = "survey_spec"
			obj["name"] = ""
			obj["description"] = ""
			// https://github.com/ansible/tower/issues/4685
			// There is a bug in the Ansible Tower when a survey is added and then the survey
			// is deleted, the survey enabled flag is not reset back to false
			/*
				if _, ok := obj["name"]; !ok {
					bol.logger.Infof("Survey Enabled is incorrectly set, ignoring it for now")
					sp := serviceplan.ServicePlan{Source: *bol.Source, Tenant: *bol.Tenant,
						Tower: base.Tower{SourceRef: s[2]}}
					err := sp.Delete(bol.dbTransaction)
					if err != nil {
						bol.logger.Errorf("Error deleting straggler survey spec %v", err)
					}
					// Ignore the error for now
					return nil
				} */
		} else {
			return errors.New("No type provided")
		}
	}

	bol.logger.Infof("Object Type %s Source Ref %s", obj["type"].(string), obj["id"].(json.Number).String())
	srcRef := obj["id"].(json.Number).String()
	switch objType := obj["type"].(string); objType {
	case "job_template", "workflow_job_template":
		so := &serviceoffering.ServiceOffering{SourceID: bol.source.ID, TenantID: bol.tenant.ID}
		err = bol.repos.serviceofferingrepo.CreateOrUpdate(ctx, bol.logger, so, obj, bol.repos.serviceplanrepo)
		if err != nil {
			bol.logger.Errorf("Error adding %s:%s %v", objType, srcRef, err)
			return err
		}

		if so.SurveyEnabled {
			bol.logger.Infof("Survey Enabled for " + so.SourceRef)
			if objType == "job_template" {
				bol.jobTemplateSurvey = append(bol.jobTemplateSurvey, so.SourceRef)
			} else {
				bol.workflowJobTemplateSurvey = append(bol.workflowJobTemplateSurvey, so.SourceRef)
			}
		}

		if so.ServiceInventorySourceRef != "" {
			if _, ok := bol.inventoryMap[so.ServiceInventorySourceRef]; ok {
				bol.inventoryMap[so.ServiceInventorySourceRef] = append(bol.inventoryMap[so.ServiceInventorySourceRef], so.ID)
			} else {
				bol.inventoryMap[so.ServiceInventorySourceRef] = []int64{so.ID}
			}
		}

	case "inventory":
		si := &serviceinventory.ServiceInventory{SourceID: bol.source.ID, TenantID: bol.tenant.ID}
		err = bol.repos.serviceinventoryrepo.CreateOrUpdate(ctx, bol.logger, si, obj)
		if err != nil {
			bol.logger.Errorf("Error adding %s:%s %v", objType, srcRef, err)
			return err
		}

	case "workflow_job_template_node":
		son := &serviceofferingnode.ServiceOfferingNode{SourceID: bol.source.ID, TenantID: bol.tenant.ID}
		err = bol.repos.serviceofferingnoderepo.CreateOrUpdate(ctx, bol.logger, son, obj)
		if err == serviceofferingnode.ErrIgnoreTowerObject {
			bol.logger.Info("Ignoring Tower Object")
			return nil
		} else if err != nil {
			bol.logger.Errorf("Error adding %s:%s %v", objType, srcRef, err)
			return err
		}

		bol.workflowNodes = append(bol.workflowNodes, WorkflowNode{SourceRef: son.SourceRef,
			ServiceOfferingSourceRef:     son.ServiceOfferingSourceRef,
			RootServiceOfferingSourceRef: son.RootServiceOfferingSourceRef,
			UnifiedJobType:               son.UnifiedJobType})
	case "credential":
		sc := &servicecredential.ServiceCredential{SourceID: bol.source.ID, TenantID: bol.tenant.ID}
		err = bol.repos.servicecredentialrepo.CreateOrUpdate(ctx, bol.logger, sc, obj)
		if err != nil {
			bol.logger.Errorf("Error adding %s:%s %v", objType, srcRef, err)
			return err
		}

		if sc.ServiceCredentialTypeSourceRef != "" {
			if _, ok := bol.serviceCredentialToCredentialTypeMap[sc.ServiceCredentialTypeSourceRef]; ok {
				bol.serviceCredentialToCredentialTypeMap[sc.ServiceCredentialTypeSourceRef] = append(bol.serviceCredentialToCredentialTypeMap[sc.ServiceCredentialTypeSourceRef], sc.ID)
			} else {
				bol.serviceCredentialToCredentialTypeMap[sc.ServiceCredentialTypeSourceRef] = []int64{sc.ID}
			}
		}
	case "credential_type":
		sct := &servicecredentialtype.ServiceCredentialType{SourceID: bol.source.ID, TenantID: bol.tenant.ID}
		err = bol.repos.servicecredentialtyperepo.CreateOrUpdate(ctx, bol.logger, sct, obj)
		if err != nil {
			bol.logger.Errorf("Error adding %s:%s %v", objType, srcRef, err)
			return err
		}
	case "survey_spec":
		ss := &serviceplan.ServicePlan{SourceID: bol.source.ID, TenantID: bol.tenant.ID}

		err = bol.repos.serviceplanrepo.CreateOrUpdate(ctx, bol.logger, ss, &spec2ddf.Converter{}, obj, r)
		if err != nil {
			bol.logger.Errorf("Error adding %s:%s %v", objType, srcRef, err)
			return err
		}
	}
	err = bol.addIDList(ctx, obj, obj["type"].(string))
	return err
}

// getObjectType based on the file name which is akin to the URL request made to tower
func getObjectType(url string) (string, error) {
	if strings.HasSuffix(url, "survey_spec/page1.json") {
		return "survey_spec", nil
	}
	s := objTypeRe.FindStringSubmatch(url)
	if len(s) < 1 {
		return "", fmt.Errorf("Could not get object type from url %s", url)
	}
	return s[1], nil
}
