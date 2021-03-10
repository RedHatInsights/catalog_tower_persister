package main

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
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/source"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/tenant"
	"github.com/RedHatInsights/catalog_tower_persister/internal/spec2ddf"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// WorkflowNode stores the workflow relations
type WorkflowNode struct {
	SourceRef                    string
	ServiceOfferingSourceRef     string
	RootServiceOfferingSourceRef string
	UnifiedJobType               string
}

// PageContext stores the cumulative information about all pages that we read from
// the tar file
type PageContext struct {
	logger                               *logrus.Entry
	tenant                               *tenant.Tenant
	source                               *source.Source
	inventoryMap                         map[string][]int64
	serviceCredentialToCredentialTypeMap map[string][]int64
	jobTemplateSurvey                    []string
	workflowJobTemplateSurvey            []string
	workflowNodes                        []WorkflowNode
	jobTemplateSourceRefs                []string
	inventorySourceRefs                  []string
	credentialSourceRefs                 []string
	credentialTypeSourceRefs             []string
	workflowNodeSourceRefs               []string
	dbTransaction                        *gorm.DB
	servicecredentialrepo                servicecredential.Repository
	servicecredentialtyperepo            servicecredentialtype.Repository
	serviceinventoryrepo                 serviceinventory.Repository
	serviceplanrepo                      serviceplan.Repository
	serviceofferingrepo                  serviceoffering.Repository
	serviceofferingnoderepo              serviceofferingnode.Repository
}

// pageResponse stores the response from the Ansible Tower API call which
// has been bundled into the tar file.
type pageResponse map[string]interface{}

var surveySpecRe = regexp.MustCompile(`api\/v2\/(job_templates|workflow_job_templates)\/(.*)\/survey_spec/page1.json`)

var objTypeRe = regexp.MustCompile(`\/api\/v2\/(.*)\/`)

// MakePageContext creates a PageContext
func MakePageContext(logger *logrus.Entry, tenant *tenant.Tenant, source *source.Source, dbTransaction *gorm.DB) *PageContext {
	pc := PageContext{
		tenant:        tenant,
		source:        source,
		dbTransaction: dbTransaction,
		logger:        logger}
	pc.inventoryMap = make(map[string][]int64)
	pc.serviceCredentialToCredentialTypeMap = make(map[string][]int64)
	pc.servicecredentialrepo = servicecredential.NewGORMRepository(dbTransaction)
	pc.servicecredentialtyperepo = servicecredentialtype.NewGORMRepository(dbTransaction)
	pc.serviceinventoryrepo = serviceinventory.NewGORMRepository(dbTransaction)
	pc.serviceplanrepo = serviceplan.NewGORMRepository(dbTransaction)
	pc.serviceofferingrepo = serviceoffering.NewGORMRepository(dbTransaction)
	pc.serviceofferingnoderepo = serviceofferingnode.NewGORMRepository(dbTransaction)
	return &pc
}

// Process handles one file at a time from the tar file
func (pc *PageContext) Process(ctx context.Context, url string, r io.Reader) error {
	objectType, err := getObjectType(url)
	if err != nil {
		pc.logger.Errorf("%v", err)
		return err
	}
	// Survey Spec have a different format and are never returned as a list
	// They don't have ID's so we need to handle them separately, also we dont
	// want to read the file twice,
	if objectType == "survey_spec" {
		obj := make(map[string]interface{})
		return pc.addObject(ctx, obj, url, r)
	}
	var pr pageResponse
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	err = decoder.Decode(&pr)
	if err != nil {
		pc.logger.Errorf("Error decoding message body %s %v", url, err)
		return err
	}

	if isListResults(pr) {
		ids := strings.Contains(url, "/id")
		pc.logger.Infof("Received %s objects idObject %v", pr["count"].(json.Number).String(), ids)
		if val, ok := pr["results"]; ok {
			for _, obj := range val.([]interface{}) {
				if ids {
					err = pc.addIDList(ctx, obj.(map[string]interface{}), objectType)
				} else {
					err = pc.addObject(ctx, obj.(map[string]interface{}), url, nil)
					if err != nil {
						pc.logger.Errorf("Error adding object %s", objectType)
						pc.logger.Errorf("Error %v", err)
						return err
					}
				}
			}
		}
	} else {
		err = pc.addObject(ctx, pr, url, nil)
		if err != nil {
			pc.logger.Errorf("Error adding object %s", url)
			pc.logger.Errorf("Error %v", err)
			return err
		}
	}

	return nil
}

// GetStats get counters for objects added/updated/deleted which can be set back to the
// Catalog Inventory API
func (pc *PageContext) GetStats(ctx context.Context) map[string]interface{} {
	stats := map[string]interface{}{
		"credentials":            pc.servicecredentialrepo.Stats(),
		"credential_types":       pc.servicecredentialrepo.Stats(),
		"inventories":            pc.serviceinventoryrepo.Stats(),
		"service_plans":          pc.serviceplanrepo.Stats(),
		"service_offering":       pc.serviceofferingrepo.Stats(),
		"service_offering_nodes": pc.serviceofferingnoderepo.Stats(),
	}
	return stats
}

// LogReports log the objects added/updated/deleted
func (pc *PageContext) LogReports(ctx context.Context) {
	x := pc.servicecredentialrepo.Stats()
	pc.logger.Info(fmt.Sprintf("Credential Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = pc.servicecredentialrepo.Stats()
	pc.logger.Info(fmt.Sprintf("Credential Type Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = pc.serviceinventoryrepo.Stats()
	pc.logger.Info(fmt.Sprintf("Inventory Type Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = pc.serviceplanrepo.Stats()
	pc.logger.Info(fmt.Sprintf("Service Plan Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = pc.serviceofferingrepo.Stats()
	pc.logger.Info(fmt.Sprintf("Service Offering Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = pc.serviceofferingnoderepo.Stats()
	pc.logger.Info(fmt.Sprintf("Service Offering Node Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
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
func (pc *PageContext) addIDList(ctx context.Context, obj map[string]interface{}, objType string) error {
	id := obj["id"].(json.Number).String()
	switch objType {
	case "job_template", "job_templates":
		if !idExists(pc.jobTemplateSourceRefs, id) {
			pc.jobTemplateSourceRefs = append(pc.jobTemplateSourceRefs, id)
		}
	case "workflow_job_template", "workflow_job_templates":
		if !idExists(pc.jobTemplateSourceRefs, id) {
			pc.jobTemplateSourceRefs = append(pc.jobTemplateSourceRefs, id)
		}
	case "inventory", "inventories":
		if !idExists(pc.inventorySourceRefs, id) {
			pc.inventorySourceRefs = append(pc.inventorySourceRefs, id)
		}
	case "credential", "credentials":
		if !idExists(pc.credentialSourceRefs, id) {
			pc.credentialSourceRefs = append(pc.credentialSourceRefs, id)
		}
	case "credential_type", "credential_types":
		if !idExists(pc.credentialTypeSourceRefs, id) {
			pc.credentialTypeSourceRefs = append(pc.credentialTypeSourceRefs, id)
		}
	case "workflow_job_template_node", "workflow_job_template_nodes":
		if !idExists(pc.workflowNodeSourceRefs, id) {
			pc.workflowNodeSourceRefs = append(pc.workflowNodeSourceRefs, id)
		}
	case "survey_spec":
	default:
		pc.logger.Errorf("Invalid Object type found %s", objType)
		return fmt.Errorf("Invalid Object type found %s", objType)
	}
	return nil
}

// addObject add an object into the Database
func (pc *PageContext) addObject(ctx context.Context, obj map[string]interface{}, url string, r io.Reader) error {
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
					pc.logger.Infof("Survey Enabled is incorrectly set, ignoring it for now")
					sp := serviceplan.ServicePlan{Source: *pc.Source, Tenant: *pc.Tenant,
						Tower: base.Tower{SourceRef: s[2]}}
					err := sp.Delete(pc.dbTransaction)
					if err != nil {
						pc.logger.Errorf("Error deleting straggler survey spec %v", err)
					}
					// Ignore the error for now
					return nil
				} */
		} else {
			return errors.New("No type provided")
		}
	}

	pc.logger.Infof("Object Type %s Source Ref %s", obj["type"].(string), obj["id"].(json.Number).String())

	switch objType := obj["type"].(string); objType {
	case "job_template", "workflow_job_template":
		so := &serviceoffering.ServiceOffering{SourceID: pc.source.ID, TenantID: pc.tenant.ID}
		err = pc.serviceofferingrepo.CreateOrUpdate(ctx, pc.logger, so, obj, pc.serviceplanrepo)
		if err != nil {
			pc.logger.Errorf("Error adding job template %s %v", so.SourceRef, err)
			return err
		}

		if so.SurveyEnabled {
			pc.logger.Infof("Survey Enabled for " + so.SourceRef)
			if objType == "job_template" {
				pc.jobTemplateSurvey = append(pc.jobTemplateSurvey, so.SourceRef)
			} else {
				pc.workflowJobTemplateSurvey = append(pc.workflowJobTemplateSurvey, so.SourceRef)
			}
		}

		if so.ServiceInventorySourceRef != "" {
			if _, ok := pc.inventoryMap[so.ServiceInventorySourceRef]; ok {
				pc.inventoryMap[so.ServiceInventorySourceRef] = append(pc.inventoryMap[so.ServiceInventorySourceRef], so.ID)
			} else {
				pc.inventoryMap[so.ServiceInventorySourceRef] = []int64{so.ID}
			}
		}

	case "inventory":
		si := &serviceinventory.ServiceInventory{SourceID: pc.source.ID, TenantID: pc.tenant.ID}
		err = pc.serviceinventoryrepo.CreateOrUpdate(ctx, pc.logger, si, obj)
		if err != nil {
			pc.logger.Errorf("Error adding inventory %s %v", si.SourceRef, err)
			return err
		}

	case "workflow_job_template_node":
		son := &serviceofferingnode.ServiceOfferingNode{SourceID: pc.source.ID, TenantID: pc.tenant.ID}
		err = pc.serviceofferingnoderepo.CreateOrUpdate(ctx, pc.logger, son, obj)
		if err == serviceofferingnode.ErrIgnoreTowerObject {
			pc.logger.Info("Ignoring Tower Object")
			return nil
		} else if err != nil {
			pc.logger.Errorf("Error adding service offering node %s %v", son.SourceRef, err)
			return err
		}

		pc.workflowNodes = append(pc.workflowNodes, WorkflowNode{SourceRef: son.SourceRef,
			ServiceOfferingSourceRef:     son.ServiceOfferingSourceRef,
			RootServiceOfferingSourceRef: son.RootServiceOfferingSourceRef,
			UnifiedJobType:               son.UnifiedJobType})
	case "credential":
		sc := &servicecredential.ServiceCredential{SourceID: pc.source.ID, TenantID: pc.tenant.ID}
		err = pc.servicecredentialrepo.CreateOrUpdate(ctx, pc.logger, sc, obj)
		if err != nil {
			pc.logger.Errorf("Error adding service credential %s", sc.SourceRef)
			return err
		}

		if sc.ServiceCredentialTypeSourceRef != "" {
			if _, ok := pc.serviceCredentialToCredentialTypeMap[sc.ServiceCredentialTypeSourceRef]; ok {
				pc.serviceCredentialToCredentialTypeMap[sc.ServiceCredentialTypeSourceRef] = append(pc.serviceCredentialToCredentialTypeMap[sc.ServiceCredentialTypeSourceRef], sc.ID)
			} else {
				pc.serviceCredentialToCredentialTypeMap[sc.ServiceCredentialTypeSourceRef] = []int64{sc.ID}
			}
		}
	case "credential_type":
		sct := &servicecredentialtype.ServiceCredentialType{SourceID: pc.source.ID, TenantID: pc.tenant.ID}
		err = pc.servicecredentialtyperepo.CreateOrUpdate(ctx, pc.logger, sct, obj)
		if err != nil {
			pc.logger.Errorf("Error adding survey credential type %s", sct.SourceRef)
			return err
		}
	case "survey_spec":
		ss := &serviceplan.ServicePlan{SourceID: pc.source.ID, TenantID: pc.tenant.ID}

		err = pc.serviceplanrepo.CreateOrUpdate(ctx, pc.logger, ss, &spec2ddf.Converter{}, obj, r)
		if err != nil {
			pc.logger.Errorf("Error adding survey spec %s", ss.SourceRef)
			return err
		}
	}
	err = pc.addIDList(ctx, obj, obj["type"].(string))
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
