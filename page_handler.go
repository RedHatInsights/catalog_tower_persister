package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/mkanoor/catalog_tower_persister/internal/logger"
	"github.com/mkanoor/catalog_tower_persister/internal/models/servicecredential"
	"github.com/mkanoor/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/mkanoor/catalog_tower_persister/internal/models/serviceinstance"
	"github.com/mkanoor/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/mkanoor/catalog_tower_persister/internal/models/serviceoffering"
	"github.com/mkanoor/catalog_tower_persister/internal/models/serviceofferingnode"
	"github.com/mkanoor/catalog_tower_persister/internal/models/serviceplan"
	"github.com/mkanoor/catalog_tower_persister/internal/models/source"
	"github.com/mkanoor/catalog_tower_persister/internal/models/tenant"
	"gorm.io/gorm"
)

type WorkflowNode struct {
	SourceRef                    string
	ServiceOfferingSourceRef     string
	RootServiceOfferingSourceRef string
	UnifiedJobType               string
}

type PageContext struct {
	glog                                 logger.Logger
	Tenant                               *tenant.Tenant
	Source                               *source.Source
	InventoryMap                         map[string][]int64
	ServiceCredentialToCredentialTypeMap map[string][]int64
	JobTemplateSurvey                    []string
	WorkflowJobTemplateSurvey            []string
	WorkflowNodes                        []WorkflowNode
	jobTemplateSourceRefs                []string
	inventorySourceRefs                  []string
	credentialSourceRefs                 []string
	credentialTypeSourceRefs             []string
	workflowNodeSourceRefs               []string
	dbTransaction                        *gorm.DB
}
type PageResponse map[string]interface{}

var surveySpecRe = regexp.MustCompile(`api\/v2\/(job_templates|workflow_job_templates)\/(.*)\/survey_spec/page1.json`)

var objTypeRe = regexp.MustCompile(`\/api\/v2\/(.*)\/`)

func MakePageContext(logger logger.Logger, tenant *tenant.Tenant, source *source.Source, dbTransaction *gorm.DB) *PageContext {
	pc := PageContext{
		Tenant:        tenant,
		Source:        source,
		dbTransaction: dbTransaction,
		glog:          logger}
	pc.InventoryMap = make(map[string][]int64)
	pc.ServiceCredentialToCredentialTypeMap = make(map[string][]int64)
	return &pc
}

func (pc *PageContext) Process(ctx context.Context, url string, r io.Reader) error {
	objectType, err := getObjectType(url)
	if err != nil {
		pc.glog.Errorf("%v", err)
		return err
	}
	// Survey Spec have a different format and are never returned as a list
	// They don't have ID's so we need to handle them separately, also we dont
	// want to read the file twice,
	if objectType == "survey_spec" {
		obj := make(map[string]interface{})
		return pc.addObject(ctx, obj, url, r)
	}
	var pr PageResponse
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	err = decoder.Decode(&pr)
	if err != nil {
		pc.glog.Errorf("Error decoding message body %s %v", url, err)
		return err
	}

	if isListResults(pr) {
		ids := strings.Contains(url, "/id")
		pc.glog.Infof("Received %s objects idObject %v", pr["count"].(json.Number).String(), ids)
		if val, ok := pr["results"]; ok {
			for _, obj := range val.([]interface{}) {
				if ids {
					err = pc.addIDList(ctx, obj.(map[string]interface{}), objectType)
				} else {
					err = pc.addObject(ctx, obj.(map[string]interface{}), url, nil)
					if err != nil {
						pc.glog.Errorf("Error adding object %s", objectType)
						pc.glog.Errorf("Error %v", err)
						return err
					}
				}
			}
		}
	} else {
		err = pc.addObject(ctx, pr, url, nil)
		if err != nil {
			pc.glog.Errorf("Error adding object %s", url)
			pc.glog.Errorf("Error %v", err)
			return err
		}
	}

	return nil
}

func isListResults(pr PageResponse) bool {
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

func idExists(ids []string, id string) bool {
	for _, str := range ids {
		if str == id {
			return true
		}
	}
	return false
}

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
		pc.glog.Errorf("Invalid Object type found %s", objType)
		return fmt.Errorf("Invalid Object type found %s", objType)
	}
	return nil
}

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
					pc.glog.Infof("Survey Enabled is incorrectly set, ignoring it for now")
					sp := serviceplan.ServicePlan{Source: *pc.Source, Tenant: *pc.Tenant,
						Tower: base.Tower{SourceRef: s[2]}}
					err := sp.Delete(pc.dbTransaction)
					if err != nil {
						pc.glog.Errorf("Error deleting straggler survey spec %v", err)
					}
					// Ignore the error for now
					return nil
				} */
		} else {
			return errors.New("No type provided")
		}
	}

	pc.glog.Infof("Object Type %s Source Ref %s", obj["type"].(string), obj["id"].(json.Number).String())

	switch objType := obj["type"].(string); objType {
	case "job_template", "workflow_job_template":
		so := &serviceoffering.ServiceOffering{Source: *pc.Source, Tenant: *pc.Tenant}
		err = so.CreateOrUpdate(ctx, pc.dbTransaction, obj)
		if err != nil {
			pc.glog.Errorf("Error adding job template %s %v", so.SourceRef, err)
			return err
		}

		if so.SurveyEnabled {
			pc.glog.Infof("Survey Enabled for " + so.SourceRef)
			if objType == "job_template" {
				pc.JobTemplateSurvey = append(pc.JobTemplateSurvey, so.SourceRef)
			} else {
				pc.WorkflowJobTemplateSurvey = append(pc.WorkflowJobTemplateSurvey, so.SourceRef)
			}
		}

		if so.ServiceInventorySourceRef != "" {
			if _, ok := pc.InventoryMap[so.ServiceInventorySourceRef]; ok {
				pc.InventoryMap[so.ServiceInventorySourceRef] = append(pc.InventoryMap[so.ServiceInventorySourceRef], so.ID)
			} else {
				pc.InventoryMap[so.ServiceInventorySourceRef] = []int64{so.ID}
			}
		}

	case "inventory":
		si := &serviceinventory.ServiceInventory{Source: *pc.Source, Tenant: *pc.Tenant}
		err = si.CreateOrUpdate(ctx, pc.dbTransaction, obj)
		if err != nil {
			pc.glog.Errorf("Error adding inventory %s %v", si.SourceRef, err)
			return err
		}

	case "workflow_job_template_node":
		son := &serviceofferingnode.ServiceOfferingNode{Source: *pc.Source, Tenant: *pc.Tenant}
		err = son.CreateOrUpdate(ctx, pc.dbTransaction, obj)
		if err == serviceofferingnode.IgnoreTowerObject {
			pc.glog.Info("Ignoring Tower Object")
			return nil
		} else if err != nil {
			pc.glog.Errorf("Error adding service offering node %s %v", son.SourceRef, err)
			return err
		}

		pc.WorkflowNodes = append(pc.WorkflowNodes, WorkflowNode{SourceRef: son.SourceRef,
			ServiceOfferingSourceRef:     son.ServiceOfferingSourceRef,
			RootServiceOfferingSourceRef: son.RootServiceOfferingSourceRef,
			UnifiedJobType:               son.UnifiedJobType})
	case "credential":
		sc := &servicecredential.ServiceCredential{Source: *pc.Source, Tenant: *pc.Tenant}
		err = sc.CreateOrUpdate(ctx, pc.dbTransaction, obj)
		if err != nil {
			pc.glog.Errorf("Error adding service credential %s", sc.SourceRef)
			return err
		}

		if sc.ServiceCredentialTypeSourceRef != "" {
			if _, ok := pc.ServiceCredentialToCredentialTypeMap[sc.ServiceCredentialTypeSourceRef]; ok {
				pc.ServiceCredentialToCredentialTypeMap[sc.ServiceCredentialTypeSourceRef] = append(pc.ServiceCredentialToCredentialTypeMap[sc.ServiceCredentialTypeSourceRef], sc.ID)
			} else {
				pc.ServiceCredentialToCredentialTypeMap[sc.ServiceCredentialTypeSourceRef] = []int64{sc.ID}
			}
		}
	case "credential_type":
		sct := &servicecredentialtype.ServiceCredentialType{Source: *pc.Source, Tenant: *pc.Tenant}
		err = sct.CreateOrUpdate(ctx, pc.dbTransaction, obj)
		if err != nil {
			pc.glog.Errorf("Error adding survey credential type %s", sct.SourceRef)
			return err
		}
	case "job":
		si := &serviceinstance.ServiceInstance{Source: *pc.Source, Tenant: *pc.Tenant}
		err = si.CreateOrUpdate(ctx, pc.dbTransaction, obj)
		if err != nil {
			pc.glog.Errorf("Error adding service instance type %s", si.SourceRef)
			return err
		}
	case "survey_spec":
		ss := &serviceplan.ServicePlan{Source: *pc.Source, Tenant: *pc.Tenant}

		err = ss.CreateOrUpdate(ctx, pc.dbTransaction, obj, r)
		if err != nil {
			pc.glog.Errorf("Error adding survey spec %s", ss.SourceRef)
			return err
		}
	}
	err = pc.addIDList(ctx, obj, obj["type"].(string))
	return err
}

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
