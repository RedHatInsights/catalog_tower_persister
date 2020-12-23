package serviceoffering

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/base"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceofferingicon"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceplan"

	log "github.com/sirupsen/logrus"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

var inventoriesRe = regexp.MustCompile(`\/api\/v2\/inventories\/(\w)\/`)

type ServiceOffering struct {
	base.Base
	base.Tower
	Name                      string
	Description               string
	Extra                     datatypes.JSON
	TenantID                  int64
	SourceID                  int64
	ServiceInventoryID        sql.NullInt64 `gorm:"default:null"`
	ServiceInventory          serviceinventory.ServiceInventory
	ServiceOfferingIconID     sql.NullInt64 `gorm:"default:null"`
	ServiceOfferingIcon       serviceofferingicon.ServiceOfferingIcon
	ServiceInventorySourceRef string `gorm:"-"`
	SurveyEnabled             bool   `gorm:"-"`
}

func (so *ServiceOffering) validateAttributes(attrs map[string]interface{}) error {
	requiredAttrs := []string{"name",
		"ask_inventory_on_launch",
		"ask_variables_on_launch",
		"survey_enabled",
		"type",
		"created",
		"modified",
		"id",
		"description"}
	for _, name := range requiredAttrs {
		if _, ok := attrs[name]; !ok {
			return errors.New("Missing Required Attribute " + name)
		}
	}
	return nil
}

func (so *ServiceOffering) makeObject(attrs map[string]interface{}) error {
	err := so.validateAttributes(attrs)
	if err != nil {
		return err
	}
	extra := make(map[string]interface{})

	optionals := []string{"ask_credential_on_launch",
		"ask_tags_on_launch",
		"ask_diff_mode_on_launch",
		"ask_skip_tags_on_launch",
		"ask_job_type_on_launch",
		"ask_limit_on_launch",
		"ask_verbosity_on_launch"}

	for _, s := range optionals {
		if _, ok := attrs[s]; ok {
			extra[s] = attrs[s].(bool)
		}
	}

	extra["ask_inventory_on_launch"] = attrs["ask_inventory_on_launch"].(bool)
	extra["survey_enabled"] = attrs["survey_enabled"].(bool)
	so.SurveyEnabled = attrs["survey_enabled"].(bool)
	extra["ask_variables_on_launch"] = attrs["ask_variables_on_launch"].(bool)

	extra["type"] = attrs["type"].(string)

	valueString, err := json.Marshal(extra)
	if err != nil {
		return err
	}
	so.Extra = datatypes.JSON(valueString)
	so.SourceCreatedAt, err = base.TowerTime(attrs["created"].(string))
	if err != nil {
		return err
	}
	/*so.SourceUpdatedAt, err = base.TowerTime(attrs["modified"].(string))
	if err != nil {
		return err
	}*/
	so.Description = attrs["description"].(string)
	so.Name = attrs["name"].(string)
	so.SourceRef = attrs["id"].(json.Number).String()

	switch attrs["inventory"].(type) {
	case string:
		s := inventoriesRe.FindStringSubmatch(attrs["inventory"].(string))
		if len(s) > 0 {
			so.ServiceInventorySourceRef = s[1]
		}
	}
	return nil
}

func (dbso *ServiceOffering) equal(other *ServiceOffering) bool {
	return dbso.Name == other.Name &&
		dbso.Description == other.Description &&
		dbso.ServiceInventory.SourceRef == other.SourceRef &&
		dbso.SurveyEnabled == other.SurveyEnabled

}

func (so *ServiceOffering) CreateOrUpdate(ctx context.Context, tx *gorm.DB, attrs map[string]interface{}) error {
	err := so.makeObject(attrs)
	if err != nil {
		log.Infof("Error creating a new service offering object %v", err)
		return err
	}
	var instance ServiceOffering
	err = tx.Preload("ServiceInventory").Where(&ServiceOffering{SourceID: so.SourceID, Tower: base.Tower{SourceRef: so.SourceRef}}).First(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Infof("Creating a new Job Template %s", so.SourceRef)
			if result := tx.Create(so); result.Error != nil {
				return fmt.Errorf("Error creating job template: %v" + result.Error.Error())
			}
		} else {
			log.Errorf("Error locating job template  %s %v", so.SourceRef, err)
			return err
		}
	} else {
		log.Infof("Job Template %s exists in DB with ID %d", so.SourceRef, instance.ID)
		so.ID = instance.ID // Get the Existing ID for the object
		var resp map[string]interface{}
		err := json.Unmarshal(instance.Extra, &resp)
		if err != nil {
			log.Errorf("Error parsing extra in service offering source ref %s", so.SourceRef)
			return err
		}

		instance.SurveyEnabled = resp["survey_enabled"].(bool)

		if !instance.equal(so) {
			log.Infof("Updating Job Template %s exists in DB with ID %d", so.SourceRef, instance.ID)
			instance.Name = so.Name
			instance.Description = so.Description
			instance.ServiceInventory = serviceinventory.ServiceInventory{}
			if !so.SurveyEnabled && instance.SurveyEnabled {
				log.Infof("Deleting Service Plan for Job Template %s", so.SourceRef)
				err := instance.DeleteServicePlan(tx)
				if err != nil {
					log.Error("Error Deleting Old Service Plan")
					return err
				}
			}
			log.Infof("Saving Job Template source ref %s", so.SourceRef)
			err := tx.Save(&instance).Error
			if err != nil {
				log.Errorf("Error Updating Service Offering %s", so.SourceRef)
				return err
			}
		} else {
			log.Infof("Job Template %s is in sync with Tower", so.SourceRef)
		}
	}
	return nil
}

// AfterDelete hook defined for cascade delete
func (so *ServiceOffering) AfterDelete(tx *gorm.DB) error {
	return tx.Model(&serviceplan.ServicePlan{}).Where("source_ref = ? AND tenant_id = ? AND source_id = ?", so.SourceRef, so.TenantID, so.SourceID).Delete(&serviceplan.ServicePlan{}).Error
}

func (so *ServiceOffering) DeleteServicePlan(tx *gorm.DB) error {
	return tx.Model(&serviceplan.ServicePlan{}).Where("source_ref = ? AND source_id = ?", so.SourceRef, so.SourceID).Delete(&serviceplan.ServicePlan{}).Error
}

func (so *ServiceOffering) DeleteOldServiceOfferings(ctx context.Context, tx *gorm.DB, sourceRefs []string) error {
	results, err := so.getDeleteIDs(tx, sourceRefs)
	if err != nil {
		log.Errorf("Error getting Delete IDs for service offerings %v", err)
		return err
	}
	for _, res := range results {
		log.Infof("Attempting to delete ServiceOffering with ID %d Source ref %s", res.ID, res.SourceRef)
		result := tx.Delete(&ServiceOffering{SourceID: so.SourceID, TenantID: so.TenantID, Tower: base.Tower{SourceRef: res.SourceRef}}, res.ID)
		if result.Error != nil {
			log.Errorf("Error deleting Service Offering %d %s %v", res.ID, res.SourceRef, result.Error)
			return result.Error
		}
	}
	return nil
}

func (so *ServiceOffering) getDeleteIDs(tx *gorm.DB, sourceRefs []string) ([]base.ResultIDRef, error) {
	var result []base.ResultIDRef
	var deleteResultIDRef []base.ResultIDRef
	sort.Strings(sourceRefs)
	length := len(sourceRefs)
	if err := tx.Model(&ServiceOffering{SourceID: so.SourceID}).Find(&result).Error; err != nil {
		log.Errorf("Error fetching ServiceOffering %v", err)
		return deleteResultIDRef, err
	}
	for _, res := range result {
		if !base.SourceRefExists(res.SourceRef, sourceRefs, length) {
			deleteResultIDRef = append(deleteResultIDRef, res)
		}
	}
	return deleteResultIDRef, nil
}
