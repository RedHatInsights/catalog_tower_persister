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
	"github.com/sirupsen/logrus"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

var inventoriesRe = regexp.MustCompile(`\/api\/v2\/inventories\/(\w)\/`)

// ServiceOffering maps a Job Template or a Workflow from Ansible Tower
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

// Repository interface supports deleted unwanted objects and creating or updating object
type Repository interface {
	DeleteUnwanted(ctx context.Context, logger *logrus.Entry, so *ServiceOffering, keepSourceRefs []string, spr serviceplan.Repository) error
	CreateOrUpdate(ctx context.Context, logger *logrus.Entry, so *ServiceOffering, attrs map[string]interface{}, spr serviceplan.Repository) error
	Stats() map[string]int
}

// gormRepository struct stores the DB handle and counters
type gormRepository struct {
	db      *gorm.DB
	updates int
	creates int
	deletes int
}

// NewGORMRepository creates a new repository object
func NewGORMRepository(db *gorm.DB) Repository {
	return &gormRepository{db: db}
}

// Stats returns a map with the number of adds/updates/deletes
func (gr *gormRepository) Stats() map[string]int {
	return map[string]int{"adds": gr.creates, "updates": gr.updates, "deletes": gr.deletes}
}

func (gr *gormRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, so *ServiceOffering, attrs map[string]interface{}, spr serviceplan.Repository) error {
	err := so.makeObject(attrs)
	if err != nil {
		logger.Infof("Error creating a new service offering object %v", err)
		return err
	}
	instance, err := so.getInstance(ctx, logger, gr.db)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			logger.Infof("Creating a new Job Template %s", so.SourceRef)
			if result := gr.db.Create(so); result.Error != nil {
				return fmt.Errorf("Error creating job template: %v", result.Error.Error())
			}
		} else {
			logger.Errorf("Error locating job template  %s %v", so.SourceRef, err)
			return err
		}
		gr.creates++
	} else {
		logger.Infof("Job Template %s exists in DB with ID %d", so.SourceRef, instance.ID)
		so.ID = instance.ID // Get the Existing ID for the object

		if !instance.equal(so) {
			logger.Infof("Updating Job Template %s exists in DB with ID %d", so.SourceRef, instance.ID)
			instance.Name = so.Name
			instance.Description = so.Description
			instance.ServiceInventory = serviceinventory.ServiceInventory{}
			if !so.SurveyEnabled && instance.SurveyEnabled {
				logger.Infof("Deleting Service Plan for Job Template %s", so.SourceRef)
				// Delete the Service Plan if any that is connected to this ServiceOffering
				err := so.deleteServicePlan(ctx, logger, spr)
				if err != nil {
					logger.Errorf("Error deleting Service Plan for Service Offering %d %s %v", so.ID, so.SourceRef, err)
					return err
				}
			}
			logger.Infof("Saving Job Template source ref %s", so.SourceRef)
			err := gr.db.Save(&instance).Error
			if err != nil {
				logger.Errorf("Error Updating Service Offering %s", so.SourceRef)
				return err
			}
			gr.updates++
		} else {
			logger.Infof("Job Template %s is in sync with Tower", so.SourceRef)
		}
	}
	return nil
}

func (gr *gormRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, so *ServiceOffering, keepSourceRefs []string, spr serviceplan.Repository) error {
	results, err := so.getDeleteIDs(ctx, logger, gr.db, keepSourceRefs)
	if err != nil {
		logger.Errorf("Error getting Delete IDs for service offerings %v", err)
		return err
	}
	for _, res := range results {
		logger.Infof("Attempting to delete ServiceOffering with ID %d Source ref %s", res.ID, res.SourceRef)

		dso := &ServiceOffering{SourceID: so.SourceID, TenantID: so.TenantID, Tower: base.Tower{SourceRef: res.SourceRef}}
		instance, err := dso.getInstance(ctx, logger, gr.db)
		if err != nil {
			logger.Errorf("Error fetching service offering instance %v", err)
			return err
		}
		result := gr.db.Delete(dso, res.ID)
		if result.Error != nil {
			logger.Errorf("Error deleting Service Offering %d %s %v", res.ID, res.SourceRef, result.Error)
			return result.Error
		}
		gr.deletes++
		// Delete the Service Plan if any that is connected to this ServiceOffering
		if instance.SurveyEnabled {
			err = dso.deleteServicePlan(ctx, logger, spr)
			if err != nil {
				logger.Errorf("Error deleting Service Plan for Service Offering %d %s %v", res.ID, res.SourceRef, err)
				return err
			}
		}
	}
	return nil
}

func (so *ServiceOffering) deleteServicePlan(ctx context.Context, logger *logrus.Entry, spr serviceplan.Repository) error {
	sp := serviceplan.ServicePlan{SourceID: so.SourceID, TenantID: so.TenantID, Tower: base.Tower{SourceRef: so.SourceRef}}
	return spr.Delete(ctx, logger, &sp)
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

func (so *ServiceOffering) equal(other *ServiceOffering) bool {
	return so.Name == other.Name &&
		so.Description == other.Description &&
		so.ServiceInventory.SourceRef == other.SourceRef &&
		so.SurveyEnabled == other.SurveyEnabled

}

func (so *ServiceOffering) getDeleteIDs(ctx context.Context, logger *logrus.Entry, tx *gorm.DB, keepSourceRefs []string) ([]base.ResultIDRef, error) {
	var result []base.ResultIDRef
	var deleteResultIDRef []base.ResultIDRef
	sort.Strings(keepSourceRefs)
	length := len(keepSourceRefs)
	if err := tx.Table("service_offerings").Select("id, source_ref").Where("source_id = ? AND archived_at IS NULL", so.SourceID).Scan(&result).Error; err != nil {
		logger.Errorf("Error fetching ServiceOffering %v", err)
		return deleteResultIDRef, err
	}
	for _, res := range result {
		if !base.SourceRefExists(res.SourceRef, keepSourceRefs, length) {
			deleteResultIDRef = append(deleteResultIDRef, res)
		}
	}
	return deleteResultIDRef, nil
}

func (so *ServiceOffering) getInstance(ctx context.Context, logger *logrus.Entry, db *gorm.DB) (*ServiceOffering, error) {
	var instance ServiceOffering
	err := db.Preload("ServiceInventory").Where(&ServiceOffering{SourceID: so.SourceID, Tower: base.Tower{SourceRef: so.SourceRef}}).First(&instance).Error
	if err != nil {
		return nil, err
	}
	var resp map[string]interface{}
	err = json.Unmarshal(instance.Extra, &resp)
	if err != nil {
		logger.Errorf("Error parsing extra in service offering source ref %s", so.SourceRef)
		return nil, err
	}
	instance.SurveyEnabled = resp["survey_enabled"].(bool)
	return &instance, nil
}
