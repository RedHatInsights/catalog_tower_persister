package serviceinventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/base"
	"github.com/sirupsen/logrus"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// Repository interface supports deleted unwanted objects and creating or updating object
type Repository interface {
	DeleteUnwanted(ctx context.Context, logger *logrus.Entry, sc *ServiceInventory, keepSourceRefs []string) error
	CreateOrUpdate(ctx context.Context, logger *logrus.Entry, sc *ServiceInventory, attrs map[string]interface{}) error
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

// ServiceInventory maps an Inventory object in Ansible Tower
type ServiceInventory struct {
	base.Base
	base.Tower
	Name        string
	Description string
	Extra       datatypes.JSON
	TenantID    int64
	SourceID    int64
}

func (si *ServiceInventory) validateAttributes(attrs map[string]interface{}) error {
	requiredAttrs := []string{"kind",
		"type",
		"variables",
		"host_filter",
		"pending_deletion",
		"organization",
		"inventory_sources_with_failures",
		"created",
		"modified",
		"name",
		"id",
		"description"}
	for _, name := range requiredAttrs {
		if _, ok := attrs[name]; !ok {
			return errors.New("Missing Required Attribute " + name)
		}
	}
	return nil
}

func (si *ServiceInventory) makeObject(attrs map[string]interface{}) error {
	err := si.validateAttributes(attrs)
	if err != nil {
		return err
	}
	extra := make(map[string]interface{})
	extra["kind"] = attrs["kind"].(string)
	extra["type"] = attrs["type"].(string)
	extra["variables"] = attrs["variables"].(string)

	if reflect.TypeOf(attrs["host_filter"]) == reflect.TypeOf("string") {
		extra["host_filter"] = attrs["host_filter"].(string)
	}

	extra["pending_deletion"] = attrs["pending_deletion"].(bool)
	orgID, err := attrs["organization"].(json.Number).Int64()
	if err != nil {
		return err
	}
	extra["organization_id"] = orgID

	failures, err := attrs["inventory_sources_with_failures"].(json.Number).Int64()
	if err != nil {
		return err
	}
	extra["inventory_sources_with_failures"] = failures

	valueString, err := json.Marshal(extra)
	if err != nil {
		return err
	}
	si.Extra = datatypes.JSON([]byte(valueString))
	si.SourceCreatedAt, err = base.TowerTime(attrs["created"].(string))
	if err != nil {
		return err
	}
	/*si.SourceUpdatedAt, err = base.TowerTime(attrs["modified"].(string))
	if err != nil {
		return err
	}*/
	si.Description = attrs["description"].(string)
	si.Name = attrs["name"].(string)
	si.SourceRef = attrs["id"].(json.Number).String()
	return nil
}

func (gr *gormRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, si *ServiceInventory, attrs map[string]interface{}) error {
	err := si.makeObject(attrs)
	if err != nil {
		logger.Infof("Error creating a new service inventory object %v", err)
		return err
	}
	var instance ServiceInventory
	err = gr.db.Where(&ServiceInventory{SourceID: si.SourceID, Tower: base.Tower{SourceRef: si.SourceRef}}).First(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			logger.Infof("Creating a new Inventory %s", si.SourceRef)
			if result := gr.db.Create(si); result.Error != nil {
				return fmt.Errorf("Error creating inventory : %v", result.Error.Error())
			}
			gr.creates++
		} else {
			logger.Errorf("Error locating Inventory %s %v", si.SourceRef, err)
			return err
		}
	} else {
		logger.Infof("Inventory %s exists in DB with ID %d", si.SourceRef, instance.ID)
		si.ID = instance.ID // Get the Existing ID for the object

		logger.Infof("Updating Inventory %s exists in DB with ID %d", si.SourceRef, instance.ID)
		instance.Name = si.Name
		instance.Description = si.Description
		instance.Extra = si.Extra
		logger.Infof("Saving Inventory source ref %s", si.SourceRef)
		err := gr.db.Save(&instance).Error
		if err != nil {
			logger.Errorf("Error Updating Service Inventory %s %v", si.SourceRef, err)
			return err
		}
		gr.updates++
	}
	return nil
}

// DeleteUnwanted deletes any objects not listed in the keepSourceRefs
// This is used to delete ServiceInventory that exist in our database but have been
// deleted from the Ansible Tower
func (gr *gormRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, si *ServiceInventory, keepSourceRefs []string) error {
	results, err := si.getDeleteIDs(ctx, logger, gr.db, keepSourceRefs)
	if err != nil {
		logger.Errorf("Error getting Delete IDs for service inventories %v", err)
		return err
	}
	for _, res := range results {
		logger.Infof("Attempting to delete ServiceInventory with ID %d Source ref %s", res.ID, res.SourceRef)
		result := gr.db.Delete(&ServiceInventory{SourceID: si.SourceID, TenantID: si.TenantID, Tower: base.Tower{SourceRef: res.SourceRef}}, res.ID)
		if result.Error != nil {
			logger.Errorf("Error deleting Service Inventory %d %s %v", res.ID, res.SourceRef, result.Error)
			return result.Error
		}
		gr.deletes++
	}
	return nil
}

func (si *ServiceInventory) getDeleteIDs(ctx context.Context, logger *logrus.Entry, tx *gorm.DB, keepSourceRefs []string) ([]base.ResultIDRef, error) {
	var result []base.ResultIDRef
	var deleteResultIDRef []base.ResultIDRef
	sort.Strings(keepSourceRefs)
	length := len(keepSourceRefs)
	if err := tx.Table("service_inventories").Select("id, source_ref").Where("source_id = ? AND archived_at IS NULL", si.SourceID).Scan(&result).Error; err != nil {
		logger.Errorf("Error fetching ServiceInventory %v", err)
		return deleteResultIDRef, err
	}
	for _, res := range result {
		if !base.SourceRefExists(res.SourceRef, keepSourceRefs, length) {
			deleteResultIDRef = append(deleteResultIDRef, res)
		}
	}
	return deleteResultIDRef, nil
}
