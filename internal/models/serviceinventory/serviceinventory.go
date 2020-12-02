package serviceinventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/mkanoor/catalog_tower_persister/internal/models/base"
	"github.com/mkanoor/catalog_tower_persister/internal/models/source"
	"github.com/mkanoor/catalog_tower_persister/internal/models/tenant"

	log "github.com/sirupsen/logrus"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type ServiceInventory struct {
	base.Base
	base.Tower
	Name        string
	Description string
	//Extra JSONB `sql:"type:jsonb"`
	Extra    datatypes.JSON
	TenantID int64
	SourceID int64
	Tenant   tenant.Tenant
	Source   source.Source
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
	org_id, err := attrs["organization"].(json.Number).Int64()
	if err != nil {
		return err
	}
	extra["organization_id"] = org_id
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

func (si *ServiceInventory) CreateOrUpdate(ctx context.Context, tx *gorm.DB, attrs map[string]interface{}) error {
	err := si.makeObject(attrs)
	if err != nil {
		log.Infof("Error creating a new service inventory object %v", err)
		return err
	}
	var instance ServiceInventory
	err = tx.Where(&ServiceInventory{SourceID: si.Source.ID, Tower: base.Tower{SourceRef: si.SourceRef}}).First(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Infof("Creating a new Inventory %s", si.SourceRef)
			if result := tx.Create(si); result.Error != nil {
				return fmt.Errorf("Error creating inventory : %v" + result.Error.Error())
			}
		} else {
			log.Errorf("Error locating Inventory %s %v", si.SourceRef, err)
			return err
		}
	} else {
		log.Infof("Inventory %s exists in DB with ID %d", si.SourceRef, instance.ID)
		si.ID = instance.ID // Get the Existing ID for the object

		log.Infof("Updating Inventory %s exists in DB with ID %d", si.SourceRef, instance.ID)
		instance.Name = si.Name
		instance.Description = si.Description
		instance.Extra = si.Extra
		log.Infof("Saving Inventory source ref %s", si.SourceRef)
		err := tx.Save(&instance).Error
		if err != nil {
			log.Error("Error Updating Service Inventory %s %v", si.SourceRef, err)
			return err
		}
	}
	return nil
}

func (si *ServiceInventory) DeleteOldServiceInventories(ctx context.Context, tx *gorm.DB, sourceRefs []string) error {
	results, err := si.getDeleteIDs(tx, sourceRefs)
	if err != nil {
		log.Errorf("Error getting Delete IDs for service inventories %v", err)
		return err
	}
	for _, res := range results {
		log.Infof("Attempting to delete ServiceInventory with ID %d Source ref %s", res.ID, res.SourceRef)
		result := tx.Delete(&ServiceInventory{SourceID: si.Source.ID, TenantID: si.Tenant.ID, Tower: base.Tower{SourceRef: res.SourceRef}}, res.ID)
		if result.Error != nil {
			log.Errorf("Error deleting Service Inventory %d %s %v", res.ID, res.SourceRef, result.Error)
			return result.Error
		}
	}
	return nil
}

func (si *ServiceInventory) getDeleteIDs(tx *gorm.DB, sourceRefs []string) ([]base.ResultIDRef, error) {
	var result []base.ResultIDRef
	var deleteResultIDRef []base.ResultIDRef
	sort.Strings(sourceRefs)
	length := len(sourceRefs)
	if err := tx.Model(&ServiceInventory{SourceID: si.Source.ID}).Find(&result).Error; err != nil {
		log.Errorf("Error fetching ServiceInventory %v", err)
		return deleteResultIDRef, err
	}
	for _, res := range result {
		if !base.SourceRefExists(res.SourceRef, sourceRefs, length) {
			deleteResultIDRef = append(deleteResultIDRef, res)
		}
	}
	return deleteResultIDRef, nil
}
