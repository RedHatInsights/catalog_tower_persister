package serviceinstance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/base"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceplan"

	log "github.com/sirupsen/logrus"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type ServiceInstance struct {
	base.Base
	base.Tower
	Name               string
	Extra              datatypes.JSON
	TenantID           int64
	SourceID           int64
	ExternalURL        string
	ServiceInventoryID sql.NullInt64 `gorm:"default:null"`
	ServiceInventory   serviceinventory.ServiceInventory
	ServicePlanID      sql.NullInt64 `gorm:"default:null"`
	ServicePlan        serviceplan.ServicePlan
}

func (si *ServiceInstance) validateAttributes(attrs map[string]interface{}) error {
	requiredAttrs := []string{"status",
		"started",
		"finished",
		"artifacts",
		"extra_vars",
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

func (si *ServiceInstance) makeObject(attrs map[string]interface{}) error {
	err := si.validateAttributes(attrs)
	if err != nil {
		return err
	}
	extra := make(map[string]interface{})
	extra["status"] = attrs["status"].(string)
	extra["started"] = attrs["started"].(string)
	extra["finished"] = attrs["finished"].(string)
	extra["artifacts"] = attrs["artifacts"].(map[string]interface{})
	extra["extra_vars"] = attrs["extra_vars"].(string)
	valueString, err := json.Marshal(extra)
	if err != nil {
		return err
	}
	si.Extra = datatypes.JSON(valueString)

	si.SourceCreatedAt, err = base.TowerTime(attrs["created"].(string))
	if err != nil {
		return err
	}
	/*si.SourceUpdatedAt, err = towerTime(attrs["modified"].(string))
	if err != nil {
		return err
	}*/
	si.Name = attrs["name"].(string)
	si.SourceRef = attrs["id"].(json.Number).String()
	return nil
}

func (si *ServiceInstance) CreateOrUpdate(ctx context.Context, tx *gorm.DB, attrs map[string]interface{}) error {
	err := si.makeObject(attrs)
	if err != nil {
		log.Infof("Error creating a new service instance object %v", err)
		return err
	}
	var instance ServiceInstance
	err = tx.Where(&ServiceInstance{SourceID: si.SourceID, Tower: base.Tower{SourceRef: si.SourceRef}}).First(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Infof("Creating a new Service Instance %s", si.SourceRef)
			if result := tx.Create(si); result.Error != nil {
				return fmt.Errorf("Error creating service instance : %v" + result.Error.Error())
			}
		} else {
			log.Infof("Error locating Service Instance %s %v", si.SourceRef, err)
			return err
		}
	} else {
		log.Infof("Service Instance %s exists in DB with ID %d", si.SourceRef, instance.ID)
		si.ID = instance.ID // Get the Existing ID for the object
		instance.Name = si.Name
		instance.Extra = si.Extra

		log.Infof("Saving Service Instance source_ref %s", si.SourceRef)
		err := tx.Save(&instance).Error
		if err != nil {
			log.Errorf("Error Updating Service Instance source_ref %s", si.SourceRef)
			return err
		}
	}
	return nil
}
