package serviceofferingnode

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/RedHatInsights/catalog_tower_persister/internal/models/base"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceoffering"

	log "github.com/sirupsen/logrus"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

var IgnoreTowerObject = errors.New("Ignoring non job template or workflow job template nodes")

var inventoriesRe = regexp.MustCompile(`\/api\/v2\/inventories\/(\w)\/`)

type ServiceOfferingNode struct {
	base.Base
	base.Tower
	Name                         string
	Extra                        datatypes.JSON
	TenantID                     int64
	SourceID                     int64
	ServiceInventoryID           sql.NullInt64 `gorm:"default:null"`
	ServiceInventory             serviceinventory.ServiceInventory
	ServiceOfferingID            sql.NullInt64 `gorm:"default:null"`
	ServiceOffering              serviceoffering.ServiceOffering
	RootServiceOfferingID        sql.NullInt64 `gorm:"default:null"`
	RootServiceOffering          serviceoffering.ServiceOffering
	ServiceInventorySourceRef    string `gorm:"-"`
	RootServiceOfferingSourceRef string `gorm:"-"`
	ServiceOfferingSourceRef     string `gorm:"-"`
	UnifiedJobType               string `gorm:"-"`
}

func (son *ServiceOfferingNode) validateAttributes(attrs map[string]interface{}) error {
	requiredAttrs := []string{"id",
		"created",
		"modified",
		"workflow_job_template",
		"unified_job_template",
		"unified_job_type"}
	for _, name := range requiredAttrs {
		if _, ok := attrs[name]; !ok {
			return errors.New("Missing Required Attribute " + name)
		}
	}

	objType := attrs["unified_job_type"].(string)
	if objType != "job" && objType != "workflow_job" {
		return IgnoreTowerObject
	}
	return nil
}

func (son *ServiceOfferingNode) makeObject(attrs map[string]interface{}) error {
	err := son.validateAttributes(attrs)
	if err != nil {
		return err
	}
	extra := make(map[string]interface{})

	extra["unified_job_type"] = attrs["unified_job_type"].(string)
	son.UnifiedJobType = attrs["unified_job_type"].(string)

	valueString, err := json.Marshal(extra)
	if err != nil {
		return err
	}
	son.Extra = datatypes.JSON(valueString)
	son.SourceCreatedAt, err = base.TowerTime(attrs["created"].(string))
	if err != nil {
		return err
	}
	/*son.SourceUpdatedAt, err = towerTime(attrs["modified"].(string))
	if err != nil {
		return err
	} */
	son.SourceRef = attrs["id"].(json.Number).String()
	son.RootServiceOfferingSourceRef = attrs["workflow_job_template"].(json.Number).String()
	son.ServiceOfferingSourceRef = attrs["unified_job_template"].(json.Number).String()

	switch attrs["inventory"].(type) {
	case string:
		s := inventoriesRe.FindStringSubmatch(attrs["inventory"].(string))
		if len(s) > 0 {
			son.ServiceInventorySourceRef = s[1]
		}
	}
	return nil
}

func (son *ServiceOfferingNode) CreateOrUpdate(ctx context.Context, tx *gorm.DB, attrs map[string]interface{}) error {
	err := son.makeObject(attrs)
	if err != nil {
		log.Infof("Error creating a new service offering node object %v", err)
		return err
	}
	var instance ServiceOfferingNode
	err = tx.Where(&ServiceOfferingNode{SourceID: son.SourceID, Tower: base.Tower{SourceRef: son.SourceRef}}).First(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Infof("Creating a new Service Offering Node %s", son.SourceRef)
			if result := tx.Create(son); result.Error != nil {
				return fmt.Errorf("Error creating service offering node : %v" + result.Error.Error())
			}
		} else {
			log.Infof("Error locating Service Offering Node %s %v", son.SourceRef, err)
			return err
		}
	} else {
		log.Infof("Service Offering Node %s exists in DB with ID %d", son.SourceRef, instance.ID)
		son.ID = instance.ID // Get the Existing ID for the object
		instance.RootServiceOfferingSourceRef = son.RootServiceOfferingSourceRef
		instance.ServiceOfferingSourceRef = son.ServiceOfferingSourceRef
		instance.Name = son.Name
		instance.ServiceInventorySourceRef = son.ServiceInventorySourceRef

		log.Infof("Saving Service Offering source_ref %s", son.SourceRef)
		err := tx.Save(&instance).Error
		if err != nil {
			log.Errorf("Error Updating Service Offering Node  source_ref %s", son.SourceRef)
			return err
		}
	}
	return nil
}
