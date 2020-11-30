package serviceplan

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mkanoor/catalog_tower_persister/internal/models/base"
	"github.com/mkanoor/catalog_tower_persister/internal/models/source"
	"github.com/mkanoor/catalog_tower_persister/internal/models/tenant"
	"github.com/mkanoor/catalog_tower_persister/internal/spec2ddf"

	log "github.com/sirupsen/logrus"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type ServicePlan struct {
	base.Base
	base.Tower
	Name               string
	Description        string
	Extra              datatypes.JSON
	CreateJsonSchema   datatypes.JSON
	UpdateJsonSchema   datatypes.JSON
	TenantID           int64
	SourceID           int64
	Tenant             tenant.Tenant
	Source             source.Source
	ServiceInventoryID sql.NullInt64 `gorm:"default:null"`
	//ServiceInventory   serviceinventory.ServiceInventory
	ServiceOfferingID sql.NullInt64 `gorm:"default:null"`
	//ServiceOffering    serviceoffering.ServiceOffering
}

func (sp *ServicePlan) validateAttributes(attrs map[string]interface{}) error {
	requiredAttrs := []string{"name",
		"description"}
	for _, name := range requiredAttrs {
		if _, ok := attrs[name]; !ok {
			return errors.New("Missing Required Attribute " + name)
		}
	}
	return nil
}

func (sp *ServicePlan) makeObject(attrs map[string]interface{}, r io.Reader) error {
	err := sp.validateAttributes(attrs)
	if err != nil {
		return err
	}
	spec, err := spec2ddf.Convert(r)
	if err != nil {
		log.Println("Error converting service plan")
		return err
	}
	sp.CreateJsonSchema = datatypes.JSON(spec)
	sp.Description = attrs["description"].(string)
	sp.Name = attrs["name"].(string)
	sp.SourceRef = attrs["id"].(json.Number).String()
	return nil
}

func (sp *ServicePlan) Delete(tx *gorm.DB) error {
	return tx.Model(&ServicePlan{}).Where("source_ref = ? AND source_id = ?", sp.SourceRef, sp.Source.ID).Delete(&ServicePlan{}).Error
}

func (sp *ServicePlan) CreateOrUpdate(tx *gorm.DB, attrs map[string]interface{}, r io.Reader) error {
	err := sp.makeObject(attrs, r)
	if err != nil {
		log.Infof("Error creating a new service plan object %v", err)
		return err
	}
	var instance ServicePlan
	err = tx.Where(&ServicePlan{SourceID: sp.Source.ID, Tower: base.Tower{SourceRef: sp.SourceRef}}).First(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Infof("Creating a new Survey Spec %s", sp.SourceRef)
			if result := tx.Create(sp); result.Error != nil {
				return fmt.Errorf("Error creating survey spec: %v" + result.Error.Error())
			}
		} else {
			log.Infof("Error locating Survey Spec %s %v", sp.SourceRef, err)
			return err
		}
	} else {
		log.Infof("Survey Spec %s exists in DB with ID %d", sp.SourceRef, instance.ID)
		sp.ID = instance.ID // Get the Existing ID for the object
		instance.CreateJsonSchema = sp.CreateJsonSchema
		instance.Description = sp.Description
		instance.Name = sp.Name

		log.Infof("Saving Survey Spec  source_ref %s", sp.SourceRef)
		err := tx.Save(&instance).Error
		if err != nil {
			log.Error("Error Updating Service Plan  source_ref %s", sp.SourceRef)
			return err
		}
	}
	return nil
}
