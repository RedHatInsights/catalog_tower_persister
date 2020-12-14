package servicecredentialtype

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/mkanoor/catalog_tower_persister/internal/models/base"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type ServiceCredentialType struct {
	base.Base
	base.Tower
	Name        string
	Description string
	Kind        string
	Namespace   string
	TenantID    int64
	SourceID    int64
}

func (sct *ServiceCredentialType) validateAttributes(attrs map[string]interface{}) error {
	requiredAttrs := []string{"kind",
		"namespace",
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

func (sct *ServiceCredentialType) makeObject(attrs map[string]interface{}) error {
	err := sct.validateAttributes(attrs)
	if err != nil {
		return err
	}

	sct.SourceCreatedAt, err = base.TowerTime(attrs["created"].(string))
	if err != nil {
		return err
	}
	/*sct.SourceUpdatedAt, err = towerTime(attrs["modified"].(string))
	if err != nil {
		return err
	}*/
	sct.Description = attrs["description"].(string)
	sct.Kind = attrs["kind"].(string)
	sct.Namespace = attrs["namespace"].(string)
	sct.Name = attrs["name"].(string)
	sct.SourceRef = attrs["id"].(json.Number).String()
	return nil
}

func (sct *ServiceCredentialType) CreateOrUpdate(ctx context.Context, tx *gorm.DB, attrs map[string]interface{}) error {
	err := sct.makeObject(attrs)
	if err != nil {
		log.Infof("Error creating a new credential type object %v", err)
		return err
	}

	var instance ServiceCredentialType

	err = tx.Where(&ServiceCredentialType{SourceID: sct.SourceID, Tower: base.Tower{SourceRef: sct.SourceRef}}).First(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Infof("Creating a new Credential Type %s", sct.SourceRef)
			if result := tx.Create(sct); result.Error != nil {
				return fmt.Errorf("Error creating service credential type : %v" + result.Error.Error())
			}
		} else {
			log.Infof("Error locating Credential Type %s %v", sct.SourceRef, err)
			return err
		}

	} else {
		log.Infof("Service Credential Type %s exists in DB with ID %d", sct.SourceRef, instance.ID)
		sct.ID = instance.ID // Get the Existing ID for the object
		instance.Description = sct.Description
		instance.Name = sct.Name
		instance.Namespace = sct.Namespace
		instance.Kind = sct.Kind

		log.Infof("Saving Service Credential Type source_ref %s", sct.SourceRef)
		err := tx.Save(&instance).Error
		if err != nil {
			log.Error("Error Updating Service Credential Type source_ref %s", sct.SourceRef)
			return err
		}
	}
	return nil
}

func (sct *ServiceCredentialType) DeleteOldServiceCredentialTypes(ctx context.Context, tx *gorm.DB, sourceRefs []string) error {
	results, err := sct.getDeleteIDs(tx, sourceRefs)
	if err != nil {
		log.Errorf("Error getting Delete IDs for service credential types %v", err)
		return err
	}
	for _, res := range results {
		log.Infof("Attempting to delete ServiceCredentialType with ID %d Source ref %s", res.ID, res.SourceRef)
		result := tx.Delete(&ServiceCredentialType{SourceID: sct.SourceID, TenantID: sct.TenantID, Tower: base.Tower{SourceRef: res.SourceRef}}, res.ID)
		if result.Error != nil {
			log.Errorf("Error deleting Service CredentialType %d %s %v", res.ID, res.SourceRef, result.Error)
			return result.Error
		}
	}
	return nil
}

func (sct *ServiceCredentialType) getDeleteIDs(tx *gorm.DB, sourceRefs []string) ([]base.ResultIDRef, error) {
	var result []base.ResultIDRef
	var deleteResultIDRef []base.ResultIDRef
	sort.Strings(sourceRefs)
	length := len(sourceRefs)
	if err := tx.Model(&ServiceCredentialType{SourceID: sct.SourceID}).Find(&result).Error; err != nil {
		log.Errorf("Error fetching ServiceCredentialType %v", err)
		return deleteResultIDRef, err
	}
	for _, res := range result {
		if !base.SourceRefExists(res.SourceRef, sourceRefs, length) {
			deleteResultIDRef = append(deleteResultIDRef, res)
		}
	}
	return deleteResultIDRef, nil
}
