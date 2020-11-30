package servicecredential

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/mkanoor/catalog_tower_persister/internal/models/base"
	"github.com/mkanoor/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/mkanoor/catalog_tower_persister/internal/models/source"
	"github.com/mkanoor/catalog_tower_persister/internal/models/tenant"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type ServiceCredential struct {
	base.Base
	base.Tower
	Name                           string
	TypeName                       string
	Description                    string
	TenantID                       int64
	SourceID                       int64
	ServiceCredentialTypeID        sql.NullInt64 `gorm:"default:null"`
	Tenant                         tenant.Tenant
	Source                         source.Source
	ServiceCredentialType          servicecredentialtype.ServiceCredentialType
	ServiceCredentialTypeSourceRef string `gorm:"-"`
}

func (sc *ServiceCredential) validateAttributes(attrs map[string]interface{}) error {
	requiredAttrs := []string{"created",
		"modified",
		"name",
		"id",
		"description",
		"credential_type"}
	for _, name := range requiredAttrs {
		if _, ok := attrs[name]; !ok {
			return errors.New("Missing Required Attribute " + name)
		}
	}
	return nil
}

func (sc *ServiceCredential) makeObject(attrs map[string]interface{}) error {
	err := sc.validateAttributes(attrs)
	if err != nil {
		return err
	}

	sc.SourceCreatedAt, err = base.TowerTime(attrs["created"].(string))
	if err != nil {
		return err
	}
	/*sc.SourceUpdatedAt, err = towerTime(attrs["modified"].(string))
	if err != nil {
		return err
	}*/
	sc.Description = attrs["description"].(string)
	sc.Name = attrs["name"].(string)
	sc.SourceRef = attrs["id"].(json.Number).String()
	sc.ServiceCredentialTypeSourceRef = attrs["credential_type"].(json.Number).String()
	return nil
}

func (sc *ServiceCredential) CreateOrUpdate(tx *gorm.DB, attrs map[string]interface{}) error {
	err := sc.makeObject(attrs)
	if err != nil {
		log.Infof("Error creating a new service credential object %v", err)
		return err
	}

	var instance ServiceCredential
	err = tx.Where(&ServiceCredential{SourceID: sc.Source.ID, Tower: base.Tower{SourceRef: sc.SourceRef}}).First(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Infof("Creating a new Credential %s", sc.SourceRef)
			if result := tx.Create(sc); result.Error != nil {
				return fmt.Errorf("Error creating service credential : %v" + result.Error.Error())
			}
		} else {
			log.Infof("Error locating Credential %s %v", sc.SourceRef, err)
			return err
		}
	} else {
		log.Infof("Service Credential %s exists in DB with ID %d", sc.SourceRef, instance.ID)
		sc.ID = instance.ID // Get the Existing ID for the object
		instance.Description = sc.Description
		instance.Name = sc.Name
		instance.ServiceCredentialTypeSourceRef = sc.ServiceCredentialTypeSourceRef

		log.Infof("Saving Service Credential source_ref %s", sc.SourceRef)
		err := tx.Save(&instance).Error
		if err != nil {
			log.Error("Error Updating Service Credential  source_ref %s", sc.SourceRef)
			return err
		}
	}
	return nil
}

func (sc *ServiceCredential) DeleteOldServiceCredentials(tx *gorm.DB, sourceRefs []string) error {
	results, err := sc.getDeleteIDs(tx, sourceRefs)
	if err != nil {
		log.Errorf("Error getting Delete IDs for service credentials %v", err)
		return err
	}
	for _, res := range results {
		log.Infof("Attempting to delete ServiceCredential with ID %d Source ref %s", res.ID, res.SourceRef)
		result := tx.Delete(&ServiceCredential{SourceID: sc.Source.ID, TenantID: sc.Tenant.ID, Tower: base.Tower{SourceRef: res.SourceRef}}, res.ID)
		if result.Error != nil {
			log.Errorf("Error deleting Service Credential %d %s %v", res.ID, res.SourceRef, result.Error)
			return result.Error
		}
	}
	return nil
}

func (sc *ServiceCredential) getDeleteIDs(tx *gorm.DB, sourceRefs []string) ([]base.ResultIDRef, error) {
	var result []base.ResultIDRef
	var deleteResultIDRef []base.ResultIDRef
	sort.Strings(sourceRefs)
	length := len(sourceRefs)
	if err := tx.Model(&ServiceCredential{SourceID: sc.Source.ID}).Find(&result).Error; err != nil {
		log.Errorf("Error fetching ServiceCredential %v", err)
		return deleteResultIDRef, err
	}
	for _, res := range result {
		if !base.SourceRefExists(res.SourceRef, sourceRefs, length) {
			deleteResultIDRef = append(deleteResultIDRef, res)
		}
	}
	return deleteResultIDRef, nil
}
