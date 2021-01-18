package serviceofferingnode

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
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceoffering"
	"github.com/sirupsen/logrus"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// ErrIgnoreTowerObject is raised when we encounter an object that we dont need
var ErrIgnoreTowerObject = errors.New("Ignoring non job template or workflow job template nodes")

var inventoriesRe = regexp.MustCompile(`\/api\/v2\/inventories\/(\w)\/`)

// ServiceOfferingNode maps to the Workflow Job Template Node fron Ansible Tower
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

// Repository interface supports deleted unwanted objects and creating or updating object
type Repository interface {
	DeleteUnwanted(ctx context.Context, logger *logrus.Entry, so *ServiceOfferingNode, keepSourceRefs []string) error
	CreateOrUpdate(ctx context.Context, logger *logrus.Entry, so *ServiceOfferingNode, attrs map[string]interface{}) error
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

// CreateOrUpdate a ServiceOfferingNode Object in the Database
func (gr *gormRepository) CreateOrUpdate(ctx context.Context, logger *logrus.Entry, son *ServiceOfferingNode, attrs map[string]interface{}) error {
	err := son.makeObject(attrs)
	if err != nil {
		logger.Infof("Error creating a new service offering node object %v", err)
		return err
	}
	var instance ServiceOfferingNode
	err = gr.db.Where(&ServiceOfferingNode{SourceID: son.SourceID, Tower: base.Tower{SourceRef: son.SourceRef}}).First(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			logger.Infof("Creating a new Service Offering Node %s", son.SourceRef)
			if result := gr.db.Create(son); result.Error != nil {
				return fmt.Errorf("Error creating service offering node : %v", result.Error.Error())
			}
		} else {
			logger.Infof("Error locating Service Offering Node %s %v", son.SourceRef, err)
			return err
		}
		gr.creates++
	} else {
		logger.Infof("Service Offering Node %s exists in DB with ID %d", son.SourceRef, instance.ID)
		son.ID = instance.ID // Get the Existing ID for the object
		instance.RootServiceOfferingSourceRef = son.RootServiceOfferingSourceRef
		instance.ServiceOfferingSourceRef = son.ServiceOfferingSourceRef
		instance.Name = son.Name
		instance.ServiceInventorySourceRef = son.ServiceInventorySourceRef

		logger.Infof("Saving Service Offering source_ref %s", son.SourceRef)
		err := gr.db.Save(&instance).Error
		if err != nil {
			logger.Errorf("Error Updating Service Offering Node  source_ref %s", son.SourceRef)
			return err
		}
		gr.updates++
	}
	return nil
}

// DeleteUnwanted deletes any objects not listed in the keepSourceRefs
// This is used to delete ServiceOfferingNode that exist in our database but have been
// deleted from the Ansible Tower
func (gr *gormRepository) DeleteUnwanted(ctx context.Context, logger *logrus.Entry, son *ServiceOfferingNode, keepSourceRefs []string) error {
	results, err := son.getDeleteIDs(ctx, logger, gr.db, keepSourceRefs)
	if err != nil {
		logger.Errorf("Error getting Delete IDs for service offering node %v", err)
		return err
	}
	for _, res := range results {
		logger.Infof("Attempting to delete ServiceOfferingNode with ID %d Source ref %s", res.ID, res.SourceRef)
		result := gr.db.Delete(&ServiceOfferingNode{SourceID: son.SourceID, TenantID: son.TenantID, Tower: base.Tower{SourceRef: res.SourceRef}}, res.ID)
		if result.Error != nil {
			logger.Errorf("Error deleting Service Offering Node %d %s %v", res.ID, res.SourceRef, result.Error)
			return result.Error
		}
		gr.deletes++
	}
	return nil
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
		return ErrIgnoreTowerObject
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

func (son *ServiceOfferingNode) getDeleteIDs(ctx context.Context, logger *logrus.Entry, tx *gorm.DB, keepSourceRefs []string) ([]base.ResultIDRef, error) {
	var result []base.ResultIDRef
	var deleteResultIDRef []base.ResultIDRef
	sort.Strings(keepSourceRefs)
	length := len(keepSourceRefs)
	if err := tx.Table("service_offering_nodes").Select("id, source_ref").Where("source_id = ? AND archived_at IS NULL", son.SourceID).Scan(&result).Error; err != nil {
		logger.Errorf("Error fetching ServiceOfferingNode %v", err)
		return deleteResultIDRef, err
	}
	for _, res := range result {
		if !base.SourceRefExists(res.SourceRef, keepSourceRefs, length) {
			deleteResultIDRef = append(deleteResultIDRef, res)
		}
	}
	return deleteResultIDRef, nil
}
