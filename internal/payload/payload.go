package payload

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredential"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/servicecredentialtype"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceinventory"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceoffering"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceofferingnode"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/serviceplan"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/source"
	"github.com/RedHatInsights/catalog_tower_persister/internal/models/tenant"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"io"
	"net/http"
)

// pageResponse stores the response from the Ansible Tower API call which
// has been bundled into the tar file.

// WorkflowNode stores the workflow relations
type WorkflowNode struct {
	SourceRef                    string
	ServiceOfferingSourceRef     string
	RootServiceOfferingSourceRef string
	UnifiedJobType               string
}

// ObjectRepos contains the different repositories for the objects we manage
type ObjectRepos struct {
	servicecredentialrepo     servicecredential.Repository
	servicecredentialtyperepo servicecredentialtype.Repository
	serviceinventoryrepo      serviceinventory.Repository
	serviceplanrepo           serviceplan.Repository
	serviceofferingrepo       serviceoffering.Repository
	serviceofferingnoderepo   serviceofferingnode.Repository
}

// BillOfLading stores the cumulative information about all pages that we read from
// the tar file
type BillOfLading struct {
	logger                               *logrus.Entry
	tenant                               *tenant.Tenant
	source                               *source.Source
	dbTransaction                        *gorm.DB
	repos                                *ObjectRepos
	inventoryMap                         map[string][]int64
	serviceCredentialToCredentialTypeMap map[string][]int64
	jobTemplateSurvey                    []string
	workflowJobTemplateSurvey            []string
	workflowNodes                        []WorkflowNode
	jobTemplateSourceRefs                []string
	inventorySourceRefs                  []string
	credentialSourceRefs                 []string
	credentialTypeSourceRefs             []string
	workflowNodeSourceRefs               []string
}

// MakeBillOfLading creates a BillOfLading
func MakeBillOfLading(logger *logrus.Entry, tenant *tenant.Tenant, source *source.Source, repos *ObjectRepos, dbTransaction *gorm.DB) *BillOfLading {
	bol := BillOfLading{
		tenant: tenant,
		source: source,
		repos:  repos,
		logger: logger}
	if bol.repos == nil {
		bol.repos = defaultObjectRepos(dbTransaction)
	}
	bol.inventoryMap = make(map[string][]int64)
	bol.serviceCredentialToCredentialTypeMap = make(map[string][]int64)
	return &bol
}

// ProcessTar downloads a Tar file from a given URL and processes one page (file) at a time
// from the compressed tar.
func (bol *BillOfLading) ProcessTar(ctx context.Context, url string, shutdown chan struct{}) error {

	fmt.Println("Debugging - Fetching URL %s", url)
	bol.logger.Infof("Fetching URL %s", url)

	resp, err := http.Get(url)
	if err != nil {
		bol.logger.Errorf("Error getting URL %s %v", url, err)
		return err
	}
	defer resp.Body.Close()

	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		bol.logger.Errorf("Error opening gzip %v", err)
		return err
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			bol.logger.Errorf("Error reading tar header %v", err)
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeReg:
			bol.logger.Infof("Contents of %s", hdr.Name)
			fmt.Println("Debugging Processing - File %s", hdr.Name)
			err = bol.ProcessPage(ctx, hdr.Name, tr)
			fmt.Println("Debugging Done Processing - File %s", hdr.Name)
			if err != nil {
				bol.logger.Errorf("Error handling file %s %v", hdr.Name, err)
				return err
			}
		}

	}

	fmt.Println("Debugging Post Processing")
	err = bol.postProcess(ctx)
	if err != nil {
		bol.logger.Errorf("Error post processing data %v", err)
		return err
	}
	fmt.Println("Debugging Log Reports")
	bol.logReports(ctx)
	return nil
}

// GetStats get counters for objects added/updated/deleted which can be set back to the
// Catalog Inventory API
func (bol *BillOfLading) GetStats(ctx context.Context) map[string]interface{} {
	stats := map[string]interface{}{
		"credentials":            bol.repos.servicecredentialrepo.Stats(),
		"credential_types":       bol.repos.servicecredentialrepo.Stats(),
		"inventories":            bol.repos.serviceinventoryrepo.Stats(),
		"service_plans":          bol.repos.serviceplanrepo.Stats(),
		"service_offering":       bol.repos.serviceofferingrepo.Stats(),
		"service_offering_nodes": bol.repos.serviceofferingnoderepo.Stats(),
	}
	return stats
}

func (bol *BillOfLading) postProcess(ctx context.Context) error {
	err := bol.ProcessLinks(ctx, bol.dbTransaction)
	if err != nil {
		bol.logger.Errorf("Error in linking objects %v", err)
		return err
	}
	err = bol.ProcessDeletes(ctx)
	if err != nil {
		bol.logger.Errorf("Error in deleting objects %v", err)
		return err
	}
	return nil
}

// logReports log the objects added/updated/deleted
func (bol *BillOfLading) logReports(ctx context.Context) {
	x := bol.repos.servicecredentialrepo.Stats()
	bol.logger.Info(fmt.Sprintf("Credential Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = bol.repos.servicecredentialrepo.Stats()
	bol.logger.Info(fmt.Sprintf("Credential Type Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = bol.repos.serviceinventoryrepo.Stats()
	bol.logger.Info(fmt.Sprintf("Inventory Type Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = bol.repos.serviceplanrepo.Stats()
	bol.logger.Info(fmt.Sprintf("Service Plan Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = bol.repos.serviceofferingrepo.Stats()
	bol.logger.Info(fmt.Sprintf("Service Offering Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
	x = bol.repos.serviceofferingnoderepo.Stats()
	bol.logger.Info(fmt.Sprintf("Service Offering Node Add %d Updates %d Deletes %d", x["adds"], x["updates"], x["deletes"]))
}

func defaultObjectRepos(dbTransaction *gorm.DB) *ObjectRepos {
	return &ObjectRepos{
		servicecredentialrepo:     servicecredential.NewGORMRepository(dbTransaction),
		servicecredentialtyperepo: servicecredentialtype.NewGORMRepository(dbTransaction),
		serviceinventoryrepo:      serviceinventory.NewGORMRepository(dbTransaction),
		serviceplanrepo:           serviceplan.NewGORMRepository(dbTransaction),
		serviceofferingrepo:       serviceoffering.NewGORMRepository(dbTransaction),
		serviceofferingnoderepo:   serviceofferingnode.NewGORMRepository(dbTransaction),
	}
}
