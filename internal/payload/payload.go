package payload

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"

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
)

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

// Loader interface has a Page Handler, after we have handled all the pages
// we handle links between objects and then get rid of any of the objects
// not needed anymore
type Loader interface {
	ProcessPage(ctx context.Context, name string, r io.Reader) error
	ProcessLinks(ctx context.Context, dbTransaction *gorm.DB) error
	ProcessDeletes(ctx context.Context) error
	GetStats(ctx context.Context) map[string]interface{}
}

// MakeBillOfLading creates a BillOfLading
func MakeBillOfLading(logger *logrus.Entry, tenant *tenant.Tenant, source *source.Source, repos *ObjectRepos, dbTransaction *gorm.DB) *BillOfLading {
	bol := BillOfLading{
		tenant:        tenant,
		source:        source,
		repos:         repos,
		dbTransaction: dbTransaction,
		logger:        logger}
	if bol.repos == nil {
		bol.repos = defaultObjectRepos(dbTransaction)
	}
	bol.inventoryMap = make(map[string][]int64)
	bol.serviceCredentialToCredentialTypeMap = make(map[string][]int64)
	return &bol
}

// ProcessTar downloads a Tar file from a given URL and processes one page (file) at a time
// from the compressed tar.
func ProcessTar(ctx context.Context, logger *logrus.Entry, loader Loader, client *http.Client, dbTransaction *gorm.DB, url string, shutdown chan struct{}) error {

	logger.Infof("Fetching URL %s", url)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		logger.Errorf("Error creating new request %v", err)
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("Error getting URL %s %v", url, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Errorf("HTTP Status for URL %s %v", url, resp.StatusCode)
		return fmt.Errorf("Download failed, HTTP Status Code %d", resp.StatusCode)
	}

	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		logger.Errorf("Error opening gzip %v", err)
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
			logger.Errorf("Error reading tar header %v", err)
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeReg:
			logger.Infof("Contents of %s", hdr.Name)
			err = loader.ProcessPage(ctx, hdr.Name, tr)
			if err != nil {
				logger.Errorf("Error handling file %s %v", hdr.Name, err)
				return err
			}
		}

	}

	err = loader.ProcessLinks(ctx, dbTransaction)
	if err != nil {
		logger.Errorf("Error in linking objects %v", err)
		return err
	}

	err = loader.ProcessDeletes(ctx)
	if err != nil {
		logger.Errorf("Error in deleting objects %v", err)
		return err
	}
	return nil
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
