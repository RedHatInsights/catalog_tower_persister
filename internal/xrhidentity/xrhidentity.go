package xrhidentity

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

type XRHIdentity struct {
	Entitlements struct {
		Insights struct {
			IsEntitled bool `json:"is_entitled"`
			IsTrial    bool `json:"is_trial"`
		} `json:"insights"`
		SmartManagement struct {
			IsEntitled bool `json:"is_entitled"`
			IsTrial    bool `json:"is_trial"`
		} `json:"smart_management"`
		HybridCloud struct {
			IsEntitled bool `json:"is_entitled"`
			IsTrial    bool `json:"is_trial"`
		} `json:"hybrid_cloud"`
		Openshift struct {
			IsEntitled bool `json:"is_entitled"`
			IsTrial    bool `json:"is_trial"`
		} `json:"openshift"`
	} `json:"entitlements"`
	Identity struct {
		Internal struct {
			AuthTime int    `json:"auth_time"`
			AuthType string `json:"auth_type"`
			OrgID    string `json:"org_id"`
		} `json:"internal"`
		AccountNumber string `json:"account_number"`
		User          struct {
			FirstName  string `json:"first_name"`
			IsActive   bool   `json:"is_active"`
			LastName   string `json:"last_name"`
			Locale     string `json:"locale"`
			IsOrgAdmin bool   `json:"is_org_admin"`
			Username   string `json:"username"`
			Email      string `json:"email"`
		} `json:"user"`
		Type string `json:"type"`
	} `json:"identity"`
}

func GetXRHIdentity(str string) (*XRHIdentity, error) {
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		fmt.Println("error:", err)
		return nil, err
	}

	var xrh XRHIdentity
	err = json.Unmarshal(data, &xrh)
	if err != nil {
		fmt.Println("error:", err)
		return nil, err
	}

	return &xrh, nil
}
