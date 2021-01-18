#!/bin/bash

# source unit_test.sh

# --------------------------------------------
# Options that must be configured by app owner
# --------------------------------------------
APP_NAME="catalog-tower-persister"  # name of app-sre "application" folder this component lives in
COMPONENT_NAME="catalog-tower-persister"  # name of app-sre "resourceTemplate" in deploy.yaml for this component
IMAGE="quay.io/cloudservices/catalog-tower-persister"  

IQE_PLUGINS="catalog-tower-persister"
IQE_MARKER_EXPRESSION="smoke"
IQE_FILTER_EXPRESSION=""


# Install bonfire repo/initialize
CICD_URL=https://raw.githubusercontent.com/RedHatInsights/bonfire/master/cicd
curl -s $CICD_URL/bootstrap.sh -o bootstrap.sh
source bootstrap.sh  # checks out bonfire and changes to "cicd" dir...

source build.sh
source deploy_ephemeral_env.sh
source smoke_test.sh
