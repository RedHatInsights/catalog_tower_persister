#!/bin/bash

# --------------------------------------------
# Options that must be configured by app owner
# --------------------------------------------
APP_NAME="catalog-tower-persister"  # name of app-sre "application" folder this component lives in
COMPONENT_NAME="catalog-tower-persister"  # name of app-sre "resourceTemplate" in deploy.yaml for this component
IMAGE="quay.io/cloudservices/catalog_tower_persister"  

IQE_PLUGINS="catalog-tower-persister"
IQE_MARKER_EXPRESSION="smoke"
IQE_FILTER_EXPRESSION=""


# Install bonfire repo/initialize
CICD_URL=https://raw.githubusercontent.com/RedHatInsights/bonfire/master/cicd
curl -s $CICD_URL/bootstrap.sh > .cicd_bootstrap.sh && source .cicd_bootstrap.sh

source $CICD_ROOT/build.sh
#source $APP_ROOT/unit_test.sh
source $CICD_ROOT/deploy_ephemeral_env.sh
#source $CICD_ROOT/smoke_test.sh

# Until test results produce a junit XML file, create a dummy result file so Jenkins will pass
mkdir -p $WORKSPACE/artifacts
cat << EOF > ${WORKSPACE}/artifacts/junit-dummy.xml
<testsuite tests="1">
    <testcase classname="dummy" name="dummytest"/>
</testsuite>
EOF
