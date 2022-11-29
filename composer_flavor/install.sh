#!/bin/bash -eu

# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Functions
function usage() {
  cat << EOF
install.sh - V2 (with Cloud Composer)
==========
Usage:
  install.sh [options]
Options - Mandatory:
  --project         GCP Project Id
Delegator Deployment Options:
  --cm360-table            BQ table contains the transformed data
  --cm360-profile-id       CM360 profile id
  --cm360-fl-activity-id   CM360 floodlight activity id
  --cm360-fl-config-id     CM360 floodlight configuration id
Options with default values:  
  --service-account Service Account name
  --dataset-sa360   Big Query dataset for SA360 to verify or create
  --dataset-gmc     Big Query dataset for Google Merchant Center to verify or create
  --dataset-profit  Big Query dataset for Business profit data to verify or create
  --storage-logs    Storage Account to upload conversion logs
  --storage-profit  Storage Account to upload the profit data
  --profit-file     Filename along with path containing the profit data
  --composer-location Google Cloud Region for Cloud Composer
  --composer-name   Composer name for data wrangling and the CM360 processing logic
Deployment directives:
  --activate-apis     Activate all missing but required Cloud APIs
  --create-service-account
                      Create the service account and client secrets
  --deploy-all        Deploy all services
  --deploy-bigquery   Create BQ datasets
  --deploy-storage    Create storage buckets
  --deploy-sql-transform Deploys the SQL Stored Proc
  --deploy-composer   Creates Cloud Composer
Test the solution with test data and code:
  --deploy-test-module Creates test data and deploys code for testing
  --delete-test-module Deletes test data and deploys code for testing
  --list-test-module Lists test data and deploys code for testing
General switches:
  --dry-run         Don't do anything, just print the commands you would otherwise run. Useful
                    for testing.
  --delete-all      Alert!-Deletes GCP resources. Useful for unit testing.
  --list-all        Lists all the GCP resources for the solution.

Example:
sh install.sh --dry-run --deploy-all \
  --project=<project_id> \
  --cm360-table=my_tbl \
  --cm360-profile-id=my_profile_id \
  --cm360-fl-activity-id=my_activity_id \
  --cm360-fl-config-id=my_config-id 

sh install.sh --dry-run --deploy-profit-data \
  --project=<project_id> \
  --dataset-profit=my_profit_ds \
  --storage-profit=my_profit_sa \
  --profit-file=my/path/file.csv

sh install.sh --dry-run --deploy-delegator \
  --project=<project_id> \
  --cm360-table=my_tbl \
  --cm360-profile-id=my_profile_id \
  --cm360-fl-activity-id=my_activity_id \
  --cm360-fl-config-id=my_config-id 

sh install.sh --dry-run --deploy-all \
  --project=<project_id> \
  --service-account=my_profitbid_sa \
  --dataset-sa360=my_sa360  \
  --dataset-gmc=my_gmc \
  --dataset-profit=my_profit \
  --storage-logs=my_con_log  \
  --storage-profit=my_profit_csv \
  --cm360-table=my_tbl \
  --cm360-profile-id=my_profile_id \
  --cm360-fl-activity-id=my_activity_id \
  --cm360-fl-config-id=my_config-id \
  --composer-location=us-central1 

EOF

}

# Provisioning and deprovisioning Solution and Unit test
# =========================================
# mkdir -p $HOME/solutions/profit-bidder
# cd $HOME/solutions/profit-bidder
# git clone https://github.com/google/profit-bidder.git .
# sh install.sh  --project=<gcp project id> --deploy-all
# sh install.sh  --project=<gcp project id> --deploy-test-module
# sh install.sh  --project=<gcp project id> --deploy-sql-transform
# sh install.sh  --project=<gcp project id> --deploy-composer
# #sh install.sh --delete-all --project=<project_id>
# 
# To test the solution first deploy the solution and then
# deploy the test module.
# sh install.sh --deploy-test-module --project=<project_id>
# sh install.sh --list-test-module --project=<project_id>
# # sh install.sh --delete-test-module --project=<project_id>

function profit_data_usage {
  cat << EOF
CSV Format and sample values for the client_margin_data_table table:
sku,profit
GGOEAAAB081014,0.19
GGOEAAEB031617,0.59
GGOEAAEJ030917,0.59
EOF
}

function join { local IFS="$1"; shift; echo "$*"; }

# Default values
SOLUTION_PREFIX="pb_"

SERVICE_ACCOUNT_NAME=$SOLUTION_PREFIX"profit-bidder"
DS_SA360=$SOLUTION_PREFIX"sa360_data"
DS_GMC=$SOLUTION_PREFIX"gmc_data"
DS_BUSINESS_DATA=$SOLUTION_PREFIX"business_data"
STORAGE_LOGS=$SOLUTION_PREFIX"conversion-upload_log"
STORAGE_PROFIT=$SOLUTION_PREFIX"profit_data"
CLIENT_MARGIN_DATA_TABLE_NAME="client_margin_data_table"
CLIENT_MARGIN_DATA_FILE_NAME="client_profit.csv"
CLIENT_MARGIN_DATA_FILE_WITH_PATH=`echo $HOME`/$CLIENT_MARGIN_DATA_FILE_NAME
COMPOSER_LOCATION="us-central1"
COMPOSER_NAME=$SOLUTION_PREFIX"composer"
COMPOSER_VERSION="composer-2.0.18-airflow-2.2.5"
SA_ROLES="roles/bigquery.user roles/bigquery.jobUser roles/composer.worker roles/composer.ServiceAgentV2Ext roles/iam.serviceAccountTokenCreator"
DAG_ID=$SOLUTION_PREFIX"pipeline"

CM360_TABLE="my_transformed_data"
CM360_PROFILE_ID="my_cm_profileid"
CM360_FL_ACTIVITY_ID="my_fl_activity_id"
CM360_FL_CONFIG_ID="my_fl_config_id"

ACTIVATE_APIS=0
CREATE_SERVICE_ACCOUNT=0
DEPLOY_BQ=0
DEPLOY_STORAGE=0
DEPLOY_PROFIT_DATA=0
DEPLOY_SQL_TRANSFORM=0
DEPLOY_COMPOSER=0
DEPLOY_TEST_MODULE=0
DELETE_TEST_MODULE=0
LIST_TEST_MODULE=0
DELETE_SOLUTION=0
LIST_SOLUTION=0

PROJECT=
ADMIN=
DRY_RUN=""

VERBOSE=false

# Command line parser
while [[ ${1:-} == -* ]] ; do
  case $1 in
    --project*)
      IFS="=" read _cmd PROJECT <<< "$1" && [ -z ${PROJECT} ] && shift && PROJECT=$1
      ;;
    --service-account*)
      IFS="=" read _cmd SERVICE_ACCOUNT_NAME <<< "$1" && [ -z ${SERVICE_ACCOUNT_NAME} ] && shift && SERVICE_ACCOUNT_NAME=$1
      ;;
    --dataset-sa360*)
      IFS="=" read _cmd DS_SA360 <<< "$1" && [ -z ${DS_SA360} ] && shift && DS_SA360=$1
      ;;
    --dataset-gmc*)
      IFS="=" read _cmd DS_GMC <<< "$1" && [ -z ${DS_GMC} ] && shift && DS_GMC=$1
      ;;
    --dataset-profit*)
      IFS="=" read _cmd DS_BUSINESS_DATA <<< "$1" && [ -z ${DS_BUSINESS_DATA} ] && shift && DS_BUSINESS_DATA=$1
      ;;
    --storage-logs*)
      IFS="=" read _cmd STORAGE_LOGS <<< "$1" && [ -z ${STORAGE_LOGS} ] && shift && STORAGE_LOGS=$1
      ;;
    --storage-profit*)
      IFS="=" read _cmd STORAGE_PROFIT <<< "$1" && [ -z ${STORAGE_PROFIT} ] && shift && STORAGE_PROFIT=$1
      ;;
    --profit-file*)
      IFS="=" read _cmd CLIENT_MARGIN_DATA_FILE <<< "$1" && [ -z ${CLIENT_MARGIN_DATA_FILE} ] && shift && CLIENT_MARGIN_DATA_FILE=$1
      ;;
    --composer-location*)
      IFS="=" read _cmd COMPOSER_LOCATION <<< "$1" && [ -z ${COMPOSER_LOCATION} ] && shift && COMPOSER_LOCATION=$1
      ;;
    --composer-name*)
      IFS="=" read _cmd COMPOSER_NAME <<< "$1" && [ -z ${COMPOSER_NAME} ] && shift && COMPOSER_NAME=$1
      ;;
    --deploy-all)
      DEPLOY_BQ=1
      DEPLOY_STORAGE=1
      DEPLOY_PROFIT_DATA=1      
      ACTIVATE_APIS=1
      CREATE_SERVICE_ACCOUNT=1
      DEPLOY_SQL_TRANSFORM=1
      DEPLOY_COMPOSER=1
      ;;
    --deploy-bigquery)
      DEPLOY_BQ=1
      ;;
    --deploy-storage)
      DEPLOY_STORAGE=1
      ;;
    --deploy-profit-data)
      DEPLOY_PROFIT_DATA=1
      ;;  
    --deploy-sql-transform)
      DEPLOY_SQL_TRANSFORM=1
      ;;
    --deploy-test-module)
      DEPLOY_TEST_MODULE=1
      ;;
    --delete-test-module)
      DELETE_TEST_MODULE=1
      ;;
    --list-test-module)
      LIST_TEST_MODULE=1
      ;;
    --cm360-table*)
      IFS="=" read _cmd CM360_TABLE <<< "$1" && [ -z ${CM360_TABLE} ] && shift && CM360_TABLE=$1
      ;;
    --cm360-profile-id*)
      IFS="=" read _cmd CM360_PROFILE_ID <<< "$1" && [ -z ${CM360_PROFILE_ID} ] && shift && CM360_PROFILE_ID=$1
      ;;
    --cm360-fl-activity-id*)
      IFS="=" read _cmd CM360_FL_ACTIVITY_ID <<< "$1" && [ -z ${CM360_FL_ACTIVITY_ID} ] && shift && CM360_FL_ACTIVITY_ID=$1
      ;;
    --cm360-fl-config-id*)
      IFS="=" read _cmd CM360_FL_CONFIG_ID <<< "$1" && [ -z ${CM360_FL_CONFIG_ID} ] && shift && CM360_FL_CONFIG_ID=$1
      ;;
    --deploy-composer*)
      DEPLOY_COMPOSER=1
      ;;  
    --delete-all)
      DELETE_SOLUTION=1
      ;;  
    --list-all)
      LIST_SOLUTION=1
      ;;  
    --activate-apis)
      ACTIVATE_APIS=1
      ;;
    --create-service-account)
      CREATE_SERVICE_ACCOUNT=1
      ;;
    --dry-run)
      DRY_RUN=echo
      ;;
    --verbose)
      VERBOSE=true
      ;;
    --no-code)
      DEPLOY_CODE=0
      ;;
    *)
      usage
      echo -e "\nUnknown parameter $1."
      exit
  esac
  shift
done

SQL_TRANSFORM_PROJECT_ID=$PROJECT
SQL_TRANSFORM_SA360_DATASET_NAME=$DS_SA360
SQL_TRANSFORM_ADVERTISER_ID="43939335402485897" #synthensized id to test.
SQL_TRANSFORM_TIMEZONE="America/New_York"
SQL_TRANSFORM_SOURCE_FLOODLIGHT_NAME="My Sample Floodlight Activity"
SQL_TRANSFORM_ACCOUNT_TYPE="Other engines"
SQL_TRANSFORM_GMC_DATASET_NAME=$DS_GMC
SQL_TRANSFORM_GMC_ACCOUNT_ID="mygmc_account_id"
SQL_TRANSFORM_BUSINESS_DATASET_NAME=$DS_BUSINESS_DATA
SQL_TRANSFORM_CLIENT_MARGIN_DATA_TABLE=$CLIENT_MARGIN_DATA_TABLE_NAME
SQL_TRANSFORM_CLIENT_PROFIT_DATA_SKU_COL="sku"
SQL_TRANSFORM_CLIENT_PROFIT_DATA_PROFIT_COL="profit"
SQL_TRANSFORM_TARGET_FLOODLIGHT_NAME="My Sample Floodlight Activity"
SQL_TRANSFORM_PRODUCT_SKU_VAR="u9"
SQL_TRANSFORM_PRODUCT_QUANTITY_VAR="u10"
SQL_TRANSFORM_PRODUCT_UNIT_PRICE_VAR="u11"
SQL_TRANSFORM_PRODUCT_SKU_REGEX="(.*?);"
SQL_TRANSFORM_PRODUCT_QUANTITY_REGEX="(.*?);"
SQL_TRANSFORM_PRODUCT_UNIT_PRICE_REGEX="(.*?);"
SQL_TRANSFORM_PRODUCT_SKU_DELIM="|"
SQL_TRANSFORM_PRODUCT_QUANTITY_DELIM="|"
SQL_TRANSFORM_PRODUCT_UNIT_PRICE_DELIM="|"
SQL_TRANSFORM_DATA_WRANGLIN_SP=$SOLUTION_PREFIX"data_wrangling_sp"

CAMPAIGN_TABLE_NAME="p_Campaign_"$SQL_TRANSFORM_ADVERTISER_ID
CONVERSION_TABLE_NAME="p_Conversion_"$SQL_TRANSFORM_ADVERTISER_ID

SA360_PUSH_CONVERSION_PY_TEMPLATE_FILE="SA360_push_conversion_template.py"
SA360_PUSH_CONVERSION_PY_FILE="push_conversion.py"

COMPOSER_DAG_TEMPLATE_FILE="dag_profitbid_template.py"
COMPOSER_DAG_FILE="dag_profitbid.py"

# comply the name and formulate the sa email account
SERVICE_ACCOUNT_NAME=${SERVICE_ACCOUNT_NAME//_/-}
SA_EMAIL=${SERVICE_ACCOUNT_NAME}@${PROJECT}.iam.gserviceaccount.com

# comply with the name for composer
COMPOSER_NAME=${COMPOSER_NAME//_/-}

function get_roles {
  gcloud projects get-iam-policy ${PROJECT} --flatten="bindings[].members" --format='table(bindings.role)' --filter="bindings.members:${SA_EMAIL}"
}

function deploy_timestamp {
  # 2021-11-01-08-21-49
  date +%Y-%m-%d-%H-%M-%S
}

function maybe_run {
    if [ "${DRY_RUN:-}" = "echo" ]; then
        echo "$@"
    else
        if [ "$VERBOSE" = "true" ]; then
            echo "$@"
        fi
        "$@"
    fi
}

function get_composer_storage_account {
  composer_name=$1
  RETVAL=$( gcloud composer environments describe ${composer_name} --location=$COMPOSER_LOCATION | grep dagGcsPrefix | awk -F '/' '{print $3}' 2>&1)
  echo $RETVAL
}

if [ "${DRY_RUN:-}" = "echo" ]; then
    echo "--dry-run enabled: commands will be echoed instead of executed"
fi

if [ -z "${PROJECT}" ]; then
  usage
  echo "\nYou must specify a project to proceed."
  exit
fi

function create_service_account {
  echo "Creating service account $SA_EMAIL"
  gcloud iam service-accounts describe $SA_EMAIL > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    maybe_run gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} --description 'Profit Bidder Service Account' --project ${PROJECT}
  fi
  for role in ${SA_ROLES}; do
    echo -n "Adding ${SERVICE_ACCOUNT_NAME} to ${role} "
    if get_roles | grep $role &> /dev/null; then
      echo "already added."
    else
      maybe_run gcloud projects add-iam-policy-binding ${PROJECT} --member="serviceAccount:${SA_EMAIL}" --role="${role}"
      echo "added."
    fi
  done   
}

function create_bq_ds {
  dataset=$1
  echo "Creating BQ dataset: '${dataset}'" 
  bq --project_id=${PROJECT} show --dataset ${dataset} > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    maybe_run bq --project_id=${PROJECT} mk --dataset ${dataset}
  else
    echo "Reusing ${dataset}."
  fi
}

function create_storage_account {
  bucket=$1
  echo "Creating Bucket: '${PROJECT}-${bucket}'"
  gsutil ls -p ${PROJECT} gs://${PROJECT}-${bucket} > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    maybe_run gsutil mb -p ${PROJECT} gs://${PROJECT}-${bucket}
  else
    echo "Reusing ${PROJECT}-${bucket}."
  fi
}

function create_bq_table {
  dataset=$1
  table_name=$2
  schema_name=$3
  sql_result=$(list_bq_table $1 $2)
  echo "Creating BQ table: '${dataset}.${table_name}'" 
  if [[ "$sql_result" == *"1"* ]]; then
    echo "Reusing ${dataset}.${table_name}."
  else
    maybe_run bq --project_id=${PROJECT} mk -t --schema ${schema_name} --time_partitioning_type DAY ${dataset}.${table_name}
  fi  
}

function prepare_sql_sp {
  echo "Going to create profit_gen_sp.sql file."
  if [ -f "profit_gen_sp.sql" ]; then
    rm -rf profit_gen_sp.sql
  fi
  maybe_run cp profit_gen_sp_template.sql profit_gen_sp.sql 
  os_type=$(uname -a)
  if [[ "$os_type" == *"Linux"* ]]; then
    maybe_run sed -i "s|<project_id>|$SQL_TRANSFORM_PROJECT_ID|" profit_gen_sp.sql
    maybe_run sed -i "s|<sa360_dataset_name>|$SQL_TRANSFORM_SA360_DATASET_NAME|" profit_gen_sp.sql
    maybe_run sed -i "s|<advertiser_id>|$SQL_TRANSFORM_ADVERTISER_ID|" profit_gen_sp.sql
    maybe_run sed -i "s|<timezone>|$SQL_TRANSFORM_TIMEZONE|" profit_gen_sp.sql
    maybe_run sed -i "s|<floodlight_name>|$SQL_TRANSFORM_SOURCE_FLOODLIGHT_NAME|" profit_gen_sp.sql
    maybe_run sed -i "s|<account_type>|$SQL_TRANSFORM_ACCOUNT_TYPE|" profit_gen_sp.sql
    maybe_run sed -i "s|<gmc_dataset_name>|$SQL_TRANSFORM_GMC_DATASET_NAME|" profit_gen_sp.sql
    maybe_run sed -i "s|<gmc_account_id>|$SQL_TRANSFORM_GMC_ACCOUNT_ID|" profit_gen_sp.sql
    maybe_run sed -i "s|<business_dataset_name>|$SQL_TRANSFORM_BUSINESS_DATASET_NAME|" profit_gen_sp.sql
    maybe_run sed -i "s|<client_margin_data_table>|$SQL_TRANSFORM_CLIENT_MARGIN_DATA_TABLE|" profit_gen_sp.sql
    maybe_run sed -i "s|<client_profit_data_sku_col>|$SQL_TRANSFORM_CLIENT_PROFIT_DATA_SKU_COL|" profit_gen_sp.sql
    maybe_run sed -i "s|<client_profit_data_profit_col>|$SQL_TRANSFORM_CLIENT_PROFIT_DATA_PROFIT_COL|" profit_gen_sp.sql
    maybe_run sed -i "s|<target_floodlight_name>|$SQL_TRANSFORM_TARGET_FLOODLIGHT_NAME|" profit_gen_sp.sql
    maybe_run sed -i "s|<product_sku_var>|$SQL_TRANSFORM_PRODUCT_SKU_VAR|" profit_gen_sp.sql
    maybe_run sed -i "s|<product_sku_regex>|$SQL_TRANSFORM_PRODUCT_SKU_REGEX|" profit_gen_sp.sql
    maybe_run sed -i "s#<product_sku_delim>#$SQL_TRANSFORM_PRODUCT_SKU_DELIM#" profit_gen_sp.sql
    maybe_run sed -i "s|<product_quantity_var>|$SQL_TRANSFORM_PRODUCT_QUANTITY_VAR|" profit_gen_sp.sql
    maybe_run sed -i "s|<product_quantity_regex>|$SQL_TRANSFORM_PRODUCT_QUANTITY_REGEX|" profit_gen_sp.sql
    maybe_run sed -i "s#<product_quantity_delim>#$SQL_TRANSFORM_PRODUCT_QUANTITY_DELIM#" profit_gen_sp.sql
    maybe_run sed -i "s|<product_unit_price_var>|$SQL_TRANSFORM_PRODUCT_UNIT_PRICE_VAR|" profit_gen_sp.sql
    maybe_run sed -i "s|<product_unit_price_regex>|$SQL_TRANSFORM_PRODUCT_UNIT_PRICE_REGEX|" profit_gen_sp.sql
    maybe_run sed -i "s#<product_unit_price_delim>#$SQL_TRANSFORM_PRODUCT_UNIT_PRICE_DELIM#" profit_gen_sp.sql
    maybe_run sed -i "s|<data_wrangling_sp>|$SQL_TRANSFORM_DATA_WRANGLIN_SP|" profit_gen_sp.sql
    maybe_run sed -i "s|<transformed_data_tbl>|$CM360_TABLE|" profit_gen_sp.sql
    if [ ${DEPLOY_TEST_MODULE} -ne 1 ]; then
        maybe_run sed -i "s|--<test>||" profit_gen_sp.sql
    fi
  else
    # below works in the shell of Mac
    maybe_run sed -i "" "s|<project_id>|$SQL_TRANSFORM_PROJECT_ID|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<sa360_dataset_name>|$SQL_TRANSFORM_SA360_DATASET_NAME|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<advertiser_id>|$SQL_TRANSFORM_ADVERTISER_ID|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<timezone>|$SQL_TRANSFORM_TIMEZONE|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<floodlight_name>|$SQL_TRANSFORM_SOURCE_FLOODLIGHT_NAME|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<account_type>|$SQL_TRANSFORM_ACCOUNT_TYPE|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<gmc_dataset_name>|$SQL_TRANSFORM_GMC_DATASET_NAME|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<gmc_account_id>|$SQL_TRANSFORM_GMC_ACCOUNT_ID|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<business_dataset_name>|$SQL_TRANSFORM_BUSINESS_DATASET_NAME|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<client_margin_data_table>|$SQL_TRANSFORM_CLIENT_MARGIN_DATA_TABLE|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<client_profit_data_sku_col>|$SQL_TRANSFORM_CLIENT_PROFIT_DATA_SKU_COL|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<client_profit_data_profit_col>|$SQL_TRANSFORM_CLIENT_PROFIT_DATA_PROFIT_COL|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<target_floodlight_name>|$SQL_TRANSFORM_TARGET_FLOODLIGHT_NAME|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<product_sku_var>|$SQL_TRANSFORM_PRODUCT_SKU_VAR|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<product_sku_regex>|$SQL_TRANSFORM_PRODUCT_SKU_REGEX|" profit_gen_sp.sql
    maybe_run sed -i "" "s#<product_sku_delim>#$SQL_TRANSFORM_PRODUCT_SKU_DELIM#" profit_gen_sp.sql
    maybe_run sed -i "" "s|<product_quantity_var>|$SQL_TRANSFORM_PRODUCT_QUANTITY_VAR|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<product_quantity_regex>|$SQL_TRANSFORM_PRODUCT_QUANTITY_REGEX|" profit_gen_sp.sql
    maybe_run sed -i "" "s#<product_quantity_delim>#$SQL_TRANSFORM_PRODUCT_QUANTITY_DELIM#" profit_gen_sp.sql
    maybe_run sed -i "" "s|<product_unit_price_var>|$SQL_TRANSFORM_PRODUCT_UNIT_PRICE_VAR|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<product_unit_price_regex>|$SQL_TRANSFORM_PRODUCT_UNIT_PRICE_REGEX|" profit_gen_sp.sql
    maybe_run sed -i "" "s#<product_unit_price_delim>#$SQL_TRANSFORM_PRODUCT_UNIT_PRICE_DELIM#" profit_gen_sp.sql
    maybe_run sed -i "" "s|<data_wrangling_sp>|$SQL_TRANSFORM_DATA_WRANGLIN_SP|" profit_gen_sp.sql
    maybe_run sed -i "" "s|<transformed_data_tbl>|$CM360_TABLE|" profit_gen_sp.sql
    if [ ${DEPLOY_TEST_MODULE} -ne 1 ]; then
      maybe_run sed -i "" "s|--<test>||" profit_gen_sp.sql
    fi
  fi  
}

function create_bq_sp {
  dataset=$1
  sp_name=$2
  table_name=$3
  # check if campaign table exists if not then don't create the sp
  #   and print that run the sp after the client marging table is created
  sql_result=$(list_bq_table $4 $3)
  echo "Checking if the preq table exists: '${dataset}.${table_name}'" 
  if [[ "$sql_result" == *"1"* ]]; then
    sql_result=$(list_bq_sp $1 $2)
    echo "Creating BQ Stored Proc: '${dataset}.${sp_name}'"
    if [[ "$sql_result" == *"1"* ]]; then
      echo "Deleting existing ${dataset}.${sp_name}."
      delete_bq_sp $1 $2
    fi
    if [ "${DRY_RUN:-}" = "echo" ]; then
        echo "bq query --use_legacy_sql=false --project_id=${PROJECT} < profit_gen_sp.sql"
    else
        bq query --use_legacy_sql=false --project_id=${PROJECT} < profit_gen_sp.sql
    fi
  echo "Create the stored procedure after CM tables are created...e.g. Campaign, Conversion tbl"
  fi  
}

function create_composer {
  composer_name=$1
  sa=$2
  RETVAL=$( gcloud composer environments describe $composer_name --location=$COMPOSER_LOCATION 2>&1)
  RETVAL=$?
  echo "$RETVAL"
  if (( ${RETVAL} != "0" )); then
    echo "Creating composer...ALERT!!! It takes a while."
    maybe_run gcloud projects add-iam-policy-binding $PROJECT \
        --member serviceAccount:$2 \
        --role roles/composer.worker

    maybe_run gcloud projects add-iam-policy-binding $PROJECT \
        --member serviceAccount:$2 \
        --role roles/composer.ServiceAgentV2Ext

    maybe_run gcloud beta composer environments create $composer_name \
    --location $COMPOSER_LOCATION \
    --image-version $COMPOSER_VERSION \
    --service-account $sa 
  else
    echo "Reusing ${composer_name}."
  fi
}

function prepare_SA360_push_conversion_py {
  echo "Going to create $SA360_PUSH_CONVERSION_PY_FILE file."
  if [ -f "$SA360_PUSH_CONVERSION_PY_FILE" ]; then
    rm -rf $SA360_PUSH_CONVERSION_PY_FILE
  fi
  maybe_run cp $SA360_PUSH_CONVERSION_PY_TEMPLATE_FILE $SA360_PUSH_CONVERSION_PY_FILE
  os_type=$(uname -a)
  if [[ "$os_type" == *"Linux"* ]]; then
    maybe_run sed -i "s|<sa_email>|$SA_EMAIL|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "s|<project_id>|$SQL_TRANSFORM_PROJECT_ID|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "s|<business_dataset_name>|$SQL_TRANSFORM_BUSINESS_DATASET_NAME|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "s|<transformed_data_tbl>|$CM360_TABLE|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "s|<timezone>|$SQL_TRANSFORM_TIMEZONE|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "s|<cm_profileid>|$CM360_PROFILE_ID|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "s|<fl_activity_id>|$CM360_FL_ACTIVITY_ID|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "s|<fl_config_id>|$CM360_FL_CONFIG_ID|" $SA360_PUSH_CONVERSION_PY_FILE
  else
    # below works in the shell of Mac
    maybe_run sed -i "" "s|<sa_email>|$SA_EMAIL|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "" "s|<project_id>|$SQL_TRANSFORM_PROJECT_ID|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "" "s|<business_dataset_name>|$SQL_TRANSFORM_BUSINESS_DATASET_NAME|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "" "s|<transformed_data_tbl>|$CM360_TABLE|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "" "s|<timezone>|$SQL_TRANSFORM_TIMEZONE|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "" "s|<cm_profileid>|$CM360_PROFILE_ID|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "" "s|<fl_activity_id>|$CM360_FL_ACTIVITY_ID|" $SA360_PUSH_CONVERSION_PY_FILE
    maybe_run sed -i "" "s|<fl_config_id>|$CM360_FL_CONFIG_ID|" $SA360_PUSH_CONVERSION_PY_FILE
  fi  
}

function prepare_dag_py {
  echo "Going to create $COMPOSER_DAG_FILE file."
  if [ -f "$COMPOSER_DAG_FILE" ]; then
    rm -rf $COMPOSER_DAG_FILE
  fi
  maybe_run cp $COMPOSER_DAG_TEMPLATE_FILE $COMPOSER_DAG_FILE
  os_type=$(uname -a)
  if [[ "$os_type" == *"Linux"* ]]; then
    maybe_run sed -i "s|<project_id>|$SQL_TRANSFORM_PROJECT_ID|" $COMPOSER_DAG_FILE
    maybe_run sed -i "s|<business_dataset_name>|$SQL_TRANSFORM_BUSINESS_DATASET_NAME|" $COMPOSER_DAG_FILE
    maybe_run sed -i "s|<data_wrangling_sp>|$SQL_TRANSFORM_DATA_WRANGLIN_SP|" $COMPOSER_DAG_FILE
    maybe_run sed -i "s|<dag_name>|$DAG_ID|" $COMPOSER_DAG_FILE
  else
    # below works in the shell of Mac
    maybe_run sed -i "" "s|<project_id>|$SQL_TRANSFORM_PROJECT_ID|" $COMPOSER_DAG_FILE
    maybe_run sed -i "" "s|<business_dataset_name>|$SQL_TRANSFORM_BUSINESS_DATASET_NAME|" $COMPOSER_DAG_FILE
    maybe_run sed -i "" "s|<transformed_data_tbl>|$CM360_TABLE|" $COMPOSER_DAG_FILE
    maybe_run sed -i "" "s|<data_wrangling_sp>|$SQL_TRANSFORM_DATA_WRANGLIN_SP|" $COMPOSER_DAG_FILE
    maybe_run sed -i "" "s|<dag_name>|$DAG_ID|" $COMPOSER_DAG_FILE
  fi  
}

function deploy_code_dag {
  composer_name=$1
  RETVAL=$( get_composer_storage_account $composer_name)
  echo $RETVAL
  if test -z "$RETVAL"; then
    echo "${composer_name} storage account doesn't exists."
  else
    # create the push python code from the template
    prepare_SA360_push_conversion_py
    # upload the python to the dag folder
    maybe_run gcloud beta composer environments storage dags import \
        --environment $COMPOSER_NAME \
        --location $COMPOSER_LOCATION \
        --source="${SA360_PUSH_CONVERSION_PY_FILE}"
    # create the dag file from the tempalte
    pushd dag
    prepare_dag_py
    popd
    # import the dag file
    pushd dag
    maybe_run gcloud beta composer environments storage dags import \
        --environment $COMPOSER_NAME \
        --location $COMPOSER_LOCATION \
        --source="${COMPOSER_DAG_FILE}"
    popd
  fi
}

function load_bq_table {
  dataset=$1
  table_name=$2
  data_file=$3
  schema_name=$4
  sql_result=$(list_bq_table $1 $2)
  echo "Loading data to BQ table: '${dataset}.${table_name}'" 
  if [[ "$sql_result" == *"1"* ]]; then
    delete_bq_table $dataset $table_name
  fi  
  if [[ "$schema_name" == *"autodetect"* ]]; then
    maybe_run bq --project_id=${PROJECT} load \
    --autodetect \
    --source_format=CSV \
    $dataset.$table_name \
    $data_file 
  else
    create_bq_table $dataset $table_name $schema_name
    maybe_run bq --project_id=${PROJECT} load \
      --source_format=CSV \
      --time_partitioning_type=DAY \
      --skip_leading_rows=1 \
      ${dataset}.${table_name} \
      ${data_file}
  fi  
}

function delete_bq_ds {
  dataset=$1
  echo "Deleting BQ dataset: '${dataset}'" 
  bq --project_id=${PROJECT} show --dataset ${dataset}
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo "${dataset} does not exist."
  else
    maybe_run bq rm -r -f -d ${PROJECT}:${dataset}
  fi

}

function delete_storage_account {
  bucket=$1
  echo "Deleting Bucket: '${PROJECT}-${bucket}'"
  gsutil ls -p ${PROJECT} gs://${PROJECT}-${bucket} > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo "${PROJECT}-${bucket} does not exist."
  else
    maybe_run gcloud alpha storage rm --recursive gs://${PROJECT}-${bucket}
  fi
}

function delete_service_account {
  saemail=$1
  echo "Deleting Service Account: '$saemail'"
  gcloud iam service-accounts describe $saemail > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo "$saemail does not exist."
  else
    maybe_run gcloud -q iam service-accounts delete $saemail
  fi
}

function delete_bq_table {
  dataset=$1
  table_name=$2
  sql_result=$(list_bq_table $1 $2)
  echo "Deleting BQ table: '${dataset}.${table_name}'" 
  if [[ "$sql_result" == *"1"* ]]; then
    maybe_run bq rm -f -t $PROJECT:$dataset.$table_name
  else
    echo "${dataset}.${table_name} doesn't exists."
  fi  
}

function delete_bq_sp {
  dataset=$1
  sp_name=$2
  sql_result=$(list_bq_sp $1 $2)
  echo "Deleting BQ Stored Proc: '${dataset}.${sp_name}'" 
  if [[ "$sql_result" == *"1"* ]]; then
    maybe_run bq rm -f -routine $PROJECT:$dataset.$sp_name
  else
    echo "${dataset}.${sp_name} doesn't exists.."
  fi  
}

function delete_composer {
  composer_name=$1
  RETVAL=$( gcloud composer environments describe $composer_name --location=$COMPOSER_LOCATION 2>&1)
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo "${composer_name} doesn't exists."
  else
    echo "Deleting composer..."
    # delete the associated cloud storage account
    RETVAL=$( get_composer_storage_account $composer_name)
    echo $RETVAL
    if test -z "$RETVAL"; then
      echo "${composer_name} storage account doesn't exists."
    else
      echo "Delete ${RETVAL} manually after backing up the DAG folder"
    fi
    maybe_run gcloud composer environments delete $composer_name \
    --location $COMPOSER_LOCATION \
    --quiet
  fi
}

function delete_composer_dag {
  composer_name=$1
  RETVAL=$( gcloud composer environments describe $composer_name --location=$COMPOSER_LOCATION 2>&1)
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo "${composer_name} doesn't exists."
  else
    # delete the associated cloud storage account
    RETVAL=$( get_composer_storage_account $composer_name)
    echo $RETVAL
    if test -z "$RETVAL"; then
      echo "${composer_name} storage account doesn't exists."
    else
      echo "Deleting ${SA360_PUSH_CONVERSION_PY_FILE} and ${COMPOSER_DAG_FILE}..."
      maybe_run gcloud composer environments storage \
        dags delete gs://${RETVAL}/dags/${SA360_PUSH_CONVERSION_PY_FILE} \
        --environment=${composer_name} \
        --location=${COMPOSER_LOCATION} \
        --quiet
      #backup when composer takes a long time to delete; handy in development phase
      maybe_run gsutil rm gs://${RETVAL}/dags/${SA360_PUSH_CONVERSION_PY_FILE}
      maybe_run gcloud composer environments storage \
        dags delete gs://${RETVAL}/dags/${COMPOSER_DAG_FILE} \
        --environment=${composer_name} \
        --location=${COMPOSER_LOCATION} \
        --quiet
      #backup when composer takes a long time to delete; handy in development phase
      maybe_run gsutil rm gs://${RETVAL}/dags/${COMPOSER_DAG_FILE}
    fi
  fi
}

function list_storage_account {
  bucket=$1
  maybe_run gsutil ls -p ${PROJECT} gs://${PROJECT}-${bucket}
}

function list_bq_ds {
  dataset=$1
  maybe_run bq --project_id=${PROJECT} show --dataset ${dataset}
}

function list_bq_table {
  dataset=$1
  table_name=$2
  echo "Checking BQ table exist: '${dataset}.${table_name}'" 
  sql_query='SELECT
    COUNT(1) AS cnt
  FROM 
    `<myproject>`.<mydataset>.__TABLES_SUMMARY__
  WHERE table_id = "<mytable_name>"'
  sql_query="${sql_query/<myproject>/${PROJECT}}"
  sql_query="${sql_query/<mydataset>/${dataset}}"
  sql_query="${sql_query/<mytable_name>/${table_name}}"

  bq_qry_cmd="bq query --use_legacy_sql=false --format=csv '<mysql_qery>'"
  bq_qry_cmd="${bq_qry_cmd/<mysql_qery>/${sql_query}}"
  sql_result=$(eval $bq_qry_cmd)  
  if [[ "$sql_result" == *"1"* ]]; then
    echo "${dataset}.${table_name} exist"
    echo "1"
  else
    echo "${dataset}.${table_name} doesn't exist"
    echo "0"
  fi   
}

function list_bq_sp {
  dataset=$1
  sp_name=$2
  echo "Checking BQ SP exist: '${dataset}.${sp_name}'" 
  sql_query='SELECT
    COUNT(1) AS cnt
  FROM 
    `<myproject>.<mydataset>`.INFORMATION_SCHEMA.ROUTINES
  WHERE routine_name = "<sp_name>"'
  sql_query="${sql_query/<myproject>/${PROJECT}}"
  sql_query="${sql_query/<mydataset>/${dataset}}"
  sql_query="${sql_query/<sp_name>/${sp_name}}"

  bq_qry_cmd="bq query --use_legacy_sql=false --format=csv '<mysql_qery>'"
  bq_qry_cmd="${bq_qry_cmd/<mysql_qery>/${sql_query}}"
  sql_result=$(eval $bq_qry_cmd)  
  if [[ "$sql_result" == *"1"* ]]; then
    echo "${dataset}.${sp_name} exist"
    echo "1"
  else
    echo "${dataset}.${sp_name} doesn't exist"
    echo "0"
  fi   
}

function list_cloud_composer {
  composer_name=$1
  RETVAL=$( gcloud composer environments describe $composer_name --location=$COMPOSER_LOCATION 2>&1)
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo "${composer_name} doesn't exists."
  else
    echo "${composer_name} exists."
  fi
}

# set the gcp project
maybe_run gcloud config set project $PROJECT

if [ ! -z ${ADMIN} ]; then
  _ADMIN="ADMINISTRATOR_EMAIL=${ADMIN}"
fi

if [ ${ACTIVATE_APIS} -eq 1 ]; then
  # Check for active APIs
  echo "Activating APIs"
  APIS_USED=(
    "bigquery"
    "bigquerystorage"
    "bigquerydatatransfer"
    "doubleclickbidmanager"
    "doubleclicksearch"
    "storage-api"
    "composer"
  )
  ACTIVE_SERVICES="$(gcloud --project=${PROJECT} services list --enabled '--format=value(config.name)')"

  for api in ${APIS_USED[@]}; do
    if [[ "${ACTIVE_SERVICES}" =~ ${api} ]]; then
      echo "${api} already active"
    else
      echo "Activating ${api}"
      maybe_run gcloud --project=${PROJECT} services enable ${api}.googleapis.com
    fi
  done
fi

# create service account
if [ ${CREATE_SERVICE_ACCOUNT} -eq 1 ]; then
  create_service_account
fi

# create cloud storage bucket
if [ ${DEPLOY_STORAGE} -eq 1 ]; then
  # Create buckets
  echo "Creating buckets"
  create_storage_account $STORAGE_LOGS
  create_storage_account $STORAGE_PROFIT
fi

# create bq datasets
if [ ${DEPLOY_BQ} -eq 1 ]; then
  # Create BQ Datasets
  echo "Creating BQ Datasets"
  create_bq_ds $DS_SA360
  create_bq_ds $DS_GMC
  create_bq_ds $DS_BUSINESS_DATA
fi

# upload profit data
if [ ${DEPLOY_PROFIT_DATA} -eq 1 ]; then
  echo "Provisioning profit data"
  # check if the profit data file is available
  if [ -f $CLIENT_MARGIN_DATA_FILE_WITH_PATH ]; then
    # check the storage account
    create_storage_account $STORAGE_PROFIT
    # check the bq dataset
    create_bq_ds $DS_BUSINESS_DATA
    # upload the data file into storage bucket
    maybe_run gsutil cp $CLIENT_MARGIN_DATA_FILE_WITH_PATH gs://${PROJECT}-${STORAGE_PROFIT}
    # load the profit data
    load_bq_table $DS_BUSINESS_DATA $CLIENT_MARGIN_DATA_TABLE_NAME "client_profit.csv" "autodetect"
  else
    echo "$CLIENT_MARGIN_DATA_FILE_WITH_PATH doesn't exist!"
    profit_data_usage
  fi
fi

# Deploys the SQL Stored proc
if [ ${DEPLOY_SQL_TRANSFORM} -eq 1 ]; then
  pushd sql
  # prepare the sql file
  prepare_sql_sp
  # create the sp
  create_bq_sp $DS_BUSINESS_DATA $SQL_TRANSFORM_DATA_WRANGLIN_SP $CLIENT_MARGIN_DATA_TABLE_NAME
  popd
fi


# Deploys the Cloud Composer
if [ ${DEPLOY_COMPOSER} -eq 1 ]; then

  # Grant ServiceAgentV3Ext to service-PROJECT_NUMBER service account to avoid error
  PROJECT_NUMBER=$(gcloud projects list --filter="$(gcloud config get-value project)" --format="value(PROJECT_NUMBER)")
  COMPOSER_API_SERVICE_AGENT="service-${PROJECT_NUMBER}@cloudcomposer-accounts.iam.gserviceaccount.com"

  maybe_run gcloud projects add-iam-policy-binding $PROJECT \
      --member serviceAccount:$COMPOSER_API_SERVICE_AGENT \
      --role roles/composer.ServiceAgentV2Ext

  # provison a cloud composer environment if doesn't exists
  create_composer $COMPOSER_NAME $SA_EMAIL
  # deploy the code and the dag
  deploy_code_dag $COMPOSER_NAME
  # trigger dag
  maybe_run gcloud composer environments run $COMPOSER_NAME \
    --location $COMPOSER_LOCATION \
    dags trigger -- $DAG_ID  
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo "if errors out, then use the AirFlow UI to trigger the DAG ${DAG_ID}"
  fi

fi

# Deployes the test data and the code
if [ ${DEPLOY_TEST_MODULE} -eq 1 ]; then
  pushd ../solution_test
  # create all the prerequsites 
  echo "PREQUISITES!!!! - Deploy the solution first. \
      Sored Proc, DAG and data will be overwritten."
  # create campaign table
  # load test data to campaign table
  load_bq_table $DS_SA360 $CAMPAIGN_TABLE_NAME "p_Campaign_${SQL_TRANSFORM_ADVERTISER_ID}.csv" "p_Campaign_schema.json"
  # create conversion table
  # load test data to conversion
  load_bq_table $DS_SA360 $CONVERSION_TABLE_NAME "p_Conversion_${SQL_TRANSFORM_ADVERTISER_ID}.csv" "p_Conversion_schema.json"
  # load test profit data
  load_bq_table $DS_BUSINESS_DATA $CLIENT_MARGIN_DATA_TABLE_NAME "client_profit.csv" "autodetect"
  popd
  
  pushd sql
  # prepare the sql file
  prepare_sql_sp
  # create the sp
  create_bq_sp $DS_BUSINESS_DATA $SQL_TRANSFORM_DATA_WRANGLIN_SP $CLIENT_MARGIN_DATA_TABLE_NAME
  popd
fi

# deletes the solution
if [ ${DELETE_SOLUTION} -eq 1 ]; then
  echo "ALERT!!!! - Deletes all the GCP Services created for the solution!"
  delete_composer_dag $COMPOSER_NAME #delets only the dag and not the storage account
  # delete_composer $COMPOSER_NAME
  # delete_bq_ds $DS_SA360
  # delete_bq_ds $DS_GMC
  # delete_bq_ds $DS_BUSINESS_DATA  
  # delete_storage_account $STORAGE_LOGS
  # delete_storage_account $STORAGE_PROFIT
  # delete_service_account $SA_EMAIL
fi

# lists the soluiton
if [ ${LIST_SOLUTION} -eq 1 ]; then
  maybe_run gcloud iam service-accounts describe $SA_EMAIL
  list_bq_ds $DS_SA360
  list_bq_ds $DS_GMC
  list_bq_ds $DS_BUSINESS_DATA  
  list_storage_account $STORAGE_LOGS
  list_storage_account $STORAGE_PROFIT
  list_bq_sp $DS_BUSINESS_DATA $SQL_TRANSFORM_DATA_WRANGLIN_SP
  list_cloud_composer $COMPOSER_NAME
fi

# lists the test module 
if [ ${LIST_TEST_MODULE} -eq 1 ]; then
  list_bq_table $DS_SA360 $CAMPAIGN_TABLE_NAME
  list_bq_table $DS_SA360 $CONVERSION_TABLE_NAME
  list_bq_table $DS_BUSINESS_DATA $CLIENT_MARGIN_DATA_TABLE_NAME
  list_cloud_composer $COMPOSER_NAME
fi

# deletes the test module
if [ ${DELETE_TEST_MODULE} -eq 1 ]; then
  echo "ALERT!!!! - Deletes test module of the solution. \
    You need to reload your data. \
    You also need to redeploy the stored proc.
    "
  delete_bq_table $DS_SA360 $CAMPAIGN_TABLE_NAME 
  delete_bq_table $DS_SA360 $CONVERSION_TABLE_NAME 
  delete_bq_table $DS_BUSINESS_DATA $CLIENT_MARGIN_DATA_TABLE_NAME
  delete_bq_sp $DS_BUSINESS_DATA $SQL_TRANSFORM_DATA_WRANGLIN_SP  
  delete_composer_dag $COMPOSER_NAME
fi

echo 'Script ran successfully!'