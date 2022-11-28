#!/bin/bash -eu

# Copyright 2021 Google LLC
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
install.sh
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
  --delegator-pubsub-topic  PubSub topic name for the Delegator processing
  --cf-region               Google Cloud Region
  --cf-delegator            Cloud Function name for the Delegator processing logic
  --scheduler_delegator     Scheduler name for the Delegator CF
  --cm360-pubsub-topic      PubSub topic name for the CM360 processing
  --cf-cm360                Cloud Function name for the CM360 processing logic
Deployment directives:
  --activate-apis     Activate all missing but required Cloud APIs
  --create-service-account
                      Create the service account and client secrets
  --deploy-all        Deploy all services
  --deploy-bigquery   Create BQ datasets
  --deploy-storage    Create storage buckets
  --deploy-delegator  Create delegator cloud function
  --deploy-cm360-function Create cm360 cloud function
  --deploy-profit-data Upload and create client_margin_data_table (*format mentioned below)
  --deploy-sql-transform Creates a BQ job scheduler 
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
  --delegator-pubsub-topic=my_delegator_topic \
  --cf-region=us-central1 \
  --cf-delegator=my_cf_delegator \
  --scheduler_delegator=my_scheduler_delegator \
  --cm360-pubsub-topic=my_cm360_topic \
  --cf-cm360=my_cf_cm360 \

EOF

}

# Provisioning and deprovisioning Unit test
# =========================================
# mkdir -p $HOME/solutions/profit-bidder
# cd $HOME/solutions/profit-bidder
# git clone https://github.com/google/profit-bidder.git .
# sh install.sh --deploy-all --project=<project_id>
# sh install.sh --list-all --project=<project_id>
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
DELEGATOR_PUBSUB_TOPIC_NAME=$SOLUTION_PREFIX"conversion_upload_delegator"
CF_DELEGATOR=$SOLUTION_PREFIX"cloud_conversion_upload_delegator"
SCHEDULER_DELGATOR=$SOLUTION_PREFIX"delegator-scheduler"
CF_CM360=$SOLUTION_PREFIX"cm360_cloud_conversion_upload_node"
CM360_PUBSUB_TOPIC_NAME=$SOLUTION_PREFIX"cm360_conversion_upload"
CF_REGION="us-central1"
SA_ROLES="roles/bigquery.dataViewer roles/pubsub.publisher roles/iam.serviceAccountTokenCreator"

CM360_TABLE="my_transformed_data"
CM360_PROFILE_ID="my_cm_profileid"
CM360_FL_ACTIVITY_ID="my_fl_activity_id"
CM360_FL_CONFIG_ID="my_fl_config_id"

ACTIVATE_APIS=0
CREATE_SERVICE_ACCOUNT=0
DEPLOY_BQ=0
DEPLOY_CM360_FUNCTION=0
DEPLOY_DELEGATOR=0
DEPLOY_STORAGE=0
DEPLOY_PROFIT_DATA=0
DEPLOY_SQL_TRANSFORM=0
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
    --cf-delegator*)
      IFS="=" read _cmd CF_DELEGATOR <<< "$1" && [ -z ${CF_DELEGATOR} ] && shift && CF_DELEGATOR=$1
      ;;
    --scheduler_delegator*)
      IFS="=" read _cmd SCHEDULER_DELGATOR <<< "$1" && [ -z ${SCHEDULER_DELGATOR} ] && shift && SCHEDULER_DELGATOR=$1
      ;;
    --delegator-pubsub-topic*)
      IFS="=" read _cmd DELEGATOR_PUBSUB_TOPIC_NAME <<< "$1" && [ -z ${DELEGATOR_PUBSUB_TOPIC_NAME} ] && shift && DELEGATOR_PUBSUB_TOPIC_NAME=$1
      ;;
    --cf-cm360*)
      IFS="=" read _cmd CF_CM360 <<< "$1" && [ -z ${CF_CM360} ] && shift && CF_CM360=$1
      ;;
    --cm360-pubsub-topic*)
      IFS="=" read _cmd CM360_PUBSUB_TOPIC_NAME <<< "$1" && [ -z ${CM360_PUBSUB_TOPIC_NAME} ] && shift && CM360_PUBSUB_TOPIC_NAME=$1
      ;;
    --cf-region*)
      IFS="=" read _cmd CF_REGION <<< "$1" && [ -z ${CF_REGION} ] && shift && CF_REGION=$1
      ;;
    --deploy-all)
      DEPLOY_BQ=1
      DEPLOY_STORAGE=1
      DEPLOY_DELEGATOR=1
      DEPLOY_CM360_FUNCTION=1
      DEPLOY_PROFIT_DATA=1      
      ACTIVATE_APIS=1
      CREATE_SERVICE_ACCOUNT=1
      DEPLOY_SQL_TRANSFORM=1
      ;;
    --deploy-bigquery)
      DEPLOY_BQ=1
      ;;
    --deploy-storage)
      DEPLOY_STORAGE=1
      ;;
    --deploy-delegator)
      DEPLOY_DELEGATOR=1
      ;;
    --deploy-cm360-function)
      DEPLOY_CM360_FUNCTION=1
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

CAMPAIGN_TABLE_NAME="p_Campaign_"$SQL_TRANSFORM_ADVERTISER_ID
CONVERSION_TABLE_NAME="p_Conversion_"$SQL_TRANSFORM_ADVERTISER_ID

SQL_TRANSFORM_SCHEDULED_QUERY_DISPLAY_NAME=$SOLUTION_PREFIX'Profit Bidder Scheduler query'
SQL_TRANSFORM_SCHEDULED_QUERY_SCHEUDLE='every 1 hours'

# comply the name and formulate the sa email account
SERVICE_ACCOUNT_NAME=${SERVICE_ACCOUNT_NAME//_/-}
SA_EMAIL=${SERVICE_ACCOUNT_NAME}@${PROJECT}.iam.gserviceaccount.com

function cm360_json {
cat <<EOF
{
  "dataset_name": "${DS_BUSINESS_DATA}",
  "table_name": "${CM360_TABLE}",
  "topic": "$CM360_PUBSUB_TOPIC_NAME",
  "cm360_config": {
    "profile_id": "${CM360_PROFILE_ID}",
    "floodlight_activity_id": "${CM360_FL_ACTIVITY_ID}",
    "floodlight_configuration_id": "${CM360_FL_CONFIG_ID}"
  }
}
EOF
}

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

function create_cloud_function {
  cf_name=$1
  mem=$2
  trigger_topic=$3
  echo "Creating Cloud Function: $cf_name"
  gcloud functions describe $cf_name > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    maybe_run gcloud functions deploy "$cf_name" \
    --region=${CF_REGION} \
    --project=${PROJECT} \
    --trigger-topic=$trigger_topic \
    --memory=$2 \
    --timeout=540s \
    --runtime python39 \
    --update-env-vars="SA_EMAIL=${SA_EMAIL},TIMEZONE=${SQL_TRANSFORM_TIMEZONE},GCP_PROJECT=${PROJECT}" \
    --update-labels="deploy_timestamp=$(deploy_timestamp)" \
    --service-account $SA_EMAIL \
    --entry-point=main 
  else
    echo "Reusing ${cf_name}."
  fi
}

function create_scheduler {
  scheduler_name=$1
  topic=$2
  message_body=$3
  echo "Creating Scheduler: $scheduler_name"
  gcloud scheduler jobs describe $scheduler_name > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    # maybe_run gcloud scheduler jobs create pubsub \
    #   "$scheduler_name" \
    #   ----location=$CF_REGION \
    #   --schedule="'0 6 * * *'" \
    #   --topic="$topic" \
    #   --message-body=\'$3\'
    if [ "${DRY_RUN:-}" = "echo" ]; then
        echo gcloud scheduler jobs create pubsub $scheduler_name --location=$CF_REGION --schedule="0 15 * * *" --topic=$topic --message-body="$message_body"
    else
        if [ "$VERBOSE" = "true" ]; then
            echo $scheduler_cmd
        fi
        gcloud scheduler jobs create pubsub $scheduler_name --location=$CF_REGION --schedule="0 15 * * *" --topic=$topic --message-body="'$message_body'"
    fi
    
  else
    echo "Reusing ${scheduler_name}."
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

function create_sql_transform_file {
  echo "Going to create profit_gen_query.sql file."
  if [ -f "profit_gen_query.sql" ]; then
    rm -rf profit_gen_query.sql
  fi
  maybe_run cp profit_gen_query_template.sql profit_gen_query.sql 
  os_type=$(uname -a)
  if [[ "$os_type" == *"Linux"* ]]; then
    maybe_run sed -i "s|<project_id>|$SQL_TRANSFORM_PROJECT_ID|" profit_gen_query.sql
    maybe_run sed -i "s|<sa360_dataset_name>|$SQL_TRANSFORM_SA360_DATASET_NAME|" profit_gen_query.sql
    maybe_run sed -i "s|<advertiser_id>|$SQL_TRANSFORM_ADVERTISER_ID|" profit_gen_query.sql
    maybe_run sed -i "s|<timezone>|$SQL_TRANSFORM_TIMEZONE|" profit_gen_query.sql
    maybe_run sed -i "s|<floodlight_name>|$SQL_TRANSFORM_SOURCE_FLOODLIGHT_NAME|" profit_gen_query.sql
    maybe_run sed -i "s|<account_type>|$SQL_TRANSFORM_ACCOUNT_TYPE|" profit_gen_query.sql
    maybe_run sed -i "s|<gmc_dataset_name>|$SQL_TRANSFORM_GMC_DATASET_NAME|" profit_gen_query.sql
    maybe_run sed -i "s|<gmc_account_id>|$SQL_TRANSFORM_GMC_ACCOUNT_ID|" profit_gen_query.sql
    maybe_run sed -i "s|<business_dataset_name>|$SQL_TRANSFORM_BUSINESS_DATASET_NAME|" profit_gen_query.sql
    maybe_run sed -i "s|<client_margin_data_table>|$SQL_TRANSFORM_CLIENT_MARGIN_DATA_TABLE|" profit_gen_query.sql
    maybe_run sed -i "s|<client_profit_data_sku_col>|$SQL_TRANSFORM_CLIENT_PROFIT_DATA_SKU_COL|" profit_gen_query.sql
    maybe_run sed -i "s|<client_profit_data_profit_col>|$SQL_TRANSFORM_CLIENT_PROFIT_DATA_PROFIT_COL|" profit_gen_query.sql
    maybe_run sed -i "s|<target_floodlight_name>|$SQL_TRANSFORM_TARGET_FLOODLIGHT_NAME|" profit_gen_query.sql
    maybe_run sed -i "s|<product_sku_var>|$SQL_TRANSFORM_PRODUCT_SKU_VAR|" profit_gen_query.sql
    maybe_run sed -i "s|<product_sku_regex>|$SQL_TRANSFORM_PRODUCT_SKU_REGEX|" profit_gen_query.sql
    maybe_run sed -i "s#<product_sku_delim>#$SQL_TRANSFORM_PRODUCT_SKU_DELIM#" profit_gen_query.sql
    maybe_run sed -i "s|<product_quantity_var>|$SQL_TRANSFORM_PRODUCT_QUANTITY_VAR|" profit_gen_query.sql
    maybe_run sed -i "s|<product_quantity_regex>|$SQL_TRANSFORM_PRODUCT_QUANTITY_REGEX|" profit_gen_query.sql
    maybe_run sed -i "s#<product_quantity_delim>#$SQL_TRANSFORM_PRODUCT_QUANTITY_DELIM#" profit_gen_query.sql
    maybe_run sed -i "s|<product_unit_price_var>|$SQL_TRANSFORM_PRODUCT_UNIT_PRICE_VAR|" profit_gen_query.sql
    maybe_run sed -i "s|<product_unit_price_regex>|$SQL_TRANSFORM_PRODUCT_UNIT_PRICE_REGEX|" profit_gen_query.sql
    maybe_run sed -i "s#<product_unit_price_delim>#$SQL_TRANSFORM_PRODUCT_UNIT_PRICE_DELIM#" profit_gen_query.sql
    if [ ${DEPLOY_TEST_MODULE} -ne 1 ]; then
        maybe_run sed -i "s|--<test>||" profit_gen_query.sql
    fi
  else
    # below works in the shell of Mac
    maybe_run sed -i "" "s|<project_id>|$SQL_TRANSFORM_PROJECT_ID|" profit_gen_query.sql
    maybe_run sed -i "" "s|<sa360_dataset_name>|$SQL_TRANSFORM_SA360_DATASET_NAME|" profit_gen_query.sql
    maybe_run sed -i "" "s|<advertiser_id>|$SQL_TRANSFORM_ADVERTISER_ID|" profit_gen_query.sql
    maybe_run sed -i "" "s|<timezone>|$SQL_TRANSFORM_TIMEZONE|" profit_gen_query.sql
    maybe_run sed -i "" "s|<floodlight_name>|$SQL_TRANSFORM_SOURCE_FLOODLIGHT_NAME|" profit_gen_query.sql
    maybe_run sed -i "" "s|<account_type>|$SQL_TRANSFORM_ACCOUNT_TYPE|" profit_gen_query.sql
    maybe_run sed -i "" "s|<gmc_dataset_name>|$SQL_TRANSFORM_GMC_DATASET_NAME|" profit_gen_query.sql
    maybe_run sed -i "" "s|<gmc_account_id>|$SQL_TRANSFORM_GMC_ACCOUNT_ID|" profit_gen_query.sql
    maybe_run sed -i "" "s|<business_dataset_name>|$SQL_TRANSFORM_BUSINESS_DATASET_NAME|" profit_gen_query.sql
    maybe_run sed -i "" "s|<client_margin_data_table>|$SQL_TRANSFORM_CLIENT_MARGIN_DATA_TABLE|" profit_gen_query.sql
    maybe_run sed -i "" "s|<client_profit_data_sku_col>|$SQL_TRANSFORM_CLIENT_PROFIT_DATA_SKU_COL|" profit_gen_query.sql
    maybe_run sed -i "" "s|<client_profit_data_profit_col>|$SQL_TRANSFORM_CLIENT_PROFIT_DATA_PROFIT_COL|" profit_gen_query.sql
    maybe_run sed -i "" "s|<target_floodlight_name>|$SQL_TRANSFORM_TARGET_FLOODLIGHT_NAME|" profit_gen_query.sql
    maybe_run sed -i "" "s|<product_sku_var>|$SQL_TRANSFORM_PRODUCT_SKU_VAR|" profit_gen_query.sql
    maybe_run sed -i "" "s|<product_sku_regex>|$SQL_TRANSFORM_PRODUCT_SKU_REGEX|" profit_gen_query.sql
    maybe_run sed -i "" "s#<product_sku_delim>#$SQL_TRANSFORM_PRODUCT_SKU_DELIM#" profit_gen_query.sql
    maybe_run sed -i "" "s|<product_quantity_var>|$SQL_TRANSFORM_PRODUCT_QUANTITY_VAR|" profit_gen_query.sql
    maybe_run sed -i "" "s|<product_quantity_regex>|$SQL_TRANSFORM_PRODUCT_QUANTITY_REGEX|" profit_gen_query.sql
    maybe_run sed -i "" "s#<product_quantity_delim>#$SQL_TRANSFORM_PRODUCT_QUANTITY_DELIM#" profit_gen_query.sql
    maybe_run sed -i "" "s|<product_unit_price_var>|$SQL_TRANSFORM_PRODUCT_UNIT_PRICE_VAR|" profit_gen_query.sql
    maybe_run sed -i "" "s|<product_unit_price_regex>|$SQL_TRANSFORM_PRODUCT_UNIT_PRICE_REGEX|" profit_gen_query.sql
    maybe_run sed -i "" "s#<product_unit_price_delim>#$SQL_TRANSFORM_PRODUCT_UNIT_PRICE_DELIM#" profit_gen_query.sql
    if [ ${DEPLOY_TEST_MODULE} -ne 1 ]; then
      maybe_run sed -i "" "s|--<test>||" profit_gen_query.sql
    fi
  fi  
}

function create_sql_schedule_query {
  echo "Going to create a scheduled query."
  dataset=$1
  table_name=$2
  cat profit_gen_query.sql \
    | bq query \
        --display_name="Scheduled Query to get SA360 conversions with profit data for Profit Bidder" \
        --schedule="every day 13:00" \
        --project_id=$SQL_TRANSFORM_PROJECT_ID \
        --destination_table=$dataset'.'$table_name \
        --use_legacy_sql=False \
        --replace=True
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

function delete_cloud_function {
  cf_name=$1
  echo "Deleting Cloud Function: $cf_name"
  gcloud functions describe $cf_name > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo "$cf_name does not exist."
  else
    maybe_run gcloud -q functions delete $cf_name
  fi
}

function delete_scheduler {
  scheduler_name=$1
  echo "Deleting Scheduler: $scheduler_name"
  gcloud scheduler jobs describe $scheduler_name > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo "$scheduler_name does not exist."
  else
    maybe_run gcloud -q scheduler jobs delete $scheduler_name --location=$CF_REGION
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

function list_storage_account {
  bucket=$1
  maybe_run gsutil ls -p ${PROJECT} gs://${PROJECT}-${bucket}
}

function list_bq_ds {
  dataset=$1
  maybe_run bq --project_id=${PROJECT} show --dataset ${dataset}
}

function list_cloud_function {
  cf_name=$1
  maybe_run gcloud functions describe $cf_name --region=$CF_REGION 
}

function list_scheduler {
  scheduler_name=$1
  maybe_run gcloud scheduler jobs describe $scheduler_name --location=$CF_REGION
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
    "cloudbuild"
    "cloudfunctions"
    "cloudscheduler"
    "doubleclickbidmanager"
    "doubleclicksearch"
    "dfareporting"
    "pubsub"
    "storage-api"
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

# create cloud funtions
if [ ${DEPLOY_DELEGATOR} -eq 1 ]; then
  echo "Provisioning delegators"
  if [ -z "${CM360_TABLE}" ] || [ -z "${CM360_PROFILE_ID}" ] || [ -z "${CM360_FL_ACTIVITY_ID}" ] || [ -z "${CM360_FL_CONFIG_ID}" ]; then
    usage
    echo "\nYou must specify --cm360-table, --cm360-profile-id, --cm360-fl-activity-id,--cm360-fl-config-id to 'deploy-cm360-function'."
  else
    # check for the service account
    create_service_account
    pushd converion_upload_delegator
    create_cloud_function $CF_DELEGATOR "2GB" $DELEGATOR_PUBSUB_TOPIC_NAME
    popd
    create_scheduler $SCHEDULER_DELGATOR $DELEGATOR_PUBSUB_TOPIC_NAME "$(cm360_json)"
  fi
fi

if [ ${DEPLOY_CM360_FUNCTION} -eq 1 ]; then
  echo "Provisioning CM360 CF"
  # check for the service account
  create_service_account
  # check the storage account
  create_storage_account $STORAGE_LOGS
  pushd CM360_cloud_conversion_upload_node
  create_cloud_function $CF_CM360 "512MB" $CM360_PUBSUB_TOPIC_NAME
  popd
  if [ "$VERBOSE" = "true" ]; then
    echo
    echo
    echo "Delegator payload JSON:"
    cm360_json
    echo
    echo
  fi  
fi

# Deploys the SQL JOB scheduler
if [ ${DEPLOY_SQL_TRANSFORM} -eq 1 ]; then
  pushd sql_query
  # prepare the sql file
  create_sql_transform_file
  # schedule the sql job
  create_sql_schedule_query $DS_BUSINESS_DATA $CM360_TABLE
  popd
fi

# Deployes the test data and the code
if [ ${DEPLOY_TEST_MODULE} -eq 1 ]; then
  pushd solution_test
  # create all the prerequsites 
  echo "PREQUISITES!!!! - Deploy the solution first. \
      Scheulde query, CF and data will be overwritten."
  # create campaign table
  # load test data to campaign table
  load_bq_table $DS_SA360 $CAMPAIGN_TABLE_NAME "p_Campaign_${SQL_TRANSFORM_ADVERTISER_ID}.csv" "p_Campaign_schema.json"
  # create conversion table
  # load test data to conversion
  load_bq_table $DS_SA360 $CONVERSION_TABLE_NAME "p_Conversion_${SQL_TRANSFORM_ADVERTISER_ID}.csv" "p_Conversion_schema.json"
  # load test profit data
  load_bq_table $DS_BUSINESS_DATA $CLIENT_MARGIN_DATA_TABLE_NAME "client_profit.csv" "autodetect"
  popd
  
  pushd sql_query
  # prepare the sql file
  create_sql_transform_file
  # schedule the sql job
  create_sql_schedule_query $DS_BUSINESS_DATA $CM360_TABLE
  popd
fi

# deletes the solution
if [ ${DELETE_SOLUTION} -eq 1 ]; then
  echo "ALERT!!!! - Deletes all the GCP Services created for the solution!"
  delete_service_account $SA_EMAIL
  delete_bq_ds $DS_SA360
  delete_bq_ds $DS_GMC
  delete_bq_ds $DS_BUSINESS_DATA  
  delete_storage_account $STORAGE_LOGS
  delete_storage_account $STORAGE_PROFIT
  delete_cloud_function $CF_DELEGATOR
  delete_scheduler $SCHEDULER_DELGATOR
  delete_cloud_function $CF_CM360
fi

# lists the soluiton
if [ ${LIST_SOLUTION} -eq 1 ]; then
  maybe_run gcloud iam service-accounts describe $SA_EMAIL
  list_bq_ds $DS_SA360
  list_bq_ds $DS_GMC
  list_bq_ds $DS_BUSINESS_DATA  
  list_storage_account $STORAGE_LOGS
  list_storage_account $STORAGE_PROFIT
  list_cloud_function $CF_DELEGATOR
  list_cloud_function $CF_CM360
  list_scheduler $SCHEDULER_DELGATOR
fi

# lists the test module 
if [ ${LIST_TEST_MODULE} -eq 1 ]; then
  list_bq_table $DS_SA360 $CAMPAIGN_TABLE_NAME
  list_bq_table $DS_SA360 $CONVERSION_TABLE_NAME
  list_bq_table $DS_BUSINESS_DATA $CLIENT_MARGIN_DATA_TABLE_NAME
fi

# deletes the test module
if [ ${DELETE_TEST_MODULE} -eq 1 ]; then
  echo "ALERT!!!! - Deletes test module of the solution. \
    You need to reload your data. \
    You also need to redeploy the job scheduler and cfs.
    "
  delete_bq_table $DS_SA360 $CAMPAIGN_TABLE_NAME 
  delete_bq_table $DS_SA360 $CONVERSION_TABLE_NAME 
  delete_bq_table $DS_BUSINESS_DATA $CLIENT_MARGIN_DATA_TABLE_NAME
fi

echo 'Script ran successfully!'