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
Options with default values:  
  --service-account Service Account name
  --dataset-sa360   Big Query dataset for SA360 to verify or create
  --dataset-gmc     Big Query dataset for Google Merchant Center to verify or create
  --dataset-profit  Big Query dataset for Business profit data to verify or create
  --storage-logs    Storage Account to upload conversion logs
  --storage-profit  Storage Account to upload the profit data
  --profit-file     Filename along with path containing the profit data
  --delegator-pubsub-topic  PubSub topic name for the Delegator processing
  --cf-delegator            Cloud Function name for the Delegator processing logic
  --scheduler_delegator     Scheduler name for the Delegator CF
  --cm360-pubsub-topic      PubSub topic name for the CM360 processing
  --cf-cm360                Cloud Function name for the CM360 processing logic
  --scheduler_cm360         Scheduler name for the CM360 CF
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
General switches:
  --dry-run         Don't do anything, just print the commands you would otherwise run. Useful
                    for testing.
  --delete-all      Alert!-Deletes GCP resources. Useful for unit testing.
  --list-all        Lists all the GCP resources for the solution.

Example:
sh install.sh --dry-run --deploy-all --project=<project_id>
sh install.sh --dry-run --deploy-profit-data  --project=<project_id> --dataset-profit=my_profit_ds --storage-profit=my_profit_sa --profit-file=my/path/file.csv
sh install.sh --dry-run --deploy-all --project=<project_id> --dataset-sa360=my_sa360  --dataset-gmc=my_gmc --dataset-profit=my_profit --storage-logs=my_con_log  --storage-profit=my_profit_csv

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

function profit_data_usage {
  cat << EOF
CSV Format and sample values for the client_margin_data_table table:
Class_Name,Scored_Value
test1,50
test2,60.60
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
SCHEDULER_CM360=$SOLUTION_PREFIX"cm360-scheduler"

ACTIVATE_APIS=0
CREATE_SERVICE_ACCOUNT=0
DEPLOY_BQ=0
DEPLOY_CM360_FUNCTION=0
DEPLOY_DELEGATOR=0
DEPLOY_STORAGE=0
DEPLOY_PROFIT_DATA=0
DELETE_SOLUTION=0
LIST_SOLUTION=0

PROJECT=
USER=
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
    --scheduler_cm360*)
      IFS="=" read _cmd SCHEDULER_CM360 <<< "$1" && [ -z ${SCHEDULER_CM360} ] && shift && SCHEDULER_CM360=$1
      ;;
    --cm360-pubsub-topic*)
      IFS="=" read _cmd CM360_PUBSUB_TOPIC_NAME <<< "$1" && [ -z ${CM360_PUBSUB_TOPIC_NAME} ] && shift && CM360_PUBSUB_TOPIC_NAME=$1
      ;;
    --deploy-all)
      DEPLOY_BQ=1
      DEPLOY_STORAGE=1
      DEPLOY_DELEGATOR=1
      DEPLOY_CM360_FUNCTION=1
      DEPLOY_PROFIT_DATA=1      
      ACTIVATE_APIS=1
      CREATE_SERVICE_ACCOUNT=1
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

# comply the name and formulate the sa email account
SERVICE_ACCOUNT_NAME=${SERVICE_ACCOUNT_NAME/_/-}
SA_EMAIL=${SERVICE_ACCOUNT_NAME}@${PROJECT}.iam.gserviceaccount.com

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
      --region=us-central1 \
      --project=${PROJECT} \
      --trigger-topic=$trigger_topic \
      --memory=$2 \
      --timeout=540s \
      --runtime python37 \
      --entry-point=main 
  else
    echo "Reusing ${cf_name}."
  fi
}

function create_scheduler {
  scheduler_name=$1
  topic=$2
  echo "Creating Scheduler: $scheduler_name"
  gcloud scheduler jobs describe $scheduler_name > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    maybe_run gcloud beta scheduler jobs create pubsub \
      "$scheduler_name" \
      --schedule="0 6 * * *" \
      --topic="$topic" \
      --message-body="RUN" 
  else
    echo "Reusing ${scheduler_name}."
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
    maybe_run gcloud -q scheduler jobs delete $scheduler_name
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
  maybe_run gcloud functions describe $cf_name
}

function list_scheduler {
  scheduler_name=$1
  maybe_run gcloud scheduler jobs describe $scheduler_name
}

function replace_placehoder {
  echo "Replacing $1 with $2 in $3"
  placeholder=$1
  placeholder_value=$2
  file_name=$3
  maybe_run grep $1 $3
  maybe_run sed -i "" "s|$1|$2|" $3
  maybe_run grep $1 $3
  maybe_run grep $2 $3
}

USER=profit-bidder@${PROJECT}.iam.gserviceaccount.com
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
  echo "Creating service account $SA_EMAIL"
  gcloud iam service-accounts describe $SA_EMAIL > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    maybe_run gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} --description 'Profit Bidder Service Account' --project ${PROJECT}
  fi
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
    maybe_run gsutil cp $CLIENT_MARGIN_DATA_FILE_WITH_PATH gs://${PROJECT}-${bucket}
    # load the profit data
    maybe_run bq load \
    --autodetect \
    --source_format=CSV \
    $DS_BUSINESS_DATA.$CLIENT_MARGIN_DATA_TABLE_NAME \
    gs://${PROJECT}-${bucket}/$CLIENT_MARGIN_DATA_FILE_NAME
  else
    echo "$CLIENT_MARGIN_DATA_FILE_WITH_PATH doesn't exist!"
    profit_data_usage
  fi
fi

# create cloud funtions
if [ ${DEPLOY_DELEGATOR} -eq 1 ]; then
  echo "Provisioning delegators"
  pushd converion_upload_delegator
  #replace placeholder variable with project specific values
  replace_placehoder "replace-with-your-project-id" $PROJECT "main.py"
  create_cloud_function $CF_DELEGATOR "2GB" $DELEGATOR_PUBSUB_TOPIC_NAME
  popd
  create_scheduler $SCHEDULER_DELGATOR $DELEGATOR_PUBSUB_TOPIC_NAME
fi

if [ ${DEPLOY_CM360_FUNCTION} -eq 1 ]; then
  echo "Provisioning CM360 CF"
  # check the storage account
  create_storage_account $STORAGE_LOGS
  pushd SA360_cloud_converion_upload_node
  #replace placeholder variable with project specific values
  replace_placehoder "conversion_upload_log" $STORAGE_LOGS "main.py"
  replace_placehoder "your-service-account@your-project-name.iam.gserviceaccount.com" $SA_EMAIL "main.py"
  create_cloud_function $CF_CM360 "256MB" $CM360_PUBSUB_TOPIC_NAME
  popd
  create_scheduler $SCHEDULER_CM360 $CM360_PUBSUB_TOPIC_NAME
fi

if [ ${DELETE_SOLUTION} -eq 1 ]; then
  echo "ALERT!!!! - Deletes all the GCP components!"
  delete_service_account $SA_EMAIL
  delete_bq_ds $DS_SA360
  delete_bq_ds $DS_GMC
  delete_bq_ds $DS_BUSINESS_DATA  
  delete_storage_account $STORAGE_LOGS
  delete_storage_account $STORAGE_PROFIT
  delete_cloud_function $CF_DELEGATOR
  delete_scheduler $SCHEDULER_DELGATOR
  delete_cloud_function $CF_CM360
  delete_scheduler $SCHEDULER_CM360
fi

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
  list_scheduler $SCHEDULER_CM360
fi

echo 'Script ran successfully!'
# NOTES:
# More automation may be needed?
# * creation of pubsub topiocs
# If gcloud scheduler fails, the user should be instructed to setup the scheduler in the cloud console
