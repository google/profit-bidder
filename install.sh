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
Options:
  --project         GCP Project Id
  --dataset-sa360   Big Query dataset for SA360 to verify or create
  --dataset-gmc     Big Query dataset for Google Merchant Center to verify or create
  --dataset-profit  Big Query dataset for Business profit data to verify or create
  --storage-logs    Storage Account to upload conversion logs
  --storage-profit  Storage Account to upload the profit data
  --profit-file     Filename along with path containing the profit data
Deployment directives:
  --activate-apis   Activate all missing but required Cloud APIs
  --create-service-account
                    Create the service account and client secrets
  --deploy-all Deploy all services
  --deploy-bigquery  Create BQ datasets
  --deploy-storage   Create storage buckets
  --deploy-delegator Create delegator cloud function
  --deploy-cm360-function Create cm360 cloud function
  --deploy-profit-data Upload and create client_margin_data_table
  --delete-solution Alert!-Deletes GCP resources. Limited amount of clean up activities.

General switches:
  --dry-run         Don't do anything, just print the commands you would otherwise run. Useful
                    for testing.

Example:
sh install.sh --dry-run --deploy-all --project=dpani-sandbox
sh install.sh --dry-run --deploy-profit-data  --project=dpani-sandbox --dataset-profit=my_profit_ds --storage-profit=my_profit_sa --profit-file=my/path/file.csv
sh install.sh --dry-run --deploy-all --project=dpani-sandbox --dataset-sa360=my_sa360  --dataset-gmc=my_gmc --dataset-profit=my_profit --storage-logs=my_con_log  --storage-profit=my_profit_csv

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

ACTIVATE_APIS=0
CREATE_SERVICE_ACCOUNT=0
DEPLOY_BQ=0
DEPLOY_CM360_FUNCTION=0
DEPLOY_DELEGATOR=0
DEPLOY_STORAGE=0
DEPLOY_PROFIT_DATA=0
DELETE_SOLUTION=0

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
    --delete-solution)
      DELETE_SOLUTION=1
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
  echo -e "\nYou must specify a project to proceed."
  exit
fi

function create_bq_ds {
  dataset=$1
  echo "Creating BQ dataset: '${dataset}'" 
  RETVAL=$?
  if ! bq --project_id=${PROJECT} show --dataset ${dataset} > /dev/null 2>&1; then
    maybe_run bq --project_id=${PROJECT} mk --dataset ${dataset}
  else
    echo "Resuing ${dataset}."
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
    echo "Resuing ${PROJECT}-${bucket}."
  fi
}

function delete_bq_ds {
  dataset=$1
  echo "Deleting BQ dataset: '${dataset}'" 
  RETVAL=$?
  if ! bq --project_id=${PROJECT} show --dataset ${dataset} > /dev/null 2>&1; then
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
  gcloud iam service-accounts describe $SA_EMAIL > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo "$saemail does not exist."
  else
  maybe_run gcloud -q iam service-accounts delete $saemail
  fi
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
  echo "Handling profit data"
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
    echo "$CLIENT_MARGIN_DATA_FILE doesn't exist!"
  fi
fi

# create cloud funtions
if [ ${DEPLOY_DELEGATOR} -eq 1 ]; then
  echo "Creating Cloud functions"
 # Create scheduled job
  maybe_run gcloud beta scheduler jobs delete \
    --location=us-central \
    --project=${PROJECT} \
    --quiet \
    "delegator-scheduler" || echo "No job to delete" 

  maybe_run gcloud beta scheduler jobs create pubsub \
    "delegator" \
    --location=us-central \
    --schedule="0 6 * * *" \
    --topic="conversion_upload_delegator" \
    --message-body="RUN" \
    --project=${PROJECT} || echo "scheduler failed!"

  echo "Deploying Delegator Cloud Function"
  pushd converion_upload_delegator
  maybe_run gcloud functions deploy "cloud_conversion_upload_delegator" \
    --region=us-central1 \
    --project=${PROJECT} \
    --trigger-topic=conversion_upload_delegator \
    --memory=2GB \
    --timeout=540s \
    --runtime python37 \
    --entry-point=main 
  popd
fi

if [ ${DEPLOY_CM360_FUNCTION} -eq 1 ]; then
  echo "Creating CM360 Cloud Function"
 # Create scheduled job
  maybe_run gcloud beta scheduler jobs delete \
    --location=us-central \
    --project=${PROJECT} \
    --quiet \
    "cm360-scheduler" || echo "No job to delete"

  maybe_run gcloud beta scheduler jobs create pubsub \
    "cm360-scheduler" \
    --location=us-central \
    --schedule="0 6 * * *" \
    --topic="conversion_upload_delegator" \
    --message-body="RUN" \
    --project=${PROJECT} || echo "scheduler failed!"

  echo "Deploying CM360 Cloud Function"
  pushd SA360_cloud_converion_upload_node
  maybe_run gcloud functions deploy "cm360_cloud_conversion_upload_node" \
    --region=us-central1 \
    --project=${PROJECT} \
    --trigger-topic=cm360_conversion_upload \
    --memory=256MB \
    --timeout=540s \
    --runtime python37 \
    --entry-point=main
  popd
fi

if [ ${DELETE_SOLUTION} -eq 1 ]; then
  echo "ALERT!!!! - Deletes all the GCP components!"
  delete_service_account $SA_EMAIL
  delete_bq_ds $DS_SA360
  delete_bq_ds $DS_SA360
  delete_bq_ds $DS_BUSINESS_DATA  
  delete_storage_account $STORAGE_LOGS
  delete_storage_account $STORAGE_PROFIT
fi

echo 'Script ran successfully!'
# NOTES:
# More automation may be needed?
# * creation of pubsub topiocs
# If gcloud scheduler fails, the user should be instructed to setup the scheduler in the cloud console
