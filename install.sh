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
  --dataset         The Big Query dataset to verify or create
Deployment directives:
  --activate-apis   Activate all missing but required Cloud APIs
  --create-service-account
                    Create the service account and client secrets
  --deploy-all Deploy all services
  --deploy-bigquery  Create BQ datasets
  --deploy-storage   Create storage buckets
  --deploy-delegator Create delegator cloud function
  --deploy-cm360-function Create cm360 cloud function

General switches:
  --dry-run         Don't do anything, just print the commands you would otherwise run. Useful
                    for testing.
EOF
}

function join { local IFS="$1"; shift; echo "$*"; }

# Switch definitions
PROJECT=
USER=
DATASET="profitbidder"

ACTIVATE_APIS=0
BACKGROUND=0
CREATE_SERVICE_ACCOUNT=0
USERNAME=0
ADMIN=
SERVICE_ACCOUNT_NAME="profit-bidder"
STORAGE_BUCKET_NAME="conversion-upload_log"
DEPLOY_BQ=0
DEPLOY_CM360_FUNCTION=0
DEPLOY_DELEGATOR=0
DEPLOY_STORAGE=0
DRY_RUN=""
VERBOSE=false

# Command line parser
while [[ ${1:-} == -* ]] ; do
  case $1 in
    --project*)
      IFS="=" read _cmd PROJECT <<< "$1" && [ -z ${PROJECT} ] && shift && PROJECT=$1
      ;;
    --dataset*)
      IFS="=" read _cmd DATASET <<< "$1" && [ -z ${DATASET} ] && shift && DATASET=$1
      ;;
    --deploy-all)
      DEPLOY_BQ=1
      DEPLOY_STORAGE=1
      DEPLOY_DELEGATOR=1
      DEPLOY_CM360_FUNCTION=1
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

function maybe-run {
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
      maybe-run gcloud --project=${PROJECT} services enable ${api}.googleapis.com
    fi
  done
fi

# create service account
SA_EMAIL=${SERVICE_ACCOUNT_NAME}@${PROJECT}.iam.gserviceaccount.com
if [ ${CREATE_SERVICE_ACCOUNT} -eq 1 ]; then
  if !gcloud iam service-accounts describe $SA_EMAIL &> /dev/null; then 
    echo "Creating service account '${SERVICE_ACCOUNT_NAME}'"
    maybe-run gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} --description 'Profit Bidder Service Account' --project ${PROJECT}
  fi
fi


# create cloud storage bucket
if [ ${DEPLOY_STORAGE} -eq 1 ]; then
  # Create buckets
  echo "Creating buckets"
  for bucket in ${STORAGE_BUCKET_NAME}; do
    gsutil ls -p ${PROJECT} gs://${PROJECT}-${bucket} > /dev/null 2>&1
    RETVAL=$?
    if (( ${RETVAL} != "0" )); then
      maybe-run gsutil mb -p ${PROJECT} gs://${PROJECT}-${bucket}
    fi
  done
fi

# create bq datasets
if [ ${DEPLOY_BQ} -eq 1 ]; then
  echo "Creating BQ datasets"
  # Create dataset
  echo "Creating datasets"  
  for dataset in sa360_data gmc_data business_data; do
    echo "Creating BQ dataset: '${dataset}'" 
    RETVAL=$?
    if ! bq --project_id=${PROJECT} show --dataset ${dataset} > /dev/null 2>&1; then
      maybe-run bq --project_id=${PROJECT} mk --dataset ${dataset}
    fi
  done
fi

echo "Deploy Delegator: ${DEPLOY_DELEGATOR}"
# create cloud funtions
if [ ${DEPLOY_DELEGATOR} -eq 1 ]; then
  echo "Creating Cloud functions"
 # Create scheduled job
  maybe-run gcloud beta scheduler jobs delete \
    --location=us-central \
    --project=${PROJECT} \
    --quiet \
    "delegator-scheduler" || echo "No job to delete" 

  maybe-run gcloud beta scheduler jobs create pubsub \
    "delegator" \
    --location=us-central \
    --schedule="0 6 * * *" \
    --topic="conversion_upload_delegator" \
    --message-body="RUN" \
    --project=${PROJECT} || echo "scheduler failed!"

  echo "Deploying Delegator Cloud Function"
  pushd converion_upload_delegator
  maybe-run gcloud functions deploy "cloud_conversion_upload_delegator" \
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
  maybe-run gcloud beta scheduler jobs delete \
    --location=us-central \
    --project=${PROJECT} \
    --quiet \
    "cm360-scheduler" || echo "No job to delete"

  maybe-run gcloud beta scheduler jobs create pubsub \
    "cm360-scheduler" \
    --location=us-central \
    --schedule="0 6 * * *" \
    --topic="conversion_upload_delegator" \
    --message-body="RUN" \
    --project=${PROJECT} || echo "scheduler failed!"

  echo "Deploying CM360 Cloud Function"
  pushd SA360_cloud_converion_upload_node
  maybe-run gcloud functions deploy "cm360_cloud_conversion_upload_node" \
    --region=us-central1 \
    --project=${PROJECT} \
    --trigger-topic=cm360_conversion_upload \
    --memory=256MB \
    --timeout=540s \
    --runtime python37 \
    --entry-point=main
  popd
fi

echo 'Script ran successfully!'
# NOTES:
# More automation may be needed?
# * creation of pubsub topiocs
# If gcloud scheduler fails, the user should be instructed to setup the scheduler in the cloud console
