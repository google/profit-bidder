#!/usr/bin/python
#
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

import base64
import datetime
import google.auth
import google.auth.impersonated_credentials
import json
import pytz

from io import StringIO

import google_auth_httplib2
from googleapiclient import discovery
from googleapiclient import errors

from google.cloud import bigquery
from google.cloud import pubsub
from google.cloud import storage

GCS_BUCKET_NAME = 'conversion_upload_log'
IMPERSONATED_SVC_ACCOUNT = 'your-service-account@your-project-name.iam.gserviceaccount.com'
API_SCOPES = [
    'https://www.googleapis.com/auth/doubleclicksearch',
    'https://www.googleapis.com/auth/devstorage.read_write'
]
SA360_API_NAME = 'doubleclicksearch'
SA360_API_VERSION = 'v2'
# Defaults to America/New_York, please update to 
# your respective timezone if needed.
PROJECT_TIMEZONE = 'America/New_York'


def setup():
  source_credentials, project_id = google.auth.default()

  target_credentials = google.auth.impersonated_credentials.Credentials(
      source_credentials=source_credentials,
      target_principal=IMPERSONATED_SVC_ACCOUNT,
      target_scopes=API_SCOPES,
      delegates=[],
      lifetime=500)

  http = google_auth_httplib2.AuthorizedHttp(target_credentials)
  # setup API service here
  return discovery.build(
      SA360_API_NAME,
      SA360_API_VERSION,
      cache_discovery=False,
      http=http)

# Unused function but can be utilized to upload logs to Cloud Storage
def upload_log_blob(data_string, destination_blob_prefix):
  """Uploads a file to the bucket."""
  today = today_date()
  destination_log_file = '{}_upload_log_{}.txt'.format(destination_blob_prefix,
                                                       today)

  storage_client = storage.Client()
  bucket = storage_client.bucket(GCS_BUCKET_NAME)
  blob = bucket.blob(destination_log_file)

  # blob.upload_from_filename(source_file_name)
  blob.upload_from_string(data_string)

  print('[{}] - Data uploaded to {}.'.format(time_now_str(), destination_log_file))


def today_date():
    tz = pytz.timezone(PROJECT_TIMEZONE)
    return datetime.datetime.now(tz).date()


def time_now_str():
      # set correct timezone for datetime check
    tz = pytz.timezone(PROJECT_TIMEZONE)
    return datetime.datetime.now(tz).strftime("%m-%d-%Y, %H:%M:%S")



def upload_data(rows):
    service = setup()
    upload_log = ''
    print('Authorization successful')
    currentrow = 0
    # For each row, create a conversion object:
    all_conversions = """{"kind": "doubleclicksearch#conversionList", "conversion": ["""
    while currentrow < len(rows):
        for row in rows[currentrow:min(currentrow+100, len(rows))]:
            conversion = json.dumps({
                'clickId': row['conversionVisitExternalClickId'],
                'conversionId': row['conversionId'],
                'conversionTimestamp': row['conversionTimestampMillis'],
                'segmentationType': 'FLOODLIGHT',
                'segmentationName': row['floodlightActivity'],
                'type': row['conversionType'],
                'revenueMicros': int(row['conversionRevenue'] * 1000000),
                'currencyCode': 'USD'
            })
            all_conversions = all_conversions + conversion + ','
        all_conversions = all_conversions[:-1] + ']}'
        request = service.conversion().insert(body=json.loads(all_conversions))
        print('[{}] - SA360 API Request: '.format(time_now_str()), request)
        try:
            response = request.execute()
            print('[{}] - SA360 API Response: '.format(time_now_str()), request)
            if 'hasFailures' not in response:
                print('Successfully inserted batch of 100.')
            else:
                status = response['status']
                print(status)
                for line in status:
                    try:
                        if line['errors']:
                            for error in line['errors']:
                                err_msg = 'Error in line ' + json.dumps(line['conversion'])
                                print('[Conversion Insert Errors][{}] - {}\n'.format(time_now_str(), err_msg))
                                print('\t[%s]: %s\n' % (error['code'], error['message']))
                    except:
                        print('Conversion with gclid ' + line['gclid'] + ' inserted.')
        except errors.HttpError as e:
            print('[Conversion HTTP Errors][{}] - {}\n'.format(time_now_str(), e))
            # errorlist = json.loads(e.content)['error']['errors']
            # for error in errorlist:
            #   print(error['message'])
        print('Either finished or found errors.')
        currentrow += 100
        print(all_conversions)
        # Reset all_conversions
        all_conversions = """{"kind": "doubleclicksearch#conversionList", "conversion": ["""


def main(event, context):
    print('[{}] Start SA360 conversion upload!'.format(time_now_str()))
    print('Event: ', event)
    cloud_client = bigquery.Client()
    # decode pub/sub payload
    payload = base64.b64decode(event.get('data')).decode('ascii')
    json_payload = json.loads(payload)
    
    print('Payload: ', json_payload)
    conversion_data = json_payload['data']['conversions']
    if conversion_data:
        upload_data(conversion_data)
    else:
        print('No conversion data passed into the function! Please check your workflow for downstream errors')