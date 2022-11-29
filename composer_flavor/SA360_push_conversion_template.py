# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#      http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Reads the from transformed table, chunks the data, 
#   and uploads the data to CM360
# We need to chunk the data so as to adhere 
#   to the payload limit of the CM360 REST API.
import pytz
import datetime
import decimal
import logging
import json
import google.auth
import google.auth.impersonated_credentials
import google_auth_httplib2
from googleapiclient import discovery
from google.cloud import bigquery

PB_SA_EMAIL = '<sa_email>'
PB_API_SCOPES = ['https://www.googleapis.com/auth/dfareporting',
              'https://www.googleapis.com/auth/dfatrafficking',
              'https://www.googleapis.com/auth/ddmconversions',
              'https://www.googleapis.com/auth/devstorage.read_write']
PB_CM360_API_NAME = 'dfareporting'
PB_CM360_API_VERSION = 'v4'
PB_REQUIRED_KEYS = [
    'conversionId',
    'conversionQuantity',
    'conversionRevenue',
    'conversionTimestamp',
    'conversionVisitExternalClickId',
]
PB_GCP_PROJECT = '<project_id>'
PB_DS_BUSINESS_DATA ='<business_dataset_name>'
PB_CM360_TABLE = '<transformed_data_tbl>'
PB_BATCH_SIZE = 100
PB_TIMEZONE = '<timezone>'
PB_CM360_PROFILE_ID = '<cm_profileid>'
PB_CM360_FL_CONFIG_ID = '<fl_config_id>'
PB_CM360_FL_ACTIVITY_ID = '<fl_activity_id>'

def today_date(timezone):
    """Returns today's date using the timezone
    Args:
        timezone(:obj:`str`): The timezone with default to America/New_York
    Returns:
      Date: today's date
    """
    tz = pytz.timezone(timezone)
    return datetime.datetime.now(tz).date()

def time_now_str(timezone):
    """Returns today's date using the timezone
    Args:
        timezone(:obj:`str`): The timezone with default to America/New_York
    Returns:
      Timezone: current timezone
    """
    # set correct timezone for datetime check
    tz = pytz.timezone(timezone)
    return datetime.datetime.now(tz).strftime("%m-%d-%Y, %H:%M:%S")

def pluralize(count):
    """An utility function 
    Args:
        count(:obj:`int`): A number
    Returns:
      str: 's' or empty
    """
    if count > 1:
        return 's'
    return ''  

def get_data(table_ref_name, cloud_client, batch_size):
    """Returns the data from the transformed table.
    Args:
        table_ref_name(:obj:`google.cloud.bigquery.table.Table`): Reference to the table
        cloud_client(:obj:`google.cloud.bigquery.client.Client`): BigQuery client
        batch_size(:obj:`int`): Batch size
    Returns:
      Array[]: list/rows of data
    """

    current_batch = []
    table = cloud_client.get_table(table_ref_name)
    print(f'Downloading {table.num_rows} rows from table {table_ref_name}')
    skip_stats = {}
    for row in cloud_client.list_rows(table_ref_name):
        missing_keys = []
        for key in PB_REQUIRED_KEYS:
            val = row.get(key)
            if val is None:
                missing_keys.append(key)
                count = skip_stats.get(key, 0)
                count += 1
                skip_stats[key] = count
        if len(missing_keys) > 0:
            row_as_dict = dict(row.items())
            logging.debug(f'Skipped row: missing values for keys {missing_keys} in row {row_as_dict}')
            continue
        result = {}
        conversionTimestamp = row.get('conversionTimestamp')
        # convert floating point seconds to microseconds since the epoch
        result['conversionTimestampMicros'] = int(conversionTimestamp.timestamp() * 1_000_000)
        for key in row.keys():
            value = row.get(key)
            if type(value) == datetime.datetime or type(value) == datetime.date:
                result[key] = value.strftime("%y-%m-%d ")
            elif type(value) == decimal.Decimal:
                result[key] = float(value)
            else:
                result[key] = value
        current_batch.append(result)
        if len(current_batch) >= batch_size:
            yield current_batch
            current_batch = []
    if len(current_batch) > 0:
        yield current_batch
    pretty_skip_stats = ', '.join([f'{val} row{pluralize(val)} missing key "{key}"' for key, val in skip_stats.items()])
    logging.info(f'Processed {table.num_rows} from table {table_ref_name} skipped {pretty_skip_stats}')

def setup(sa_email, api_scopes, api_name, api_version):
    """Impersonates a service account, authenticate with Google Service,
      and returns a discovery api for further communication with Google Services.
    Args:
        sa_email(:obj:`str`): Service Account to impersonate
        api_scopes(:obj:`Any`): An array of scope that the service account 
          expectes to have permission in the CM360
        api_name(:obj:`str`): CM360 API Name
        api_version(:obj:`str`): CM360 API version
    Returns:
      module:discovery: to interact with Goolge Services.
    """

    source_credentials, project_id = google.auth.default()

    target_credentials = google.auth.impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=sa_email,
        target_scopes=api_scopes,
        delegates=[],
        lifetime=500)

    http = google_auth_httplib2.AuthorizedHttp(target_credentials)
    # setup API service here
    try: 
      return discovery.build(
          api_name,
          api_version,
          cache_discovery=False,
          http=http)
    except Exception as e:
        print(f'Could not authenticate: {str(e)}')


def upload_data(timezone, rows, profile_id, fl_configuration_id, fl_activity_id):
    """POSTs the conversion data using CM360 API
    Args:
        timezone(:obj:`Timezone`): Current timezone or defaulted to America/New_York 
        rows(:obj:`Any`): An array of conversion data
        profile_id(:obj:`str`): Profile id - should be gathered from the CM360
        fl_configuration_id(:obj:`str`): Floodlight config id - should be gathered from the CM360
        fl_activity_id(:obj:`str`): Floodlight activity id - should be gathered from the CM360
    """
  
    print('Starting conversions for ' + time_now_str(timezone))
    if not fl_activity_id or not fl_configuration_id:
        print('Please make sure to provide a value for both floodlightActivityId and floodlightConfigurationId!!')
        return
    # Build the API connection
    try:       
      service = setup(PB_SA_EMAIL, PB_API_SCOPES, 
                      PB_CM360_API_NAME,  PB_CM360_API_VERSION)
      # upload_log = ''
      print('Authorization successful')
      currentrow = 0
      all_conversions = """{"kind": "dfareporting#conversionsBatchInsertRequest", "conversions": ["""
      while currentrow < len(rows):
          for row in rows[currentrow:min(currentrow+100, len(rows))]:
              conversion = json.dumps({
                  'kind': 'dfareporting#conversion',
                  'gclid': row['conversionVisitExternalClickId'],
                  'floodlightActivityId': fl_activity_id, # (Use short form CM Floodlight Activity Id )
                  'floodlightConfigurationId': fl_configuration_id, # (Can be found in CM UI)
                  'ordinal': row['conversionId'],
                  'timestampMicros': row['conversionTimestampMicros'],
                  'value': row['conversionRevenue'],
                  'quantity': row['conversionQuantity'] if 'conversionQuantity' in row else 1 #(Alternatively, this can be hardcoded to 1)
              })
              # print('Conversion: ', conversion) # uncomment if you want to output each conversion
              all_conversions = all_conversions + conversion + ','
          all_conversions = all_conversions[:-1] + ']}'
          payload = json.loads(all_conversions)
          print(f'CM360 request payload: {payload}')
          request = service.conversions().batchinsert(profileId=profile_id, body=payload)
          print('[{}] - CM360 API Request: '.format(time_now_str(timezone)), request)
          response = request.execute()
          print('[{}] - CM360 API Response: '.format(time_now_str(timezone)), response)
          if not response['hasFailures']:
              print('Successfully inserted batch of 100.')
          else:
              status = response['status']
              for line in status:
                  try:
                      if line['errors']:
                          for error in line['errors']:
                              print('Error in line ' + json.dumps(line['conversion']))
                              print('\t[%s]: %s' % (error['code'], error['message']))
                  except:
                      print('Conversion with gclid ' + line['gclid'] + ' inserted.')
          print('Either finished or found errors.')
          currentrow += 100
          all_conversions = """{"kind": "dfareporting#conversionsBatchInsertRequest", "conversions": ["""
    except Exception as e:
        print(f'Error: {str(e)}')

def partition_and_distribute(cloud_client, table_ref_name, batch_size, timezone, 
                             profile_id, fl_configuration_id, fl_activity_id):
    """Partitions the data to chunks of batch size and
        uploads to the CM360
    Args:
        table_ref_name(:obj:`google.cloud.bigquery.table.Table`): Reference to the table
        cloud_client(:obj:`google.cloud.bigquery.client.Client`): BigQuery client
        batch_size(:obj:`int`): Batch size
        timezone(:obj:`Timezone`): Current timezone or defaulted to America/New_York 
        profile_id(:obj:`str`): Profile id - should be gathered from the CM360
        fl_configuration_id(:obj:`str`): Floodlight config id - should be gathered from the CM360
        fl_activity_id(:obj:`str`): Floodlight activity id - should be gathered from the CM360
    """
    for batch in get_data(table_ref_name, cloud_client, batch_size):
        # print(f'Batch size: {len(batch)} batch: {batch}')
        upload_data(timezone, batch, profile_id, fl_configuration_id, 
                    fl_activity_id)
        # DEBUG BREAK!
        if batch_size == 1:
            break

def push_conversion():
    try: 
        bq_client = bigquery.Client(project=PB_GCP_PROJECT)
        table = bq_client.get_table(f'{PB_DS_BUSINESS_DATA}.{PB_CM360_TABLE}')
    except:
        print ('Could not find table with the provided table name: {}.'.format(f'{PB_DS_BUSINESS_DATA}.{PB_CM360_TABLE}'))    
        table = None

    todays_date = today_date(PB_TIMEZONE)

    if table is not None:
        table_ref_name = table.full_table_id.replace(':', '.')
        if table.modified.date() == todays_date or table.created.date() == todays_date:
            print('[{}] is up-to-date. Continuing with upload...'.format(table_ref_name))
            partition_and_distribute(bq_client, table_ref_name, PB_BATCH_SIZE,
                                    PB_TIMEZONE, PB_CM360_PROFILE_ID, 
                                    PB_CM360_FL_CONFIG_ID, PB_CM360_FL_ACTIVITY_ID) 
        else:
            print('[{}] data may be stale. Please check workflow to verfiy that it has run correctly. Upload is aborted!'.format(table_ref_name))
    else:
        print('Table not found! Please double check your workflow for any errors.')