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
import decimal
import json
import logging
import os
import pytz

from io import StringIO

from googleapiclient import errors

from google.cloud import bigquery
from google.cloud import pubsub


# Instantiates a Pub/Sub client
publisher = pubsub.PublisherClient()
PROJECT_ID = os.getenv('GCP_PROJECT')
# Defaults to America/New_York, please update to 
# your respective timezone if needed.
PROJECT_TIMEZONE = os.getenv('TIMEZONE')

def today_date():
    tz = pytz.timezone(PROJECT_TIMEZONE)
    return datetime.datetime.now(tz).date()


def time_now_str():
      # set correct timezone for datetime check
    tz = pytz.timezone(PROJECT_TIMEZONE)
    return datetime.datetime.now(tz).strftime("%m-%d-%Y, %H:%M:%S")


def get_dataset(dataset_name, table_name, cloud_client):
    try: 
        return cloud_client.get_table(f'{dataset_name}.{table_name}')
    except:
        raise ValueError('Could not find table with the provided table name: {}.'.format(f'{dataset_name}.{table_name}'))    

# Publishes a message to a Cloud Pub/Sub topic.
def publish(data, topic_name, config):
    if not topic_name or not data:
        print('Missing "topic" and/or "data" parameter.')
        return

    print('Publishing message to topic {}'.format(topic_name))

    # References an existing topic
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)

    conversion_data = data

    message = {'data': None}
    # setup message data appropriately
    if config:
        message['data'] = {
            'conversions': conversion_data,
            'config': config
        }
    else:
        message['data'] = {
            'conversions': conversion_data
        }

    message_json = json.dumps(message)
    message_bytes = message_json.encode('utf-8')
    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        res = publish_future.result()  # Verify the publish succeeded
        print('Message published to: {}'.format(res))
    except Exception as e:
        print('Exception found: {}'.format(e))


REQUIRED_KEYS = [
    'conversionId',
    'conversionQuantity',
    'conversionRevenue',
    'conversionTimestamp',
    'conversionVisitExternalClickId',
]
def get_data(table_ref_name, cloud_client, batch_size):
    current_batch = []
    table = cloud_client.get_table(table_ref_name)
    print(f'Downloading {table.num_rows} rows from table {table_ref_name}')
    skip_stats = {}
    for row in cloud_client.list_rows(table_ref_name):
        missing_keys = []
        for key in REQUIRED_KEYS:
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


def pluralize(count):
    if count > 1:
        return 's'
    return ''


def partition_and_distribute(cloud_client, table_ref_name, topic, config):
    batch_size = 1000
    for batch in get_data(table_ref_name, cloud_client, batch_size):
        print(f'Batch size: {len(batch)} batch: {batch}')
        publish(batch, topic, config)
        # DEBUG BREAK!
        if batch_size == 1:
            break


def decode_json(payload):
    '''
    Example payload:
    {
      "dataset_name": "dataset",
      "table_name": "table",
      "topic": "topic",
      "cm360_config": {
        "profile_id": "",
        "floodlight_activity_id": "",
        "floodlight_configuration_id": ""
      }
    }
    '''
    try:
        json_payload = json.loads(payload)
    except:
        print(f'Unable to parse json payload: {payload}')
        raise
    dataset_name = json_payload['dataset_name'] if 'dataset_name' in json_payload else None
    table_name = json_payload['table_name'] if 'table_name' in json_payload else None
    topic = json_payload['topic'] if 'topic' in json_payload else None
    config = json_payload['cm360_config'] if 'cm360_config' in json_payload else None
    return dataset_name, table_name, topic, config

def main(event, context):
    print('[{}] - Start Conversion upload delegator'.format(time_now_str()))
    print(f'EVENT: {event}')
    # set correct timezone for datetime check
    todays_date = today_date()

    # Instansiate BQ client
    cloud_client = bigquery.Client(project=PROJECT_ID)

    payload = ''
    if 'type.googleapis.com/google.pubsub.v1.PubsubMessage' == event.get('@type', ''):
        # decode pub/sub payload
        payload = base64.b64decode(event.get('data', '')).decode('utf-8')
        # the below is to get rid of the sourrounding single quotes
        # the single quotes are injected at the creation time the shell script 
        #   the shell cmds injects them.
        payload = payload.replace("'{", "{").replace("}'", "}")
    else:
        # the CF is inovked from the Testing functionalities of the console
        payload = json.dumps(event)
    dataset_name, table_name, topic, config = decode_json(payload)
    print(f'dataset: {dataset_name}, table: {table_name} topic: {topic} config: {config}')

    table = get_dataset(dataset_name, table_name, cloud_client)
    
    if table is not None:
        table_ref_name = table.full_table_id.replace(':', '.')
        if table.modified.date() == todays_date or table.created.date() == todays_date:
            print('[{}] is up-to-date. Continuing with upload...'.format(table_ref_name))
            if topic:
                partition_and_distribute(cloud_client, table_ref_name, topic, config)
            else:
                print('No target pub/sub topic name provided. Please update and retry....upload aborted!')
        else:
            print('[{}] data may be stale. Please check workflow to verfiy that it has run correctly. Upload is aborted!'.format(table_ref_name))
    else:
        print('Table not found! Please double check your workflow for any errors.')