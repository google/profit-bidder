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
import os
import pytz

from io import StringIO

from googleapiclient import errors

from google.cloud import bigquery
from google.cloud import pubsub


# Instantiates a Pub/Sub client
publisher = pubsub.PublisherClient()
PROJECT_ID = 'replace-with-your-project-id'
# Defaults to America/New_York, please update to 
# your respective timezone if needed.
PROJECT_TIMEZONE = 'America/New_York'


def today_date():
    tz = pytz.timezone(PROJECT_TIMEZONE)
    return datetime.datetime.now(tz).date()


def time_now_str():
      # set correct timezone for datetime check
    tz = pytz.timezone(PROJECT_TIMEZONE)
    return datetime.datetime.now(tz).strftime("%m-%d-%Y, %H:%M:%S")


def get_data(table, cloud_client):
  rows_iter = cloud_client.list_rows(table)
  rows = list(rows_iter)
  print('Downloaded {} rows from table {}'.format(len(rows), table))
  return rows


def get_dataset(table_name, cloud_client):
    datasets = cloud_client.list_datasets()
    found = False
    table_ref = None
    for dataset in datasets:
        print(dataset)
        table = '{}.{}.{}'.format(cloud_client.project,
                                               dataset.dataset_id,
                                               table_name)
        try:
            print('trying {}'.format(table))
            table_ref = cloud_client.get_table(table)
        except:
            print('Table not found')
            pass
        else:
            print('Table found: {} | {} | {} | {}'.format(table_ref.dataset_id,
                                                        table_ref.full_table_id,
                                                        table_ref.created.date(),
                                                        table_ref.modified.date()))
            found = True
            return table_ref
    if not found:
        # TODO(angelozamudio) Add Error Handling
        raise ValueError('Could not find table with the provided table name: {}.'.format(table_name))


# Publishes a message to a Cloud Pub/Sub topic.
def publish(data, topic_name, config):
    if not topic_name or not data:
        print('Missing "topic" and/or "data" parameter.')
        return

    print('Publishing message to topic {}'.format(topic_name))

    # References an existing topic
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)

    conversion_data = []
    for row in data:
        result = {}
        for key in row.keys():
            value = row.get(key)
            if type(value) == datetime.datetime or type(value) == datetime.date:
                result[key] = value.strftime("%Y-%m-%d ")
            elif type(value) == decimal.Decimal:
                result[key] = float(value)
            else:
                result[key] = value
        conversion_data.append(result)

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


def partition_and_distribute(data_rows, topic, config):
    current_row = 0
    while current_row < len(data_rows):
        segment = data_rows[current_row:min(current_row+1000, len(data_rows))]
        print('Batch size: ', len(segment))
        publish(segment, topic, config)
        current_row += 1000


def main(event, context):
    print('[{}] - Start Conversion upload delegator'.format(time_now_str()))
    print('EVENT: ', event)
    # set correct timezone for datetime check
    todays_date = today_date()

    # Instansiate BQ client
    cloud_client = bigquery.Client()

    # decode pub/sub payload
    payload = base64.b64decode(event.get('data')).decode('utf-8')
    json_payload = json.loads(payload)
    
    print('Payload: ', json_payload)

    table_name = json_payload['table_name'] if 'table_name' in json_payload else None
    topic = json_payload['topic'] if 'topic' in json_payload else None
    config = json_payload['cm360_config'] if 'cm360_config' in json_payload else None

    table = get_dataset(table_name, cloud_client)
    
    if table:
        table_ref_name = table.full_table_id.replace(':', '.')
        if table.modified.date() == todays_date or table.created.date() == todays_date:
            print('[{}] is up-to-date. Continuing with upload...'.format(table_ref_name))
            data = get_data(table_ref_name, cloud_client)
            if topic:
                partition_and_distribute(data, topic, config)
            else:
                print('No target pub/sub topic name provided. Please update and retry....upload aborted!')
        else:
            print('[{}] data may be stale. Please check workflow to verfiy that it has run correctly. Upload is aborted!'.format(table_ref_name))
    else:
        print('Table not found! Please double check your workflow for any errors.')
