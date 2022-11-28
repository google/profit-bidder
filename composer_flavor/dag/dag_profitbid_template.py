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

from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from push_conversion import push_conversion

PB_GCP_PROJECT = '<project_id>'
PB_DS_BUSINESS_DATA ='<business_dataset_name>'
PB_SP_DATAWRANGLING = '<data_wrangling_sp>'
PB_DAG_NAME = '<dag_name>'

dag_args = {
    'start_date': datetime.now(), 
    'retries': 1,
    'retry_delay': timedelta(minutes=15)}

dag = DAG(
    dag_id=PB_DAG_NAME,
    default_args=dag_args,
    end_date=None,
    schedule_interval="0 13 * * *")

t1 = BigQueryInsertJobOperator(
    task_id="transform_aggregate",
    configuration={
        "query": {
            "query": f'''
            BEGIN
                CALL `{PB_GCP_PROJECT}.{PB_DS_BUSINESS_DATA}.{PB_SP_DATAWRANGLING}`();
            EXCEPTION WHEN ERROR THEN
                SELECT
                @@error.message,
                @@error.stack_trace,
                @@error.statement_text,
                @@error.formatted_stack_trace;
            END
            ''',
            "useLegacySql": False,
        }
    },
    dag=dag
)

t2 = PythonOperator(
    task_id='push_conversion',
    python_callable=push_conversion,
    dag=dag)

t1 >> t2