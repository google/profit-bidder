## License
```
Copyright 2022 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

# SA360 Profit Bidder Pipeline

> Disclaimer: This is not an official Google product.  

Deploy the pipeline using Cloud Composer. There is an alternative deployment architecture available [here](../README.md).

### Solution Architecture
Please find below the architecture of the solution:
![Architecture](/assets/images/Profitbid%20_arch_Composer%20flavor.svg?raw=true "Architecture")

### Requirements

The pipeline is built within a **Google Cloud** project instance and uses the following cloud products and technologies:

	- Big Query
		- Function:
			- SA360 to Big Query Connector
			- Google Merchant Center (GMC) Data Transfer
			- Store the  margin data file
			- Data storage
			- Data transformation
	- Google Cloud Composer
		- Function:
			- Execute a Stored Procedure to transform and calculate profit
            - Push the profit conversion data to SA360/CM360
	- Python 3.7
	- Standard SQL
	- Caampaign Manager 360 (CM360)
		- Create Offline Floodlight Tag
		- Grant service account access
	- Search Ads 360 (SA360)
		- Grant service account access

### Setup Guide

1) Create a Google Cloud Project instance
For this step you may [create a new project instance](https://cloud.google.com/appengine/docs/standard/nodejs/building-app/creating-project) if needed or utilize an already existing project in which case you may skip this step.


2) Enable required APIs (or APIs can be enabled as you perform each subsequent step). 
    * [Documentation to enable APIs](https://cloud.google.com/apis/docs/getting-started#enabling_apis) 
    * [Cloud Console API Library](https://console.cloud.google.com/apis/library?project=_&_ga=2.81994262.1809712873.1601650202-1765314580.1599253374) to enable the following APIs:
      * BigQuery API
      * BigQuery Storage API
      * BigQuery Data Transfer API
      * Cloud Storage API
      * Cloud Composer API
      * Campaign Manager API
      * Search Ads API


3) Create keyless service account 
    [Create service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts) for your project.
    * *(Note: Do not generate keys)* - [Link to GCP](https://console.cloud.google.com/iam-admin/serviceaccounts?_ga=2.150218550.1809712873.1601650202-1765314580.1599253374)


4) Grant service account product permissions
    - *(*NOTE: it is recommended to utilize the CM360 API for offline conversion uploads whenever possible. If your use case cannot be accommodated via CM360 then you may proceed with SA360)*
    - **(Option A)** - Add service account email to CM360 account.
    [Add the generated service account email as a user to your Account/Advertiser(s)](https://support.google.com/campaignmanager/answer/6098287?hl=en), making sure to grant user role permissions depending on the granularity of access you are comfortable with. At minimum the user account will need [Insert Offline Conversions / Edit Offline Conversions](https://developers.google.com/doubleclick-advertisers/guides/conversions_overview#prerequisites) permissions for the Account/Advertisers used in this project.

    - **(Option B)** - Add service account email to SA360 account. [Add the generated service account email as a user to your Agency/Advertiser(s)](https://support.google.com/searchads/answer/6051717?hl=en), making sure to grant with Agency User or Advertiser User level access (allowed to upload conversions) depending on the granularity of access you are comfortable with. At minimum the account will need Advertiser User access to the specific target advertisers used in this project.


5) **(Optional)** Create Cloud Storage Bucket for upload logs
    - Follow steps outlined [here](https://cloud.google.com/storage/docs/creating-buckets) to create a GCS bucket named ```converison_upload_log```


6) Create Bigquery datasets to segment data.
    - [Documentation](https://cloud.google.com/bigquery/docs/datasets) to create BQ dataset.
    - Create datasets for SA360, GMC and business data.
    - **SA360:** 1 per required Advertiser
      - **Name:** ```<advertiser_name>```
      - **Default table expiration:** 7 days - Never (table data is appended daily, up to your discretion)
    - **Google Merchant Center (GMC):**
      - **Name:** ```<account_name>_GMC_feed```
      - **Default table expiration:** 7 days - Never (table data is appended daily, up to your discretion)
    - **Business data** (margin file)
      - **Name:** ```business_data```
      - **Default table expiration:** Any

7) Create Bigquery Data Transfers
    - [Create following data transfers](https://cloud.google.com/bigquery-transfer/docs/merchant-center-transfer):
      - **SA360** (1 per advertiser, as needed) [[link](https://cloud.google.com/bigquery-transfer/docs/sa360-transfer)]
        - **Display Name:** Any
        - **Schedule:** Daily (recommended to run early morning, ex: 4AM EST)
        - **Dataset ID:** Relevant SA360 Advertiser dataset created in Step 6
        - **Agency/Advertiser ID:** Both IDs can be found in SA360
      - **Google Merchant Center** [[link](https://cloud.google.com/bigquery-transfer/docs/merchant-center-transfer)]
        - **Display Name:** Any
        - **Schedule:** Daily (recommended to run early morning, ex: 4AM EST)
        - **Dataset ID:** Google Merchant Center dataset created in Step 6
        - **Merchant ID:** ID can be found in GMC
        - For this project only the **Products & product issues** option is required and  should be checked.


8) Upload Margin data into Bigquery
    - [Manually upload](https://cloud.google.com/bigquery/docs/loading-data-local) margin data (```.csv``` file format recommended) into **business_data** dataset.
    - A data transfer from Google Cloud Storage may also used to automatically pull a specifed file which would refresh the target table at a set schedule.


9) Create Cloud Composer
    - [Create a Cloud Composer](https://cloud.google.com/composer/docs/composer-2/composer-overview) to run transformation queries for each advertiser and push the conversion data to SA360/CM360.

### Quick start up guide
[Notebook](/solution_test/profit_bidder_quickstart.ipynb) uses the synthesized data, which you can run in less than 30 mins to comprehend the core concept and familiarize yourself with the code. 

We recommend that you follow three broad phases to productionalize the solution: 
* Phase 1 - use the notebook to valid account access, etc., 
* Phase 2 - use the test module to further test with the full stack of the services, and finally, 
* Phase 3 - operationalize the solution in your environment.

### Demo solution with synthesized data
We provide synthesized test data to test the solution in the [test_solution](/solution_test/) folder. Please use the install.sh with proper parameters to install the demo module.