# SA360 Profit Bidder Pipeline

> Disclaimer: This is not an official Google product.

**Author:** Angelo Zamudio (angelozamudio@google.com), Damodar Panigrahi (dpani@google.com)

### Objective

To create an automated data pipeline that will run daily to extract the previous day's conversion data via an SA360 data transfer, generate new conversions with calculated order profit as revenue based on margin data file and upload the new conversions back into Search Ads 360 (SA360) where it will be leveraged for Custom Bidding and/or reporting.

### Requirements

The pipeline is built within a **Google Cloud** project instance and uses the following cloud products and technologies:

	- Big Query
		- Function:
			- SA360 to Big Query Connector
			- Google Merchant Center (GMC) Data Transfer
			- Store the  margin data file
			- Data storage
			- Data transformation
	- Google Cloud Functions
		- Function:
			- Execute script to upload new conversions via SA360 API
	- Google Cloud Scheduler
		- Function:
			- Trigger cloud function
	- Google Cloud Storage  *(optional)*
		- Function:
				- Store upload script execution/error logs
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
      * Cloud Functions API
      * Cloud Storage API
      * Cloud Pub/Sub API
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


9) Create Scheduled Query
    - [Create a scheduled query](https://cloud.google.com/bigquery/docs/scheduling-queries#setting_up_a_scheduled_query) to run transformation queries for each advertiser.
    - Example Configuration:
      - **Scheduled Query Name:** ```<advertiser_name/id> Profit Gen``` or Any
      - **Destination Dataset:** Rescpective advertiser dataset created in Step 6
      - **Destination Table:** ```conversion_final_<sa360_advertiser_id>```
      - **Write Preference:** ```WRITE_TRUNCATE```
      - **Query String:** Reference query code in the ```sql_query``` folder.


10) Create Delegator Cloud Function
    - [Create cloud function](https://cloud.google.com/functions/docs/deploying/console) with the following configurations:
    - Step 1 configuration:
      - **Function Name:** ```cloud_conversion_upload_delegator```
      - **Region:** us-central1
      - **Trigger:** Pub/Sub
      - **Authentication:** Require authentication
      - **Advanced:**
        - **Memory allocated:** 2 GB
        - **Timeout:** 540 seconds
        - **Service Account:** App Engine default service account
    - Step 2 configuration:
      - **Runtime:** Python 3.7
      - **Entry point:** ```main```
      - **Code:** Reference code in the ```conversion_upload_delegator``` folder


11) Create Upload Cloud Function - For this step you have the option of standing up either the CM360 upload node or the SA360 upload node.
> NOTE: It is recommended to utilize the CM360 API for offline conversion uploads unless your use case can only be supported by the SA360 API.
    - **(Option A)** - Create CM360 Cloud Function. [Create cloud function](https://cloud.google.com/functions/docs/deploying/console) with the following configurations:
      - Step 1 configuration:
        - **Function Name:** ```cm360_cloud_conversion_upload_node```
        - **Region:** us-central1
        - **Trigger:** Pub/Sub
        - **Authentication:** Require authentication
        - **Advanced:**
          - **Memory allocated:** 256 MB
          - **Timeout:** 540 seconds
          - **Service Account:** App Engine default service account
      - Step 2 configuration:
        - **Runtime:** Python 3.7
        - **Entry point:** ```main```
        - **Code:** Reference code in the ```CM360_cloud_conversion_upload_node``` folder.

    - **(Option B)** - Create SA360 Cloud Function. [Create cloud function](https://cloud.google.com/functions/docs/deploying/console) with the following configurations:
      - Step 1 configuration:
        - **Function Name:** ```sa360_cloud_conversion_upload_node```
        - **Region:** us-central1
        - **Trigger:** Pub/Sub
        - **Authentication:** Require authentication
        - **Advanced:**
          - **Memory allocated:** 256 MB
          - **Timeout:** 540 seconds
          - **Service Account:** App Engine default service account
      - Step 2 configuration:
        - **Runtime:** Python 3.7
        - **Entry point:** ```main```
        - **Code:** Reference code in the ```SA360_cloud_conversion_upload_node``` folder.

11) Standup Cloud Scheduler Job(s)
    - [Create a Cloud Scheduler job](https://cloud.google.com/scheduler/docs/creating) per target advertiser. Please note that each advertiser will have its own scheduled job and Frequency should be staggered by 5 minutes within the same hour.
    - Example configuration:
      - **Name:** Any
      - **Description:** Any
      - **Frequency:**
        - Example: starting everyday at 6 AM staggered by 5 minutes:
          - ```0 6 * * *```
          - ```5 6 * * *```
          - ```10 6 * * *```
          - etc...
      - **Timezone:** As per your preference
      - **Target:** Pub/Sub
      - **Topic:** ```conversion_upload_delegator```
      - Payload samples in the section below.


### Cloud Scheduler Pub/Sub Payload Examples
CM360 sample payload:
```json
{  
	"table_name": "conversion_final_<CM360_advertiser_id>",  
	"topic": "cm360_conversion_upload",  // hardcoded
	"cm360_config": {  
		"profile_id": <service account CM360 Profile ID>,  
		"floodlight_activity_id" : <CM360 short form Floodlight Activity ID>,  
		"floodlight_configuration_id": <CM360 Floodlight Configuration ID>  
	}  
}
```

SA360 sample payload:
```json
{  
	"table_name": "conversion_final_<SA360_advertiser_id>",  
	"topic": "SA360_conversion_upload"  // hardcoded
}
```
