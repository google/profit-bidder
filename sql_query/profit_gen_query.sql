-- Copyright 2021 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ******    TEMPLATE CODE    ******
-- NOTE: Please thoroughly review and test your version of this query before launching your pipeline
-- The resulting data from this script should provide all the necessary columns for upload via 
-- the CM360 API and the SA360 API

WITH
campaigns AS (
    -- Example: Extracting all campaign names and IDs if needed for filtering for
    -- conversions for a subset of campaigns
    SELECT
        campaign,
        campaignId,
        row_number() OVER (partition BY campaignId ORDER BY lastModifiedTimestamp DESC) as row_num -- for de-duping
    FROM `<sa360_dataset_name>.p_Campaign_<advertiser_id>`
    -- Be sure to replace the Timezone with what is appropriate for your use case
    WHERE EXTRACT(DATE FROM _PARTITIONTIME) >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 7 DAY)
),
expanded_conversions AS (
    -- Parses out all relevant product data from a conversion request string
    SELECT
        conv.*,
        campaign,
        -- example of U-Variables that are parsed to extract product purchase data
        SPLIT(REGEXP_EXTRACT(floodlightEventRequestString, "u5=(.*?);"),"|") AS u5,
        SPLIT(REGEXP_EXTRACT(floodlightEventRequestString, "u6=(.*?);"),"|") AS u6,
        SPLIT(REGEXP_EXTRACT(floodlightEventRequestString, "u7=(.*?);"),"|") AS u7,
    FROM `<sa360_dataset_name>.p_Conversion_<advertiser_id>` AS conv
    LEFT JOIN (
        SELECT campaign, campaignId
        FROM campaigns
        WHERE row_num = 1
        GROUP BY 1,2
    ) AS camp
    USING (campaignId)
    WHERE
        -- Filter for conversions that occured in the previous day
        -- Be sure to replace the Timezone with what is appropriate for your use case
        conv.conversionDate = DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY)
        AND floodlightActivity IN ('My Sales Floodlight Name')
        AND accountType = 'Google AdWords' -- filter by Account Type as needed
),
flattened_conversions AS (
    -- Flattens the extracted product data for each conversion which leaves us with a row
    -- of data for each product purchased as part of a given conversion
    SELECT
        advertiserId,
        campaignId,
        conversionId,
        skuId,
        pos1,
        quantity,
        pos2,
        cost,
        pos3
    FROM expanded_conversions,
    UNNEST(expanded_conversions.u5) AS skuId WITH OFFSET pos1,
    UNNEST(expanded_conversions.u6) AS quantity WITH OFFSET pos2,
    UNNEST(expanded_conversions.u7) AS cost WITH OFFSET pos3
    WHERE pos1 = pos2 AND pos1 = pos3 AND skuId != ''
    GROUP BY 1,2,3,4,5,6,7,8,9
    ORDER BY conversionId
),
gmc AS (
    -- Extract all relevant fields from GMC product feed to help identify what margin value
    -- should be applied to a product
    -- NOTE: this may not be needed if enough data is made available via U-Variables
    SELECT 
        offer_id,
        title,
        custom_labels.label_1 as label_1, -- sample case, this label identified a margin 'class'
        product_type,
        row_number() OVER (partition BY product_id ORDER BY product_data_timestamp DESC) as row_num -- for de-duping
    FROM `<gmc_dataset_name>.Products_<gmc_account_id>`
    -- Be sure to replace the Timezone with what is appropriate for your use case
    WHERE EXTRACT(DATE FROM _PARTITIONTIME) >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 7 DAY)
),
gmc_to_margin AS (
    -- Merges the GMC products from the previous query with the margin data table provided by the client
    -- NOTE: This portion will have to be modified to best fit the margin data tabel that is provided by the client.
    SELECT
        gmc.offer_id,
        gmc.label_1,
        gmc.product_type, 
        bd.Class_Name,
        bd.Scored_Value as margin
    FROM gmc
    LEFT JOIN `business_data.<client_margin_data_table>` as bd
    -- Specify how GMC product should be matched to margin data
    ON REPLACE(gmc.label_1, "  ", " ") = REPLACE(bd.Class_Name, "  ", " ")
    WHERE gmc.row_num = 1
    GROUP BY 1,2,3,4,5
    ORDER by 1 ASC,2 ASC
)
,inject_gmc_margin AS (
    -- Merges GMC/Margin data with the products found in the conversion data
    SELECT 
        advertiserId,
        campaignId,
        conversionId,
        skuId,
        quantity,
        IF(cost = '', '0', cost) as cost,
        pos1,
        pos2,
        pos3,
        -- PLACEHOLDER MARGIN, X% for unclassified items
        CASE
        WHEN margin IS NULL THEN 0.X
        ELSE margin
        END AS margin,
        offer_id,
        MIN(label_1) AS label_1
    FROM flattened_conversions
    LEFT JOIN gmc_to_margin
    ON flattened_conversions.skuId = offer_id
group by 1,2,3,4,5,6,7,8,9,10,11
),
all_conversions as (
    -- Rolls up all previously expanded conversion data while calculating profit based on the matched 
    -- margin value. Also assigns timestamp in millis and micros 
    SELECT
        e.account,
        e.accountId,
        e.accountType,
        e.advertiser,
        igm.advertiserId,
        e.agency,
        e.agencyId,
        igm.campaignId,
        e.campaign,
        e.conversionAttributionType,
        e.conversionDate,
        -- '00' may be changed to any string value that will help you identify these
        -- new conversions in reporting
        CONCAT(igm.conversionId, '00') as conversionId,
        e.conversionLastModifiedTimestamp,
        -- Note:Rounds float quantity and casts to INT, change based on use case
        -- This is done to support CM360 API
        CAST(ROUND(e.conversionQuantity) AS INT64) AS conversionQuantity,
        e.conversionRevenue,
        SUM(
            FLOOR(CAST(igm.cost AS FLOAT64))
        ) AS CALCULATED_REVENUE,
        -- PROFIT CALCULATED HERE, ADJUST LOGIC AS NEEDED FOR YOUR USE CASE
        ROUND(
            SUM(
                -- multiply item cost by class margin
                SAFE_MULTIPLY(
                    CAST(igm.cost AS FLOAT64),
                    igm.margin)
            ),2
        ) AS CALCULATED_PROFIT,
        e.conversionSearchTerm,
        e.conversionTimestamp,
        -- SA360 timestamp should be in millis
        UNIX_MILLIS(e.conversionTimestamp) as conversionTimestampMillis,
        -- CM360 Timestamp should be in micros
        UNIX_MICROS(e.conversionTimestamp) as conversionTimestampMicros,
        e.conversionType,
        e.conversionVisitExternalClickId,
        e.conversionVisitId,
        e.conversionVisitTimestamp,
        e.deviceSegment,
        e.floodlightActivity,
        e.floodlightActivityId,
        e.floodlightActivityTag,
        e.floodlightEventRequestString,
        e.floodlightOrderId,
        e.floodlightOriginalRevenue,
        status
    FROM inject_gmc_margin AS igm
    LEFT JOIN expanded_conversions AS e
    ON igm.advertiserID = e.advertiserId AND igm.campaignId = e.campaignID AND igm.conversionId = e.conversionId
    GROUP BY 1,2,3,4,5,6,8,7,9,10,11,12,13,14,15,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
)

-- The columns below represent the original conversion data with their new profit
-- values calculated (assigned to conversionRevenue column) along with any original 
-- floofdlight data that the client wishes to keep for trouble shooting.
SELECT 
    account,
    accountId,
    accountType,
    advertiser,
    advertiserId,
    agency,
    agencyId,
    campaignId,
    campaign,
    conversionId,
    conversionAttributionType,
    conversionDate,
    conversionTimestamp,
    conversionTimestampMillis,
    conversionTimestampMicros,
    CALCULATED_PROFIT AS conversionRevenue,
    conversionQuantity,
    -- Here you will set your target/placeholder floodlight name
    "My Placeholder - Floodlight Activity" AS floodlightActivity,
    conversionSearchTerm,
    conversionType,
    conversionVisitExternalClickId,
    conversionVisitId,
    conversionVisitTimestamp,
    deviceSegment,
    CALCULATED_PROFIT,
    CALCULATED_REVENUE,
    -- Please prefix any original conversion values you wish to keep with "original". 
    -- These values may help with troubleshooting
    conversionRevenue AS originalConversionRevenue,
    floodlightActivity AS originalFloodlightActivity,
    floodlightActivityId AS originalFloodlightActivityId,
    floodlightActivityTag AS originalFloodlightActivityTag,
    floodlightOriginalRevenue AS originalFloodlightRevenue,
    floodlightEventRequestString,
    floodlightOrderId
FROM all_conversions
WHERE CALCULATED_PROFIT > 0.0
ORDER BY account ASC
