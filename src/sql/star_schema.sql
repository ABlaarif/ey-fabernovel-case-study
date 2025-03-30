
-- Create fact_sessions table
CREATE OR REPLACE TABLE analytics_star_schema.fact_sessions AS
SELECT
  fullVisitorId AS user_id,
  visitId AS session_id,
  PARSE_DATE('%Y%m%d', date) AS session_date,
  totals.pageviews,
  totals.hits AS total_hits,
  IFNULL(totals.transactionRevenue / 1e6, 0) AS revenue_usd,
  device.deviceCategory,
  trafficSource.source,
  trafficSource.medium
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170731';

-- Create dim_users table
CREATE OR REPLACE TABLE analytics_star_schema.dim_users AS
SELECT
  fullVisitorId AS user_id,
  geoNetwork.country,
  geoNetwork.region,
  geoNetwork.city,
  geoNetwork.continent,
  geoNetwork.subContinent
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
GROUP BY user_id, country, region, city, continent, subContinent;

-- Create dim_device table
CREATE OR REPLACE TABLE analytics_star_schema.dim_device AS
SELECT DISTINCT
  device.deviceCategory,
  device.browser,
  device.operatingSystem,
  device.isMobile
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`;

-- Create dim_traffic table
CREATE OR REPLACE TABLE analytics_star_schema.dim_traffic AS
SELECT DISTINCT
  trafficSource.source,
  trafficSource.medium,
  channelGrouping
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`;

-- Create dim_date table
CREATE OR REPLACE TABLE analytics_star_schema.dim_date AS
SELECT DISTINCT
  PARSE_DATE('%Y%m%d', date) AS session_date,
  EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date)) AS year,
  EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', date)) AS month,
  FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', date)) AS day_of_week
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`;

-- Create session_products bridge table
CREATE OR REPLACE TABLE analytics_star_schema.session_products AS
SELECT
  fullVisitorId AS user_id,
  visitId AS session_id,
  h.product.v2ProductName AS product_name,
  h.product.v2ProductCategory AS product_category,
  h.product.productRevenue / 1e6 AS product_revenue_usd
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
UNNEST(hits) AS h
WHERE h.type = 'TRANSACTION';
