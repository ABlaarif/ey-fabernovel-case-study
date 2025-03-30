-- Create the fact_sessions table
-- This is the main fact table that captures core session-level metrics, user behavior, and traffic data.
CREATE OR REPLACE TABLE analytics_star_schema.fact_sessions AS
SELECT
  fullVisitorId AS user_id,                          -- Unique identifier for the user
  visitId AS session_id,                             -- Unique identifier for the session
  PARSE_DATE('%Y%m%d', date) AS session_date,        -- Session date parsed to DATE format
  totals.pageviews,                                  -- Total number of pageviews in the session
  totals.hits AS total_hits,                         -- Total number of hits (pageviews, events, etc.)
  IFNULL(totals.transactionRevenue / 1e6, 0) AS revenue_usd, -- Revenue in USD (converted from micros)
  device.deviceCategory,                             -- Device category (e.g., desktop, mobile)
  trafficSource.source,                              -- Traffic source (e.g., google, direct)
  trafficSource.medium                               -- Traffic medium (e.g., organic, referral)
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170731'; -- Filters data between August 2016 and July 2017

-- Create the dim_users table
-- Dimension table with geographic attributes for each user
CREATE OR REPLACE TABLE analytics_star_schema.dim_users AS
SELECT
  fullVisitorId AS user_id,                          -- Unique user ID
  geoNetwork.country,                                -- User's country
  geoNetwork.region,                                 -- User's region/state
  geoNetwork.city,                                   -- User's city
  geoNetwork.continent,                              -- User's continent
  geoNetwork.subContinent                            -- Sub-continent information
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
GROUP BY user_id, country, region, city, continent, subContinent;

-- Create the dim_device table
-- Dimension table describing devices used during sessions
CREATE OR REPLACE TABLE analytics_star_schema.dim_device AS
SELECT DISTINCT
  device.deviceCategory,                             -- Device type (desktop, tablet, mobile)
  device.browser,                                    -- Web browser used
  device.operatingSystem,                            -- OS of the device
  device.isMobile                                    -- Boolean indicating if device is mobile
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`;

-- Create the dim_traffic table
-- Dimension table detailing how users arrived at the site
CREATE OR REPLACE TABLE analytics_star_schema.dim_traffic AS
SELECT DISTINCT
  trafficSource.source,                              -- Source of traffic (e.g., Google)
  trafficSource.medium,                              -- Medium (e.g., organic, referral)
  channelGrouping                                    -- High-level channel grouping
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`;

-- Create the dim_date table
-- Date dimension to support time-based analysis
CREATE OR REPLACE TABLE analytics_star_schema.dim_date AS
SELECT DISTINCT
  PARSE_DATE('%Y%m%d', date) AS session_date,        -- Parsed session date
  EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date)) AS year,   -- Year
  EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', date)) AS month, -- Month
  FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', date)) AS day_of_week -- Day of the week (e.g., Monday)
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`;

-- Create the session_products bridge table
-- Bridge table to link sessions with the products purchased
CREATE OR REPLACE TABLE analytics_star_schema.session_products AS
SELECT
  fullVisitorId AS user_id,                          -- Unique user ID
  visitId AS session_id,                             -- Session ID
  h.product.v2ProductName AS product_name,           -- Name of the product purchased
  h.product.v2ProductCategory AS product_category,   -- Category of the product
  h.product.productRevenue / 1e6 AS product_revenue_usd -- Revenue per product in USD
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
UNNEST(hits) AS h                                     -- Unnest hits to access product-level details
WHERE h.type = 'TRANSACTION';                         -- Filter only transactions
