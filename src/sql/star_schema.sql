-- Create or replace the fact_sessions table (1 year of data, fully cleaned and enhanced)
CREATE OR REPLACE TABLE `ey-fabernovel-use-case.analytics_star_schema.fact_sessions` AS
SELECT
  -- Visitor and session identifiers
  fullVisitorId,                             -- Unique ID for the user
  visitId,                                   -- Unique ID for the session
  visitStartTime,                            -- Timestamp of the session start
  PARSE_DATE('%Y%m%d', date) AS session_date, -- Parsed session date
  visitNumber,                               -- Session number for the user

  -- Session-level totals (cleaned and casted)
  SAFE_CAST(totals.visits AS INT64) AS session_count,     -- Number of visits (must be > 0)
  SAFE_CAST(totals.hits AS INT64) AS total_hits,          -- Total number of hits in session
  SAFE_CAST(totals.pageviews AS INT64) AS pageviews,      -- Number of pageviews in session
  SAFE_CAST(IFNULL(totals.transactions, 0) AS INT64) AS transactions, -- Total transactions
  SAFE_CAST(IFNULL(totals.transactionRevenue, 0) AS FLOAT64) / 1e6 AS revenue_usd, -- Revenue in USD

  -- Flags derived from behavior
  IF(SAFE_CAST(totals.transactionRevenue AS FLOAT64) > 0, 1, 0) AS is_transaction, -- 1 if session had revenue
  IF(SAFE_CAST(totals.bounces AS INT64) = 1, 1, 0) AS bounce,                      -- 1 if session was a bounce

  -- Time features for behavior analysis
  FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', date)) AS day_of_week,   -- Day name (e.g., Monday)
  FORMAT_DATE('%B', PARSE_DATE('%Y%m%d', date)) AS month_name,    -- Month name (e.g., January)
  EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) AS hour_of_day, -- Hour of session 

  -- Device information (cleaned and normalized)
  LOWER(TRIM(IFNULL(device.deviceCategory, 'unknown'))) AS device_category,     -- Device type
  LOWER(TRIM(IFNULL(device.operatingSystem, 'unknown'))) AS operating_system,   -- OS used

  -- Geolocation (title-cased country name)
  INITCAP(IFNULL(geoNetwork.country, 'unknown')) AS country,     -- User country

  -- Traffic source information (cleaned)
  LOWER(TRIM(IFNULL(trafficSource.source, 'unknown'))) AS source,   -- Traffic source
  LOWER(TRIM(IFNULL(trafficSource.medium, 'unknown'))) AS medium    -- Traffic medium

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`

-- Only include 1 full year of data
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20170731'

-- Exclude incomplete or invalid sessions
  AND SAFE_CAST(totals.visits AS INT64) IS NOT NULL
  AND SAFE_CAST(totals.visits AS INT64) > 0
  AND fullVisitorId IS NOT NULL
  AND visitId IS NOT NULL;












-- Create the date dimension table from distinct session dates
CREATE OR REPLACE TABLE `ey-fabernovel-use-case.analytics_star_schema.dim_date` AS
SELECT
  session_date AS date,                               -- The actual session date
  EXTRACT(DAY FROM session_date) AS day,              -- Day of the month
  EXTRACT(MONTH FROM session_date) AS month,          -- Month number (1–12)
  EXTRACT(YEAR FROM session_date) AS year,            -- Year
  FORMAT_DATE('%A', session_date) AS day_name,        -- Weekday name (e.g., Monday)
  FORMAT_DATE('%B', session_date) AS month_name,      -- Month name (e.g., January)
  EXTRACT(WEEK FROM session_date) AS week_number      -- Week number of the year
FROM `ey-fabernovel-use-case.analytics_star_schema.fact_sessions`
GROUP BY session_date;










-- Create user dimension with session and revenue summaries
CREATE OR REPLACE TABLE `ey-fabernovel-use-case.analytics_star_schema.dim_users` AS
SELECT
  fullVisitorId,                                      -- Unique user ID
  COUNT(DISTINCT visitId) AS total_sessions,          -- Total number of sessions
  COUNTIF(is_transaction = 1) AS converting_sessions, -- Number of sessions with purchases
  SUM(pageviews) AS total_pageviews,                  -- Total pages viewed
  SUM(transactions) AS total_transactions,            -- Total transactions
  ROUND(SUM(revenue_usd), 2) AS total_revenue         -- Total revenue in USD
FROM `ey-fabernovel-use-case.analytics_star_schema.fact_sessions`
GROUP BY fullVisitorId;
















-- Create device dimension with distinct device + OS combos
CREATE OR REPLACE TABLE `ey-fabernovel-use-case.analytics_star_schema.dim_device` AS
SELECT DISTINCT
  device_category,     -- Device type (e.g., mobile, desktop)
  operating_system     -- OS used (e.g., iOS, Android, Windows)
FROM `ey-fabernovel-use-case.analytics_star_schema.fact_sessions`;





-- Create traffic dimension to capture distinct traffic sources and mediums
CREATE OR REPLACE TABLE `ey-fabernovel-use-case.analytics_star_schema.dim_traffic` AS
SELECT DISTINCT
  source,     -- Traffic source (e.g., google, newsletter)
  medium      -- Traffic medium (e.g., cpc, organic, referral)
FROM `ey-fabernovel-use-case.analytics_star_schema.fact_sessions`;



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
