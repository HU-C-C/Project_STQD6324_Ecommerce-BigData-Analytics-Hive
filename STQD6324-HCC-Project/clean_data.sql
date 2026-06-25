-- ====================================================================
-- STQD6324 Data Management Final Project
-- Script Purpose: E-Commerce Transaction Log Cleaning & Transformation
-- ====================================================================
-- Revision note: the original version of this script declared raw_sales
-- with only 6 fields and a plain comma-delimited row format. The source
-- CSV actually has 18 columns, so every field from the 3rd column onward
-- was silently misaligned, and a quoted value containing an internal
-- comma ("Korea, Rep.") additionally corrupted 53 rows. Both issues are
-- fixed below. See README.txt Section 2 for the full diagnosis.
-- ====================================================================

-- 1. Initialize Database Environment
CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

-- 2. Raw Staging Table (Quote-Aware CSV Parsing)
-- OpenCSVSerde correctly handles quoted fields with embedded commas
-- (e.g. "Korea, Rep."), unlike the default FIELDS TERMINATED BY ','
-- row format used in the original script. All columns are typed as
-- STRING here; numeric typing is restored in Step 3 below.
DROP TABLE IF EXISTS raw_sales;

CREATE EXTERNAL TABLE IF NOT EXISTS raw_sales (
    order_datetime          STRING,
    order_year              STRING,
    order_month             STRING,
    week_of_year            STRING,
    day_of_week             STRING,
    order_hour              STRING,
    is_weekend               STRING,
    country                  STRING,
    country_code             STRING,
    product_id               STRING,
    customer_id              STRING,
    unit_price_gbp           STRING,
    quantity_sold             STRING,
    sales_amount_gbp         STRING,
    population_total         STRING,
    gdp_current_usd          STRING,
    gdp_growth_pct           STRING,
    inflation_consumer_pct   STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/user/maria_dev/ecommerce_data/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 3. Corrected & Typed Table
-- Casts each STRING field back to its proper numeric type, renames
-- fields for downstream clarity, and retains the columns needed for
-- all six charts (revenue, pricing, macro indicators, country, month,
-- weekend flag).
DROP TABLE IF EXISTS cleaned_ecommerce_sales;

CREATE TABLE IF NOT EXISTS ecommerce_db.cleaned_ecommerce_sales AS
SELECT
    order_datetime                          AS transaction_timestamp,
    CAST(order_year AS INT)                 AS order_year,
    CAST(order_month AS INT)                AS order_month,
    CAST(is_weekend AS INT)                 AS is_weekend,
    country,
    CAST(unit_price_gbp AS DOUBLE)          AS unit_price_gbp,
    CAST(quantity_sold AS INT)              AS quantity_sold,
    CAST(sales_amount_gbp AS DOUBLE)        AS sales_value,
    CAST(gdp_growth_pct AS DOUBLE)          AS gdp_growth,
    CAST(inflation_consumer_pct AS DOUBLE)  AS inflation_rate
FROM ecommerce_db.raw_sales

-- 4. Enterprise Data Filtering Rules
WHERE transaction_timestamp IS NOT NULL
  AND transaction_timestamp != 'order_datetime'  -- Strips out duplicate system header rows
  AND transaction_timestamp != '';                -- Filters out damaged empty records

-- ====================================================================
-- 5. Verification Queries (run after the above to confirm correctness)
-- ====================================================================

-- 5a. Confirm the 18-column schema landed correctly
-- DESCRIBE raw_sales;
-- SELECT * FROM raw_sales LIMIT 5;

-- 5b. Confirm the quoted "Korea, Rep." rows no longer corrupt gdp_growth_pct
-- SELECT MIN(gdp_growth), MAX(gdp_growth), AVG(gdp_growth)
-- FROM cleaned_ecommerce_sales WHERE order_year = 2010;

-- 5c. Core revenue + macro aggregation (used in ambari_sql.png / ambari_chart.png)
-- SELECT order_year,
--        SUM(sales_value) AS total_revenue,
--        AVG(unit_price_gbp) AS avg_unit_price,
--        AVG(gdp_growth) AS avg_gdp_growth,
--        AVG(inflation_rate) AS avg_inflation
-- FROM ecommerce_db.cleaned_ecommerce_sales
-- GROUP BY order_year;

-- 5d. Top 10 countries by revenue (Chart 3 - pie chart)
-- SELECT country, SUM(sales_value) AS total_revenue
-- FROM ecommerce_db.cleaned_ecommerce_sales
-- GROUP BY country ORDER BY total_revenue DESC LIMIT 10;

-- 5e. Monthly revenue seasonality (Chart 4)
-- SELECT order_month, SUM(sales_value) AS total_revenue
-- FROM ecommerce_db.cleaned_ecommerce_sales
-- GROUP BY order_month ORDER BY order_month;

-- 5f. Weekday vs. weekend order behaviour (Chart 5)
-- SELECT is_weekend, SUM(sales_value) AS total_revenue, COUNT(*) AS num_orders,
--        AVG(sales_value) AS avg_order_value
-- FROM ecommerce_db.cleaned_ecommerce_sales
-- GROUP BY is_weekend;

-- 5g. Top 10 products by revenue, excluding non-product ledger entries (Chart 6)
-- 'M' = manual account adjustment, 'POST' = postage/shipping fee line item
-- SELECT product_id, SUM(CAST(sales_amount_gbp AS DOUBLE)) AS total_revenue,
--        SUM(CAST(quantity_sold AS INT)) AS total_qty
-- FROM ecommerce_db.raw_sales
-- WHERE order_datetime IS NOT NULL AND order_datetime != ''
--   AND product_id NOT IN ('M', 'POST')
-- GROUP BY product_id ORDER BY total_revenue DESC LIMIT 10;
