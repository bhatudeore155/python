-- Sample SQL ETL script
-- This file shows typical extract, transform and load steps using plain SQL.
-- Adjust object names and column types to match your database.

-- 1. Extract: read from source table (e.g. sales data from staging schema)
-- (In practice this might be a SELECT FROM a CSV-loaded table or remote source.)
---- this etl only for learning purpose, not for production use
CREATE TABLE IF NOT EXISTS staging.sales_raw (
    order_id    INT,
    customer_id INT,
    product_id  INT,
    quantity    INT,
    price       DECIMAL(10,2),
    order_date  DATE
);

-- Example data insertion into staging (would usually be handled by COPY/LOAD command)
-- INSERT INTO staging.sales_raw VALUES
--   (1, 100, 200, 2, 19.99, '2023-01-01'),
--   (2, 101, 201, 1, 5.50,  '2023-01-02');

-- 2. Transform: clean and aggregate

-- remove duplicates
CREATE TABLE IF NOT EXISTS staging.sales_dedup AS
SELECT DISTINCT *
FROM staging.sales_raw;

-- compute total price and filter out small orders
CREATE TABLE IF NOT EXISTS staging.sales_transformed AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    price,
    (quantity * price) AS total_price,
    order_date
FROM staging.sales_dedup
WHERE quantity > 0
  AND price >= 0.0;

-- 3. Load: move into production (fact table)

CREATE TABLE IF NOT EXISTS warehouse.sales_facts (
    order_id      INT PRIMARY KEY,
    customer_id   INT,
    product_id    INT,
    quantity      INT,
    price         DECIMAL(10,2),
    total_price   DECIMAL(12,2),
    order_date    DATE
);

INSERT INTO warehouse.sales_facts
SELECT * FROM staging.sales_transformed;

-- Optionally, you could materialize aggregates or refresh summary tables here.

-- Clean up staging tables if desired
-- DROP TABLE staging.sales_raw;
-- DROP TABLE staging.sales_dedup;
-- DROP TABLE staging.sales_transformed;
