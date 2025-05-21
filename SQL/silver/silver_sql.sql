Use DataWarehouse;

-- ********************************************************
-- ********************************************************
-- First Table: crm_cust_info
-- ********************************************************
-- ********************************************************
SELECT TOP (10) *  FROM [DataWarehouse].[bronze].[crm_cust_info];

-- Remove NULL and Duplicates
SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1 OR cst_id IS NULL;

SELECT *  FROM [bronze].[crm_cust_info] WHERE cst_id = 29466;

SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

SELECT
*
FROM(
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;

-- Remove unwanted spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);


SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM(
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;

-- Check the consistency of values in low cardinality columns
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     Else 'N/A'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     Else 'N/A'
END cst_gndr,
cst_create_date
FROM(
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;


TRUNCATE TABLE silver.crm_cust_info;
PRINT '>>> Inserting Data Into silver.crm_cust_info';
-- Insert inside silver table
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date)
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     Else 'N/A'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     Else 'N/A'
END cst_gndr,
cst_create_date
FROM(
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;


-- Checking Quality of Data:
-- Check Nulls and Duplicates:
SELECT cst_id, COUNT(*) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1 OR cst_id IS NULL;

-- Check for Unwanted space:
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Check data standarization and consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;


-- ********************************************************
-- ********************************************************
-- Second Table: crm_prd_info
-- ********************************************************
-- ********************************************************

SELECT * FROM bronze.crm_prd_info;

-- Remove NULL and Duplicates
SELECT prd_id, COUNT(*) FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT (*) > 1 OR prd_id IS NULL;

-- Split prd_key -> CO-RF-FR-R92B-58 
SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
    SELECT sls_prd_key FROM bronze.crm_sales_details
);

-- Checking unwanted spaces
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for Nulls or Negative Numbers
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
    SELECT sls_prd_key FROM bronze.crm_sales_details
);

-- Data Standardization & Consistency
SELECT DISTINCT prd_line 
FROM bronze.crm_prd_info;

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
     WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
     WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
     Else 'N/A'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
    SELECT sls_prd_key FROM bronze.crm_sales_details
);


-- Quick CASE WHEN
SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Other Sales'
    WHEN 'T' THEN 'Touring'
    Else 'N/A'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
    SELECT sls_prd_key FROM bronze.crm_sales_details
);

-- Check For invalid date orders
SELECT * FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Put start date of next to end date of previous one
SELECT 
  prd_id,
  prd_key,
  prd_nm,
  prd_start_dt,
  prd_end_dt,
  DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');


SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Other Sales'
    WHEN 'T' THEN 'Touring'
    Else 'N/A'
END AS prd_line,
prd_start_dt,
prd_end_dt,
DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
    SELECT sls_prd_key FROM bronze.crm_sales_details
);

-- We need to edit table types:
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id VARCHAR(100),
    prd_key VARCHAR(100),
    prd_nm VARCHAR(100),
    prd_cost INT,
    prd_line VARCHAR(50), 
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()    
);

TRUNCATE TABLE silver.crm_prd_info;
PRINT '>>> Inserting Data Into silver.crm_prd_info';
-- Insert to silver table:
INSERT INTO silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Other Sales'
    WHEN 'T' THEN 'Touring'
    Else 'N/A'
END AS prd_line,
prd_start_dt,
DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
    SELECT sls_prd_key FROM bronze.crm_sales_details
);

SELECT * FROM silver.crm_prd_info;


-- ********************************************************
-- ********************************************************
-- Third Table: crm_sales_details
-- ********************************************************
-- ********************************************************


SELECT * FROM bronze.crm_sales_details;

-- Checking unwanted space
SELECT * FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Checking prd_key which are not in crm_prd_info table
SELECT * FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Checking cust_id which are not in crm_cust_info table
SELECT * FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Checking negative and zero dates:
SELECT NULLIF(sls_order_dt, 0) sls_order_dt 
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt < 19000101 
OR sls_order_dt > 20500101;


SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details;

-- sls_ship_dt column
SELECT NULLIF(sls_ship_dt, 0) sls_ship_dt 
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt < 19000101 
OR sls_ship_dt > 20500101;

-- sls_due_dt column
SELECT NULLIF(sls_due_dt, 0) sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt < 19000101 
OR sls_due_dt > 20500101;



SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details;


-- Checking Order date be before Shipping date and Due date
SELECT * FROM bronze.crm_sales_details WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;
SELECT * FROM bronze.crm_sales_details WHERE sls_ship_dt > sls_due_dt;

-- Checking Data Consistency for Sales 
SELECT 
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details 
WHERE sls_sales != (sls_quantity * sls_price) 
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
OR sls_price <= 0 OR sls_quantity <= 0 OR sls_sales <= 0;


-- Order them:
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details 
WHERE sls_sales != (sls_quantity * sls_price) 
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
OR sls_price <= 0 OR sls_quantity <= 0 OR sls_sales <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- Transformation based on rules:
-- 1) If Sales is negative, zero, or null, derive it using Quantity and Price.
-- 2) If Price is zero or null, calculate it using Sales and Quantity.
-- 3) If Price is negative, convert it to a positive value
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details 
WHERE sls_sales != (sls_quantity * sls_price) 
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
OR sls_price <= 0 OR sls_quantity <= 0 OR sls_sales <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

TRUNCATE TABLE silver.crm_sales_details;
PRINT '>>> Inserting Data Into silver.crm_sales_details';
-- put cases inside the query and insert into table:
INSERT INTO silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;


SELECT * FROM silver.crm_sales_details;


-- ********************************************************
-- ********************************************************
-- Forth Table: erp_cust_az12
-- ********************************************************
-- ********************************************************

SELECT * FROM bronze.erp_cust_az12;


WITH b AS (
  SELECT CID, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
  FROM bronze.erp_cust_az12
),
s AS (
  SELECT cst_id, cst_key, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
  FROM silver.crm_cust_info
)
SELECT 
  b.CID, 
  s.cst_id, 
  s.cst_key
FROM b
JOIN s ON b.rn = s.rn
WHERE b.rn <= 10;

SELECT
  COUNT(CASE WHEN cid LIKE 'NAS%' THEN 1 END) AS count_nas,
  COUNT(CASE WHEN cid NOT LIKE 'NAS%' THEN 1 END) AS count_not_nas,
  COUNT(CASE WHEN cid NOT LIKE 'NAS%' AND cid LIKE 'AW%' THEN 1 END) AS count_aw_not_nas
FROM bronze.erp_cust_az12;

-- Remove NAS from first of cid:
SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12;


-- Check all cid are inside crm_cust_info table:
SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE CID
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Query
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12;

-- Check previous and future birthdates
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Query
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
    ELSE bdate
END AS bdate,
gen
FROM bronze.erp_cust_az12;


SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'N/A'
END AS gen
FROM bronze.erp_cust_az12;


SELECT DISTINCT gen,
  CASE 
    WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('M', 'MALE') THEN 'Male'
    ELSE 'N/A'
  END AS standardized_gen
FROM bronze.erp_cust_az12;

TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>>> Inserting Data Into silver.erp_cust_az12';
-- Insert inside silver table
INSERT INTO silver.erp_cust_az12(
    cid,
    bdate,
    gen
)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
    ELSE bdate
END AS bdate,
CASE 
    WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('M', 'MALE') THEN 'Male'
    ELSE 'N/A'
  END AS gen
FROM bronze.erp_cust_az12;


SELECT * FROM silver.erp_cust_az12;


-- ********************************************************
-- ********************************************************
-- Fifth Table: erp_loc_a101
-- ********************************************************
-- ********************************************************

SELECT * FROM bronze.erp_loc_a101;

-- Compare cid and cst_key
WITH b AS (
  SELECT CID, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
  FROM bronze.erp_loc_a101
),
s AS (
  SELECT cst_key, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
  FROM silver.crm_cust_info
)
SELECT 
  b.CID,
  s.cst_key
FROM b
JOIN s ON b.rn = s.rn
WHERE b.rn <= 10;


-- Remove dash sign after AW
SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101;

-- Check availability ID in both tables
SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101 
WHERE REPLACE(cid, '-', '') NOT IN
(SELECT cst_key FROM bronze.crm_cust_info);

-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY CNTRY;

-- Correction of countries, normalize and handle missing or blank country codes
SELECT 
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
     ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

-- If the above query didn't work:
SELECT 
  REPLACE(cid, '-', '') AS cid,
  CASE 
    WHEN UPPER(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) = 'DE' THEN 'Germany'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('US', 'USA') THEN 'United States'
    WHEN cntry IS NULL OR 
         LEN(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) = 0 THEN 'N/A'
    ELSE 
      REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')
  END AS cntry
FROM bronze.erp_loc_a101;


SELECT DISTINCT
cntry AS old_cntry,
CASE 
    WHEN UPPER(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) = 'DE' THEN 'Germany'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('US', 'USA') THEN 'United States'
    WHEN cntry IS NULL OR 
         LEN(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) = 0 THEN 'N/A'
    ELSE 
      REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>>> Inserting Data Into silver.erp_loc_a101';
-- Insert inside siver table:
INSERT INTO silver.erp_loc_a101
(cid, cntry)
SELECT 
  REPLACE(cid, '-', '') AS cid,
  CASE 
    WHEN UPPER(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) = 'DE' THEN 'Germany'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('US', 'USA') THEN 'United States'
    WHEN cntry IS NULL OR 
         LEN(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) = 0 THEN 'N/A'
    ELSE 
      REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')
  END AS cntry
FROM bronze.erp_loc_a101;

SELECT * FROM silver.erp_loc_a101;

-- ********************************************************
-- ********************************************************
-- Sixth Table: erp_px_cat_g1v2
-- ********************************************************
-- ********************************************************
-- Compare IDs
SELECT TOP(4) id FROM bronze.erp_px_cat_g1v2;
SELECT TOP(4) prd_id, cat_id, prd_key FROM silver.crm_prd_info;

-- Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE CAT != TRIM(CAT) OR SUBCAT != TRIM(SUBCAT) OR MAINTENANCE != TRIM(MAINTENANCE);

-- Data Standardization & Consistency
SELECT DISTINCT
CAT
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
SUBCAT
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
MAINTENANCE
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT
  REPLACE(REPLACE(LTRIM(RTRIM(MAINTENANCE)), CHAR(13), ''), CHAR(10), '') AS MAINTENANCE
FROM bronze.erp_px_cat_g1v2;

TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>>> Inserting Data Into silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2
(id, cat, subcat, maintenance)
SELECT
id,
cat,
subcat,
REPLACE(REPLACE(LTRIM(RTRIM(MAINTENANCE)), CHAR(13), ''), CHAR(10), '') AS MAINTENANCE
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2;

-- Now we put all code inside procedure:
CREATE OR ALTER PROCEDURE silver.initial_load_silver AS
BEGIN
    TRUNCATE TABLE silver.crm_cust_info;
    PRINT '>>> Inserting Data Into silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date)
    SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        Else 'N/A'
    END cst_marital_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        Else 'N/A'
    END cst_gndr,
    cst_create_date
    FROM(
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    )t WHERE flag_last = 1;

    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>>> Inserting Data Into silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        Else 'N/A'
    END AS prd_line,
    prd_start_dt,
    DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
    FROM bronze.crm_prd_info
    WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
        SELECT sls_prd_key FROM bronze.crm_sales_details
    );

    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>>> Inserting Data Into silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
    FROM bronze.crm_sales_details;

    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT '>>> Inserting Data Into silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12(
        cid,
        bdate,
        gen
    )
    SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END cid,
    CASE WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,
    CASE 
        WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('M', 'MALE') THEN 'Male'
        ELSE 'N/A'
    END AS gen
    FROM bronze.erp_cust_az12;

    TRUNCATE TABLE silver.erp_loc_a101;
    PRINT '>>> Inserting Data Into silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101
    (cid, cntry)
    SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN UPPER(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) = 'DE' THEN 'Germany'
        WHEN UPPER(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('US', 'USA') THEN 'United States'
        WHEN cntry IS NULL OR 
            LEN(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) = 0 THEN 'N/A'
        ELSE 
        REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')
    END AS cntry
    FROM bronze.erp_loc_a101;

    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    PRINT '>>> Inserting Data Into silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2
    (id, cat, subcat, maintenance)
    SELECT
    id,
    cat,
    subcat,
    REPLACE(REPLACE(LTRIM(RTRIM(MAINTENANCE)), CHAR(13), ''), CHAR(10), '') AS MAINTENANCE
    FROM bronze.erp_px_cat_g1v2;

END;
GO

EXEC silver.initial_load_silver;


-- **************************************************************************************
-- **************************************************************************************
-- **************************************************************************************
-- **************************************************************************************

-- Now we add messages, error handling, duration of each step, and duration of loading

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';
            PRINT '------------------------------------------------';
            PRINT 'Loading CRM Tables';
            PRINT '------------------------------------------------';
            -- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
            PRINT '>> Truncating Table: silver.crm_cust_info';
            TRUNCATE TABLE silver.crm_cust_info;
            PRINT '>>> Inserting Data Into silver.crm_cust_info';
            INSERT INTO silver.crm_cust_info (
                cst_id,
                cst_key,
                cst_firstname,
                cst_lastname,
                cst_marital_status,
                cst_gndr,
                cst_create_date)
            SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                Else 'N/A'
            END cst_marital_status,
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                Else 'N/A'
            END cst_gndr,
            cst_create_date
            FROM(
                SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
                FROM bronze.crm_cust_info
                WHERE cst_id IS NOT NULL
            )t WHERE flag_last = 1;
            SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

            -- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
            PRINT '>> Truncating Table: silver.crm_prd_info';
            TRUNCATE TABLE silver.crm_prd_info;
            PRINT '>>> Inserting Data Into silver.crm_prd_info';
            INSERT INTO silver.crm_prd_info(
                prd_id,
                cat_id,
                prd_key,
                prd_nm,
                prd_cost,
                prd_line,
                prd_start_dt,
                prd_end_dt
            )
            SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                Else 'N/A'
            END AS prd_line,
            prd_start_dt,
            DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
            FROM bronze.crm_prd_info
            WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
                SELECT sls_prd_key FROM bronze.crm_sales_details
            );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

            -- Loading silver.crm_sales_details
        SET @start_time = GETDATE();
            PRINT '>> Truncating Table: silver.crm_sales_details';
            TRUNCATE TABLE silver.crm_sales_details;
            PRINT '>>> Inserting Data Into silver.crm_sales_details';
            INSERT INTO silver.crm_sales_details(
                sls_ord_num,
                sls_prd_key,
                sls_cust_id,
                sls_order_dt,
                sls_ship_dt,
                sls_due_dt,
                sls_sales,
                sls_quantity,
                sls_price
            )
            SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <= 0
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
            FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

            -- Loading silver.erp_cust_az12
        SET @start_time = GETDATE();
            PRINT '>> Truncating Table: silver.erp_cust_az12';
            TRUNCATE TABLE silver.erp_cust_az12;
            PRINT '>>> Inserting Data Into silver.erp_cust_az12';
            INSERT INTO silver.erp_cust_az12(
                cid,
                bdate,
                gen
            )
            SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END cid,
            CASE WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE 
                WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('M', 'MALE') THEN 'Male'
                ELSE 'N/A'
            END AS gen
            FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

            -- Loading silver.erp_loc_a101
        SET @start_time = GETDATE();
            PRINT '>> Truncating Table: silver.erp_loc_a101';
            TRUNCATE TABLE silver.erp_loc_a101;
            PRINT '>>> Inserting Data Into silver.erp_loc_a101';
            INSERT INTO silver.erp_loc_a101
            (cid, cntry)
            SELECT 
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN UPPER(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) = 'DE' THEN 'Germany'
                WHEN UPPER(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) IN ('US', 'USA') THEN 'United States'
                WHEN cntry IS NULL OR 
                    LEN(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')) = 0 THEN 'N/A'
                ELSE 
                REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), ' ', '')
            END AS cntry
            FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

            -- Loading silver.erp_px_cat_g1v2
        SET @start_time = GETDATE();
            PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
            TRUNCATE TABLE silver.erp_px_cat_g1v2;
            PRINT '>>> Inserting Data Into silver.erp_px_cat_g1v2';
            INSERT INTO silver.erp_px_cat_g1v2
            (id, cat, subcat, maintenance)
            SELECT
            id,
            cat,
            subcat,
            REPLACE(REPLACE(LTRIM(RTRIM(MAINTENANCE)), CHAR(13), ''), CHAR(10), '') AS MAINTENANCE
            FROM bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END;
GO

EXEC silver.load_silver;
