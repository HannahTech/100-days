/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    Script Purpose:
    This script creates a database, schemas, and after that create tables in the 'bronze' schema, dropping existing tables 
    If they already exist.
	  Run this script to redefine the DDL structure of 'bronze' Tables
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

-- Creating one database
-- If we want to keep the database
-- IF NOT EXISTS (
--     SELECT name FROM sys.databases WHERE name = 'DataWarehouse'
-- )
-- BEGIN
--     CREATE DATABASE DataWarehouse;
-- END;
-- If we want to replace the database

USE master;
GO

DROP DATABASE IF EXISTS DataWarehouse;
CREATE DATABASE DataWarehouse;
GO
-- Defaulting database
USE DataWarehouse;
GO

-- Creating Schema for grouping our related tables
IF NOT EXISTS (
    SELECT * FROM sys.schemas WHERE name = 'bronze'
)
BEGIN
    EXEC('CREATE SCHEMA bronze');
END;
GO

IF NOT EXISTS (
    SELECT * FROM sys.schemas WHERE name = 'silver'
)
BEGIN
    EXEC('CREATE SCHEMA silver');
END;
GO

IF NOT EXISTS (
    SELECT * FROM sys.schemas WHERE name = 'gold'
)
BEGIN
    EXEC('CREATE SCHEMA gold');
END;
GO

-- ---------------------------
-- Check if table exist delete them before creating new one
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

-- Creating crm_cust_info Table
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(1),
    cst_gndr VARCHAR(1),
    cst_create_date DATE
);
GO

-- ---------------------------
-- Creating crm_prd_info Table
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(100),
    prd_nm VARCHAR(100),
    prd_cost INT,
    prd_line VARCHAR(5), 
    prd_start_dt DATE,
    prd_end_dt DATE
);
GO

-- ---------------------------
-- Creating crm_sales_details Table
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key	VARCHAR(50),
    sls_cust_id	INT,
    sls_order_dt INT,
    sls_ship_dt	INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);
GO

-- ---------------------------
-- Creating erp_cust_az12 Table
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(50)
);
GO

-- ---------------------------
-- Creating erp_loc_a101 Table
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    CID VARCHAR(50),
    CNTRY VARCHAR(50)
);
GO

-- ---------------------------
-- Creating erp_px_cat_g1v2 Table
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50)
);
GO

-- Creating Procedure for adding infos inside tables:
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        SET @start_time = GETDATE();
        -- crm_cust_info Table
        -- Clear table if it has some rows
        TRUNCATE TABLE bronze.crm_cust_info;
        -- Adding all rows from csv file
        BULK INSERT bronze.crm_cust_info
        FROM '/mnt/data/datasets/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();

        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        -- ---------------------------
        -- crm_prd_info Table
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM '/mnt/data/datasets/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        -- ---------------------------
        -- crm_sales_details Table
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM '/mnt/data/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        -- ---------------------------
        -- erp_cust_az12 Table
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM '/mnt/data/datasets/source_erp/cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        -- ---------------------------
        -- erp_loc_a101 Table
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM '/mnt/data/datasets/source_erp/loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        -- ---------------------------
        -- erp_px_cat_g1v2 Table
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/mnt/data/datasets/source_erp/px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        
        -- Count all rows in tables
        SELECT 'crm_cust_info' AS table_name, COUNT(*) AS row_count FROM bronze.crm_cust_info
        UNION ALL
        SELECT 'crm_prd_info', COUNT(*) FROM bronze.crm_prd_info
        UNION ALL
        SELECT 'crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
        UNION ALL
        SELECT 'erp_cust_az12', COUNT(*) FROM bronze.erp_cust_az12
        UNION ALL
        SELECT 'erp_loc_a101', COUNT(*) FROM bronze.erp_loc_a101
        UNION ALL
        SELECT 'erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2;
    END TRY
    BEGIN CATCH
        PRINT '===================================================='
        PRINT 'Error ! Please Check Your Query'
        PRINT '===================================================='
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST(ERROR_MESSAGE() AS NVARCHAR);
        PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '===================================================='
    END CATCH
    SET @batch_end_time = GETDATE();
    PRINT '>> Load Duration of All Bronze: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
END


USE DataWarehouse;
GO

EXEC bronze.load_bronze
