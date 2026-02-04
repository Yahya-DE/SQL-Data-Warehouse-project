/*
============================================================
 Script Name : sp_data_transfer.sql
 Layer       : Silver
 Purpose     :
    - Transform and clean data from Bronze layer
    - Remove duplicates and standardize values
    - Convert data types (especially dates)
    - Prepare trusted, cleaned tables for downstream usage

 Notes:
    - Deduplication logic is applied where needed
    - Business rules are enforced in this layer
    - Silver layer reflects cleaned and conformed data
============================================================
*/

CREATE OR ALTER PROCEDURE Silver.sp_data_transfer
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        /* =====================================================
           CRM - CUSTOMER INFORMATION
        ===================================================== */
        IF OBJECT_ID('Silver.crm_cust_info', 'U') IS NOT NULL
            DROP TABLE Silver.crm_cust_info;

        CREATE TABLE Silver.crm_cust_info
        (
            cst_id             INT,
            cst_key            VARCHAR(50),
            cst_firstname      VARCHAR(50),
            cst_lastname       VARCHAR(50),
            cst_marital_status VARCHAR(50),
            cst_gndr           VARCHAR(50),
            cst_create_date    DATE
        );

        ;WITH cte_remove_duplicate AS
        (
            SELECT
                cst_id,
                cst_create_date
            FROM
            (
                SELECT
                    cst_id,
                    cst_create_date,
                    ROW_NUMBER() OVER
                    (
                        PARTITION BY cst_id
                        ORDER BY cst_create_date DESC
                    ) AS cst_rank
                FROM Bronze.crm_cust_info
            ) t
            WHERE cst_rank = 1
              AND cst_id IS NOT NULL
        )
        INSERT INTO Silver.crm_cust_info
        SELECT
            c.cst_id,
            c.cst_key,
            CASE
                WHEN TRIM(c.cst_firstname) IS NULL THEN 'n/a'
                ELSE TRIM(c.cst_firstname)
            END AS cst_firstname,
            CASE
                WHEN TRIM(c.cst_lastname) IS NULL THEN 'n/a'
                ELSE TRIM(c.cst_lastname)
            END AS cst_lastname,
            CASE UPPER(TRIM(c.cst_marital_status))
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE UPPER(TRIM(c.cst_gndr))
                WHEN 'M' THEN 'Male'
                WHEN 'F' THEN 'Female'
                ELSE 'n/a'
            END AS cst_gndr,
            c.cst_create_date
        FROM Bronze.crm_cust_info c
        WHERE EXISTS
        (
            SELECT 1
            FROM cte_remove_duplicate r
            WHERE c.cst_id = r.cst_id
              AND c.cst_create_date = r.cst_create_date
        );

        PRINT 'Data transfer to table Silver.crm_cust_info is done';
        PRINT '===========================================';

        /* =====================================================
           CRM - PRODUCT INFORMATION
        ===================================================== */
        IF OBJECT_ID('Silver.crm_prd_info', 'U') IS NOT NULL
            DROP TABLE Silver.crm_prd_info;

        CREATE TABLE Silver.crm_prd_info
        (
            prd_id       INT,
            prd_key      VARCHAR(100),
            prd_cat_key  VARCHAR(50),
            prd_nm       VARCHAR(100),
            prd_cost     VARCHAR(50),
            prd_line     VARCHAR(50),
            prd_start_dt DATE,
            prd_end_dt   DATE
        );

        INSERT INTO Silver.crm_prd_info
        SELECT
            prd_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            SUBSTRING(prd_key, 1, 5)            AS prd_cat_key,
            prd_nm,
            CASE 
                WHEN prd_cost IS NULL THEN 0
                ELSE prd_cost
            END AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Sport'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            prd_start_dt,
            DATEADD
            (
                DAY,
                -1,
                LEAD(prd_start_dt)
                OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
            ) AS prd_end_dt
        FROM Bronze.crm_prd_info;

        PRINT 'Data transfer to table Silver.crm_prd_info is done';
        PRINT '===========================================';

        /* =====================================================
           CRM - SALES DETAILS
        ===================================================== */
        IF OBJECT_ID('Silver.crm_sales_details', 'U') IS NOT NULL
            DROP TABLE Silver.crm_sales_details;

        CREATE TABLE Silver.crm_sales_details
        (
            sls_ord_num  VARCHAR(50),
            sls_prd_key  VARCHAR(50),
            sls_cust_id  INT,
            sls_order_dt DATE,
            sls_ship_dt  DATE,
            sls_due_dt   DATE,
            sls_sales    INT,
            sls_quantity INT,
            sls_price    INT
        );

        INSERT INTO Silver.crm_sales_details
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE LEN(sls_order_dt) 
                WHEN 8 THEN TRY_CAST(TRY_CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE LEN(sls_ship_dt) 
                WHEN 8 THEN TRY_CAST(TRY_CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE LEN(sls_due_dt)
                WHEN 8 THEN TRY_CAST(TRY_CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE 
                WHEN (sls_sales <= 0 OR sls_sales IS NULL OR
                      sls_sales != (sls_quantity * sls_quantity))
                     AND (sls_price > 0 AND sls_price IS NOT NULL) 
                THEN sls_quantity * sls_price
                ELSE sls_sales
            END AS sls_sales,
            CASE 
                WHEN sls_quantity <= 0 OR sls_quantity IS NULL THEN sls_sales / NULLIF(sls_price, 0)
                ELSE sls_quantity
            END AS sls_quantity,
            CASE 
                WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM Bronze.crm_sales_details;

        PRINT 'Data transfer to table Silver.crm_sales_details is done';
        PRINT '===========================================';

        /* =====================================================
           ERP - CUSTOMER
        ===================================================== */
        IF OBJECT_ID('Silver.erp_cust_az12', 'U') IS NOT NULL
            DROP TABLE Silver.erp_cust_az12;

        CREATE TABLE Silver.erp_cust_az12
        (
            cid    VARCHAR(50),
            b_date DATE,
            gen    VARCHAR(50)
        );

        INSERT INTO Silver.erp_cust_az12
        SELECT
            CASE
                WHEN LEN(SUBSTRING(CID, 4, LEN(CID))) = 7
                THEN CONCAT('AW0', SUBSTRING(CID, 4, LEN(CID)))
                ELSE SUBSTRING(CID, 4, LEN(CID))
            END AS cid,
            BDATE AS b_date,
            CASE
                WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE')   THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM Bronze.erp_cust_az12
        WHERE BDATE < GETDATE();

        PRINT 'Data transfer to table Silver.erp_cust_az12 is done';
        PRINT '===========================================';

        /* =====================================================
           ERP - LOCATION
        ===================================================== */
        IF OBJECT_ID('Silver.erp_loc_a101', 'U') IS NOT NULL
            DROP TABLE Silver.erp_loc_a101;

        CREATE TABLE Silver.erp_loc_a101
        (
            cid   VARCHAR(50),
            cntry VARCHAR(50)
        );

        INSERT INTO Silver.erp_loc_a101
        SELECT
            REPLACE(CID, '-', ''),
            CASE
                WHEN UPPER(TRIM(CNTRY)) IN ('US', 'USA') THEN 'United States'
                WHEN UPPER(TRIM(CNTRY)) = 'DE'           THEN 'Germany'
                WHEN CNTRY IS NULL OR TRIM(CNTRY) = ''  THEN 'n/a'
                ELSE CNTRY
            END AS cntry
        FROM Bronze.erp_loc_a101;

        PRINT 'Data transfer to table Silver.erp_loc_a101 is done';
        PRINT '===========================================';

        /* =====================================================
           ERP - PRODUCT CATEGORY
        ===================================================== */
        IF OBJECT_ID('Silver.erp_px_cat_g1v2', 'U') IS NOT NULL
            DROP TABLE Silver.erp_px_cat_g1v2;

        CREATE TABLE Silver.erp_px_cat_g1v2
        (
            id          VARCHAR(50),
            cat         VARCHAR(50),
            sub_cat     VARCHAR(50),
            maintenance VARCHAR(50)
        );

        INSERT INTO Silver.erp_px_cat_g1v2
        SELECT
            RE
