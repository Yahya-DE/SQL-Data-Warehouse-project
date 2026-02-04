/*
============================================================
 Script Name : sp_loading_data.sql
 Layer       : Bronze
 Purpose     :
    - Load raw data from CSV files into Bronze tables
    - Truncate existing data before each load
    - Preserve source data without any transformation
    - Measure execution time for each load step

 Notes:
    - BULK INSERT is used for performance and simplicity
    - File paths are environment-specific
    - CSV file paths are local and must be updated
      according to the user's environment before execution
============================================================
*/

CREATE OR ALTER PROCEDURE Bronze.sp_loading_data
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        DECLARE 
            @start_time DATETIME2(3),
            @end_time   DATETIME2(3);

        /* =====================================================
           CRM - CUSTOMER INFORMATION
        ===================================================== */
        TRUNCATE TABLE Bronze.crm_cust_info;
        PRINT 'Truncating table: Bronze.crm_cust_info';

        SET @start_time = SYSDATETIME();
        PRINT 'Loading data into Bronze.crm_cust_info';

        BULK INSERT Bronze.crm_cust_info
        FROM 'C:\Users\DECRYPT\Downloads\DWH\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH
        (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = SYSDATETIME();
        PRINT 'Execution time (seconds): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR);
        PRINT '========================';

        /* =====================================================
           CRM - PRODUCT INFORMATION
        ===================================================== */
        TRUNCATE TABLE Bronze.crm_prd_info;
        PRINT 'Truncating table: Bronze.crm_prd_info';

        SET @start_time = SYSDATETIME();
        PRINT 'Loading data into Bronze.crm_prd_info';

        BULK INSERT Bronze.crm_prd_info
        FROM 'C:\Users\DECRYPT\Downloads\DWH\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH
        (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = SYSDATETIME();
        PRINT 'Execution time (seconds): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR);
        PRINT '========================';

        /* =====================================================
           CRM - SALES DETAILS
        ===================================================== */
        TRUNCATE TABLE Bronze.crm_sales_details;
        PRINT 'Truncating table: Bronze.crm_sales_details';

        SET @start_time = SYSDATETIME();
        PRINT 'Loading data into Bronze.crm_sales_details';

        BULK INSERT Bronze.crm_sales_details
        FROM 'C:\Users\DECRYPT\Downloads\DWH\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH
        (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = SYSDATETIME();
        PRINT 'Execution time (seconds): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR);
        PRINT '========================';

        /* =====================================================
           ERP - CUSTOMER
        ===================================================== */
        TRUNCATE TABLE Bronze.erp_cust_az12;
        PRINT 'Truncating table: Bronze.erp_cust_az12';

        SET @start_time = SYSDATETIME();
        PRINT 'Loading data into Bronze.erp_cust_az12';

        BULK INSERT Bronze.erp_cust_az12
        FROM 'C:\Users\DECRYPT\Downloads\DWH\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH
        (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = SYSDATETIME();
        PRINT 'Execution time (seconds): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR);
        PRINT '========================';

        /* =====================================================
           ERP - LOCATION
        ===================================================== */
        TRUNCATE TABLE Bronze.erp_loc_a101;
        PRINT 'Truncating table: Bronze.erp_loc_a101';

        SET @start_time = SYSDATETIME();
        PRINT 'Loading data into Bronze.erp_loc_a101';

        BULK INSERT Bronze.erp_loc_a101
        FROM 'C:\Users\DECRYPT\Downloads\DWH\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH
        (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = SYSDATETIME();
        PRINT 'Execution time (seconds): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR);
        PRINT '========================';

        /* =====================================================
           ERP - PRODUCT CATEGORY
        ===================================================== */
        TRUNCATE TABLE Bronze.erp_px_cat_g1v2;
        PRINT 'Truncating table: Bronze.erp_px_cat_g1v2';

        SET @start_time = SYSDATETIME();
        PRINT 'Loading data into Bronze.erp_px_cat_g1v2';

        BULK INSERT Bronze.erp_px_cat_g1v2
        FROM 'C:\Users\DECRYPT\Downloads\DWH\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH
        (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = SYSDATETIME();
        PRINT 'Execution time (seconds): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR);
        PRINT '========================';

    END TRY
    BEGIN CATCH
        PRINT '=========================================';
        PRINT 'Error Line   : ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '=========================================';
    END CATCH
END;
GO
