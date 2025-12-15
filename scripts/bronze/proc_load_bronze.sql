/* 
===============================================================================================
Stored Procedure:Load Bronze Layer(Source->Layer)
===============================================================================================
*/

EXEC bronze.load_bronze;



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME,@st_time DATETIME,@ed_time DATETIME;
BEGIN TRY
	PRINT '==============================';
	PRINT 'Loading Bronze Layer';
	PRINT '==============================';

	SET @st_time=GETDATE();
	PRINT '==============================';
	PRINT 'Loading CRM Tables';
	PRINT '==============================';

	SET @start_time = GETDATE();

	PRINT '>>Inserting Data into: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\ACIAGO\Downloads\Data WareHouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR=',',
		TABLOCK
	);

	SET @end_time = GETDATE();

	PRINT '>>Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

	PRINT '------------------------------------------------------'


	SET @start_time = GETDATE();

	TRUNCATE TABLE bronze.crm_prd_info;

	PRINT '>>Inserting Data into: bronze.crm_prd_info';
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\ACIAGO\Downloads\Data WareHouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
	SET @end_time = GETDATE();

	PRINT '>>Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

	PRINT '------------------------------------------------------'

	SET @start_time = GETDATE();

	TRUNCATE TABLE bronze.crm_sales_details;
	PRINT '>>Inserting Data into: crm_sales_details';
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\ACIAGO\Downloads\Data WareHouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
	SET @end_time = GETDATE();

	PRINT '>>Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

	PRINT '------------------------------------------------------'

	PRINT '==============================';
	PRINT 'Loading ERP Tables';
	PRINT '==============================';
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_loc_a101;
	PRINT '>>Inserting Data into:  bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\ACIAGO\Downloads\Data WareHouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
	SET @end_time = GETDATE();

	PRINT '>>Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

	PRINT '------------------------------------------------------'
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_cust_az12;
	PRINT '>>Inserting Data into: bronze.erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\ACIAGO\Downloads\Data WareHouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
	SET @end_time = GETDATE();

	PRINT '>>Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

	PRINT '------------------------------------------------------'

	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	PRINT '>>Inserting Data into: bronze.erp_px_cat_g1v2';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\ACIAGO\Downloads\Data WareHouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
	SET @end_time = GETDATE();

	PRINT '>>Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

	PRINT '------------------------------------------------------'

	SET @ed_time=GETDATE();

	PRINT '>>Load Duration Of Whole Bronze Layer: '+ CAST(DATEDIFF(second,@st_time,@ed_time) AS NVARCHAR) + 'seconds'
	END TRY

	BEGIN CATCH
		PRINT '=======================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE ' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '=======================';
	END CATCH
 END
