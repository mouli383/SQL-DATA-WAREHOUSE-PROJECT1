
EXEC silver.load_silver;
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME,@en_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time=GETDATE();
	PRINT '=====================================================';
	PRINT 'Loading Silver Layer';
	PRINT '=====================================================';

	PRINT '-----------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '-----------------------------------------------------';

	--Loading silver.crm_cust_info
	SET @start_time=GETDATE();
	PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Into: Silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_martial_status,cst_gndr,cst_create_date)
	select cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE UPPER(TRIM(cst_martial_status))
	WHEN 'S' THEN 'Single'
	WHEN 'M' THEN 'Married'
	ELSE 'n/a'
	END AS cst_martial_status,
	CASE UPPER(TRIM(cst_gndr)) 
	WHEN 'M' THEN 'Male'
	WHEN 'F' THEN 'Female'
	ELSE 'n/a'
	END AS cst_gndr,cst_create_date
	from(SELECT *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info)t
	WHERE flag_last=1 AND cst_id IS NOT NULL
	SET @en_time=GETDATE();
	PRINT 'Load Duration: '+cast(datediff(second,@start_time,@en_time)AS Nvarchar)+' seconds';
	PRINT '>>------------------';

	PRINT '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting Data Into: Silver.crm_prd_info';
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
	SELECT prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_')AS cat_id,  -- Extract category ID
	SUBSTRING(prd_key,7,LEN(prd_key)) AS pd_key,       -- Extract product key
	prd_nm,COALESCE(prd_cost,0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
	END AS prd_line,  --Map product line codes to descriptive values
	CAST(prd_start_dt AS DATE),
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- Calculate end date as one day before the next start date
	FROM bronze.crm_prd_info


	PRINT '>> Truncating Table: silver.crm_sales_Details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting Data Into: Silver.crm_sales_Details';

	INSERT INTO 
		silver.crm_sales_details
			(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			when sls_orders_dt =0 OR LEN(sls_orders_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_orders_dt AS VARCHAR)AS DATE)
		END AS sls_order_dt, 
		CASE 
			when sls_ship_dt =0 OR LEN(sls_ship_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
		END AS sls_ship_dt,
		CASE 
			when sls_due_dt =0 OR LEN(sls_due_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_Sales IS NULL OR sls_sales <=0 OR sls_Sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price<=0
			THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details


	PRINT '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting Data Into: Silver.erp_cust_az12';

	INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid 
	END cid,
	CASE WHEN bdate > GETDATE() THEN NULL 
		ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
		ELSE 'n/a'
	END AS gen 
	FROM bronze.erp_cust_az12

	PRINT '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inserting Data Into: Silver.erp_loc_a101';

	INSERT INTO silver.erp_loc_a101(cid,cntry)
	SELECT REPLACE(cid,'-','') AS cid,
	CASE WHEN TRIM(cntry) ='DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US','USA','United States') THEN 'United States'
		WHEN TRIM(cntry) =''  OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_loc_a101

	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting Data Into: Silver.erp_px_cat_g1v2';
	INSERT into silver.erp_px_cat_g1v2(id,cat,subcat,maintainance)
	SELECT id,
	cat,
	subcat,
	maintainance 
	FROM bronze.erp_px_cat_g1v2;
END;
