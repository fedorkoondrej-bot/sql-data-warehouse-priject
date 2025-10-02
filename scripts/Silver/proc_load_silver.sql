/*
===========================================================================
  Stored Procedure: Load Silver Layer (Bronze -> Silver)
===========================================================================
Script Purpose:
  	This stored procedure performs ETL (Extract, Transform, Load) process to
	populate the 'silver' schema tables from the 'bronze' schema 
Actions Performed:
	-	Truncates Silver tables
	-	Insert transformed and cleansed data from Bronze into Silver tables

Parameters: 
  None.
  This stored procedure does not accept any parameters or return
  any values.

Usage Example:
  CALL load_bronze_layer();
===========================================================================
*/


DELIMITER //


CREATE PROCEDURE silver.bronze_to_silver ()
BEGIN
	-- remove old records from the silver table if needed
	TRUNCATE TABLE silver.crm_cust_info;

	-- insert transformed, clean data from bronze to silver

	INSERT INTO silver.crm_cust_info (
	  cst_id,
	  cst_key,
	  cst_firstname,
	  cst_lastname,
	  cst_marital_status,
	  cst_gndr,
	  cst_create_date
	)
	SELECT
	  cst_id,
	  cst_key,
	  TRIM(cst_firstname) AS cst_firstname,
	  TRIM(cst_lastname) AS cst_lastname,
	  CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	  END AS cst_marital_status,  -- Normalize marital status values to readable format
	  CASE
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	  END AS cst_gndr, -- Normalize gender values to readable format
	  cst_create_date
	FROM (
	  SELECT *,
			 ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last -- removing duplicates
	  FROM bronze.crm_cust_info
	  WHERE cst_create_date IS NOT NULL
		AND CAST(cst_create_date AS CHAR) != '0000-00-00'
	) AS subq
	WHERE flag_last = 1; -- Select the most recent record per customer

	TRUNCATE TABLE silver.crm_prd_info;

	INSERT INTO silver.crm_prd_info (
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
	  prd_info,
	  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
	  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,	-- Extract Product Key
	  prd_nm,
	  prd_cost,
	  CASE
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	  END AS prd_line,  -- Map product linbe codes to descriptive values
	  CAST(prd_start_dt AS DATE) AS prd_start_dt,
	  CAST(
		DATE_SUB(
		  LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
		  INTERVAL 1 DAY
		) AS DATE
	  ) AS prd_end_dt 		-- Calculate end date asone day before the next start date
	FROM bronze.crm_prd_info;

	TRUNCATE TABLE silver.crm_sales_details;

	INSERT INTO silver.crm_sales_details (
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
	CASE 
	  WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS CHAR)) != 8 THEN NULL
	  ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
	END AS sls_order_dt																    -- handling invalid date format
	,
	CASE 
	  WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS CHAR)) != 8 THEN NULL
	  ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
	END AS sls_ship_dt, 																-- handling invalid date format
	CASE 
	  WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS CHAR)) != 8 THEN NULL
	  ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
	END AS sls_due_dt, 																	-- handling invalid date format
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,																	-- Recalculater sales if original value is missing or incorect
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)											-- Derive price if original value is incorect
		ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details;

	TRUNCATE TABLE silver.erp_cust_az12;

	INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)

	SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))	-- remove 'NAS' prefix if present
		ELSE cid
	END cid,
	CASE WHEN bdate > CURDATE() THEN NULL
		ELSE bdate
	END AS bdate,	-- set future birthdates to NULL
	CASE 
	  WHEN UPPER(TRIM(gen)) LIKE 'F%' OR UPPER(TRIM(gen)) LIKE 'FEMALE%' THEN 'Female'
	  WHEN UPPER(TRIM(gen)) LIKE 'M%' OR UPPER(TRIM(gen)) LIKE 'MALE%' THEN 'Male'
	  ELSE 'n/a'
	END AS gen		-- normalize gender values and handle unknow cases
	FROM bronze.erp_cust_az12;

	TRUNCATE TABLE silver.erp_loc_a101;

	INSERT INTO silver.erp_loc_a101 (cid, cntry)
	SELECT
	  REPLACE(cid, '-', '') AS cid,
	  CASE
		WHEN UPPER(TRIM(cntry)) LIKE 'DE%' OR UPPER(TRIM(cntry)) LIKE 'GERMANY%' THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) LIKE 'US%' OR UPPER(TRIM(cntry)) LIKE 'UNITED%' THEN 'United States'
		WHEN UPPER(TRIM(cntry)) LIKE 'GB%' OR UPPER(TRIM(cntry)) LIKE 'UK%' OR UPPER(TRIM(cntry)) LIKE 'UNITED KINGDOM%' THEN 'United Kingdom'
		WHEN UPPER(TRIM(cntry)) LIKE 'FR%' OR UPPER(TRIM(cntry)) LIKE 'FRANCE%' THEN 'France'
		WHEN UPPER(TRIM(cntry)) LIKE 'CA%' OR UPPER(TRIM(cntry)) LIKE 'CANADA%' THEN 'Canada'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	  END AS cntry
	FROM bronze.erp_loc_a101;


	TRUNCATE TABLE silver.erp_loc_a101;

	INSERT INTO silver.erp_px_cat_g1v2
	(id, cat, subcat, maintenance)

	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
END

DELIMITER ;
