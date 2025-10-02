/*
=============================================================
  Stored Procedure: Load Bronze Layer (Source -> Bronze)
=============================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema 
  from external CSV files. 
It performs the following actions:
  -  Truncate the bronze tables before loading data.
  -  Uses teh 'Bulk Insert' command to load data from csv 
     files to bronze tables.

Parameters: 
  None.
  This stored procedure does not accept any parameters or return
  any values.

Usage Example:
  CALL load_bronze_layer();

DELIMITER //

CREATE PROCEDURE load_bronze_layer()
BEGIN
  -- CRM Tables
  TRUNCATE TABLE bronze.crm_cust_info;
  LOAD DATA LOCAL INFILE 'path to docs'
  INTO TABLE bronze.crm_cust_info
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

  TRUNCATE TABLE bronze.crm_prd_info;
  LOAD DATA LOCAL INFILE 'path to docs''
  INTO TABLE bronze.crm_prd_info
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

  TRUNCATE TABLE bronze.crm_sales_details;
  LOAD DATA LOCAL INFILE 'path to docs''
  INTO TABLE bronze.crm_sales_details
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

  -- ERP Tables
  TRUNCATE TABLE bronze.erp_cust_az12;
  LOAD DATA LOCAL INFILE 'path to docs''
  INTO TABLE bronze.erp_cust_az12
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

  TRUNCATE TABLE bronze.erp_loc_a101;
  LOAD DATA LOCAL INFILE 'path to docs''
  INTO TABLE bronze.erp_loc_a101
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;

  TRUNCATE TABLE bronze.erp_px_cat_g1v2;
  LOAD DATA LOCAL INFILE 'path to docs''
  INTO TABLE bronze.erp_px_cat_g1v2
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;
END//

DELIMITER ;
