/*
===================================================
DDL Script: Create Bronze Table
===================================================
Script Purpoe:
  This script load data and create 'bronze' schema,
  dropping existing tables if they already exists.
===================================================
*/

-- Loading CRM Tables

TRUNCATE TABLE bronze.crm_cust_info;

LOAD DATA LOCAL INFILE 'path to data'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.crm_prd_info;

LOAD DATA LOCAL INFILE 'path to data'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.crm_sales_details;

LOAD DATA LOCAL INFILE 'path to data'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- Loading ERP Tables

TRUNCATE TABLE bronze.erp_cust_az12;

LOAD DATA LOCAL INFILE 'path to data'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


TRUNCATE TABLE bronze.erp_loc_a101;

LOAD DATA LOCAL INFILE 'path to data'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

TRUNCATE TABLE bronze.erp_px_cat_g1v2;

LOAD DATA LOCAL INFILE 'path to data'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
