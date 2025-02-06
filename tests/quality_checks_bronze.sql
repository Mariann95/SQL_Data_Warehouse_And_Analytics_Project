/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'bronze' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

	These data quality issues are resolved while loading the data into the Silver Layer.

Usage Notes:
    - Run these checks after data loading Bronze Layer.
===============================================================================
*/


-- =============================================
-- bronze.crm_cust_info quality check
-- =============================================

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Data Standardization & Consistency
SELECT
	DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT
	DISTINCT cst_marital_status
FROM bronze.crm_cust_info;


-- =============================================
-- bronze.crm_prd_info quality check
-- =============================================

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Ensure we can connect the 2 tables
SELECT
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2);

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT
	prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Numbers
-- Expectation: No Result
SELECT
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT
	DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt;


-- =============================================
-- bronze.crm_sales_details quality check
-- =============================================

-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT
	sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
	OR LEN(sls_order_dt) != 8
	OR sls_order_dt < 20100101
	OR sls_order_dt > 20250101;

SELECT
	sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0
	OR LEN(sls_ship_dt) != 8
	OR sls_ship_dt < 20100101
	OR sls_ship_dt > 20250101;

SELECT
	sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
	OR LEN(sls_due_dt) != 8
	OR sls_due_dt < 20100101
	OR sls_due_dt > 20250101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Result
SELECT
	*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
	OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Between Sales, Quantity, and Price
-- Sales = Quantity * Price
-- Values must not be NULL, 0, or negative.
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_quantity * sls_price != sls_sales
	OR sls_sales IS NULL
	OR sls_quantity IS NULL
	OR sls_price IS NULL
	OR sls_sales <= 0
	OR sls_quantity <= 0
	OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- Rules: If Sales is negative, 0 or NULL, derive it using Quantity and Price.
-- If Price is 0 or NULL, calculate it using Sales and Quantity.
-- If Price is negative, convert it to a positive value.
SELECT DISTINCT
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price AS old_sls_price,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END sls_sales,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END sls_price
FROM bronze.crm_sales_details
WHERE sls_quantity * sls_price != sls_sales
	OR sls_sales IS NULL
	OR sls_quantity IS NULL
	OR sls_price IS NULL
	OR sls_sales <= 0
	OR sls_quantity <= 0
	OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-- =============================================
-- bronze.erp_cust_az12 quality check
-- =============================================

-- Ensure we can connect the 2 tables
SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Identify Out-of-range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01'
	OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT
	gen
FROM bronze.erp_cust_az12;


-- =============================================
-- bronze.erp_loc_a101
-- =============================================

-- Ensure we can connect the 2 tables
SELECT
	REPLACE(cid, '-', ''),
	cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Data Standardization & Consistency
SELECT DISTINCT
	cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;


-- =============================================
-- bronze.erp_px_cat_g1v2
-- =============================================

-- Check for unwanted spaces
-- Expectation: No Results
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
	OR subcat != TRIM(subcat)
	OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT
	cat
FROM bronze.erp_px_cat_g1v2;

-- Data Standardization & Consistency
SELECT DISTINCT
	subcat
FROM bronze.erp_px_cat_g1v2;

-- Data Standardization & Consistency
SELECT DISTINCT
	maintenance
FROM bronze.erp_px_cat_g1v2;
