/*====================================================================
SILVER LAYER – DATA QUALITY CHECKS
======================================================================

Purpose:
This script documents the data quality validations and transformation
rules applied when loading data from the Bronze Layer into the
Silver Layer of the Data Warehouse.

The Silver Layer focuses on:
• Data cleansing
• Data standardization
• Data validation
• Deduplication
• Handling missing values
• Implementing Slowly Changing Dimensions (SCD Type 2)

These checks ensure that the curated data is reliable before it is
consumed by the Gold Layer for analytics and reporting.

====================================================================*/


/*====================================================================
1. CUSTOMER DATA QUALITY CHECKS
Table: silver.crm_cust_info
====================================================================*/

-- 1.1 Remove Duplicate Customers
-- Keep only the latest record for each customer based on creation date
SELECT
    cst_id,
    cst_create_date,
    ROW_NUMBER() OVER (
        PARTITION BY cst_id
        ORDER BY cst_create_date DESC
    ) AS rn
FROM bronze.crm_cust_info;


-- 1.2 Remove Leading and Trailing Spaces
-- Ensures consistent customer names
SELECT
    TRIM(cst_firstname) AS cleaned_firstname,
    TRIM(cst_lastname)  AS cleaned_lastname
FROM bronze.crm_cust_info;


-- 1.3 Standardize Marital Status Values
-- Converts coded values into readable business values
SELECT
    cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END AS standardized_marital_status
FROM bronze.crm_cust_info;


-- 1.4 Standardize Gender Values
SELECT
    cst_gender,
    CASE
        WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END AS standardized_gender
FROM bronze.crm_cust_info;



/*====================================================================
2. PRODUCT DATA QUALITY CHECKS
Table: silver.crm_prd_info
====================================================================*/

-- 2.1 Extract Category ID from Product Key
SELECT
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS extracted_category_id
FROM bronze.crm_prd_info;


-- 2.2 Handle Missing Product Cost
-- Replace NULL values with 0
SELECT
    prd_cost,
    ISNULL(prd_cost,0) AS cleaned_cost
FROM bronze.crm_prd_info;


-- 2.3 Standardize Product Line Codes
SELECT
    prd_line,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'N/A'
    END AS standardized_product_line
FROM bronze.crm_prd_info;


-- 2.4 SCD Type 2 Logic for Product History
-- Derive end date using LEAD function
SELECT
    prd_key,
    prd_start_dt,
    LEAD(prd_start_dt) OVER (
        PARTITION BY prd_key
        ORDER BY prd_start_dt
    ) AS next_start_date
FROM bronze.crm_prd_info;



/*====================================================================
3. SALES DATA QUALITY CHECKS
Table: silver.crm_sales_details
====================================================================*/

-- 3.1 Validate Order Date Format
-- Ensure integer dates follow YYYYMMDD format
SELECT
    sls_order_dt,
    CASE
        WHEN sls_order_dt = 0
        OR LEN(CAST(sls_order_dt AS VARCHAR)) != 8
        THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS cleaned_order_date
FROM bronze.crm_sales_details;


-- 3.2 Validate Shipping Date
SELECT
    sls_ship_dt,
    CASE
        WHEN sls_ship_dt = 0
        OR LEN(CAST(sls_ship_dt AS VARCHAR)) != 8
        THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS cleaned_ship_date
FROM bronze.crm_sales_details;


-- 3.3 Validate Due Date
SELECT
    sls_due_dt,
    CASE
        WHEN sls_due_dt = 0
        OR LEN(CAST(sls_due_dt AS VARCHAR)) != 8
        THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS cleaned_due_date
FROM bronze.crm_sales_details;


-- 3.4 Validate Sales Amount
-- Recalculate sales if NULL, negative, or inconsistent
SELECT
    sls_sales,
    sls_quantity,
    sls_price,
    CASE
        WHEN sls_sales IS NULL
        OR sls_sales <= 0
        OR sls_sales <> sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS validated_sales
FROM bronze.crm_sales_details;


-- 3.5 Validate Product Price
-- Derive price if missing or invalid
SELECT
    sls_price,
    sls_sales,
    sls_quantity,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN ABS(sls_sales / NULLIF(sls_quantity,0))
        ELSE ABS(sls_price)
    END AS validated_price
FROM bronze.crm_sales_details;



/*====================================================================
4. ERP CUSTOMER DATA QUALITY CHECKS
Table: silver.erp_cust_az12
====================================================================*/

-- 4.1 Remove NAS Prefix from Customer IDs
SELECT
    cid,
    CASE
        WHEN cid LIKE 'NAS%'
        THEN SUBSTRING(cid,4,LEN(cid))
        ELSE cid
    END AS cleaned_customer_id
FROM bronze.erp_cust_az12;


-- 4.2 Validate Birthdate
-- Remove future birthdates
SELECT
    bdate,
    CASE
        WHEN bdate > GETDATE()
        THEN NULL
        ELSE bdate
    END AS validated_birthdate
FROM bronze.erp_cust_az12;


-- 4.3 Standardize Gender Values
SELECT
    gen,
    CASE
        WHEN UPPER(TRIM(gen)) LIKE '%F%' THEN 'Female'
        WHEN UPPER(TRIM(gen)) LIKE '%M%' THEN 'Male'
        ELSE 'N/A'
    END AS standardized_gender
FROM bronze.erp_cust_az12;



/*====================================================================
5. ERP LOCATION DATA QUALITY CHECKS
Table: silver.erp_loc_a101
====================================================================*/

-- 5.1 Remove Hyphens from Customer IDs
SELECT
    cid,
    REPLACE(cid,'-','') AS cleaned_customer_id
FROM bronze.erp_loc_a101;


-- 5.2 Standardize Country Names
SELECT
    cntry,
    CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) LIKE 'US%' THEN 'United States'
        WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'N/A'
        ELSE TRIM(cntry)
    END AS standardized_country
FROM bronze.erp_loc_a101;



/*====================================================================
6. ERP PRODUCT CATEGORY DATA
Table: silver.erp_px_cat_g1v2
====================================================================*/

-- Direct load validation
-- Reference data is preserved without transformation
SELECT *
FROM bronze.erp_px_cat_g1v2;



/*====================================================================
SUMMARY OF DATA QUALITY RULES IMPLEMENTED
====================================================================

✔ Duplicate removal
✔ String trimming
✔ Value standardization
✔ Missing value handling
✔ Date validation
✔ Numeric validation
✔ Sales calculation validation
✔ Identifier normalization
✔ Slowly Changing Dimension (SCD Type 2)

====================================================================*/
