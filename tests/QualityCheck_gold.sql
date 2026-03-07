/*===============================================================
DATA QUALITY TESTS
=================================================================
Purpose:
This script documents the data quality checks implemented during
the transformation of data from the Silver Layer to the Gold Layer
in the Data Warehouse.

These tests ensure:
• Clean and reliable analytical datasets
• Consistent attribute values
• Valid dimensional relationships
• Removal of outdated records
================================================================*/


/*===============================================================
1. Gender Data Validation
-----------------------------------------------------------------
Rule:
CRM system is treated as the master source for gender.
If CRM gender is unavailable ('n/a'), ERP data is used instead.
If both sources are missing, 'n/a' is assigned.

Purpose:
Prevent inconsistent or missing gender values.
================================================================*/

SELECT
    ci.cst_gender,
    ca.gen,
    CASE 
        WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender
        ELSE COALESCE(ca.gen, 'n/a')
    END AS Final_Gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid;



/*===============================================================
2. Handling Missing Gender Values
-----------------------------------------------------------------
Rule:
If ERP gender is NULL, replace with 'n/a'.

Purpose:
Avoid NULL values in analytical datasets.
================================================================*/

SELECT
    gen,
    COALESCE(gen, 'n/a') AS Cleaned_Gender
FROM silver.erp_cust_az12;



/*===============================================================
3. Filtering Historical Products
-----------------------------------------------------------------
Rule:
Only include active products where product end date is NULL.

Purpose:
Prevent historical or inactive products from appearing in reports.
================================================================*/

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt IS NULL;



/*===============================================================
4. Surrogate Key Generation
-----------------------------------------------------------------
Rule:
Generate unique surrogate keys for dimension tables.

Purpose:
Ensure each dimension record has a unique identifier
for linking with fact tables.
================================================================*/

-- Customer surrogate keys
SELECT
ROW_NUMBER() OVER (ORDER BY cst_id) AS Customer_Key,
cst_id
FROM silver.crm_cust_info;


-- Product surrogate keys
SELECT
ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key) AS Product_Key,
prd_id
FROM silver.crm_prd_info;



/*===============================================================
5. Referential Integrity Check
-----------------------------------------------------------------
Rule:
Sales transactions must reference valid customers and products.

Purpose:
Ensure fact table records are linked to dimension tables.
================================================================*/

SELECT
sd.sls_order_num,
sd.sls_prd_key,
sd.sls_cust_id,
pr.Product_Number,
cu.Customer_id
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.Product_Number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.Customer_id;



/*===============================================================
6. Standardized Naming for Analytics
-----------------------------------------------------------------
Rule:
Source system column names are transformed into standardized
business-friendly naming conventions.

Purpose:
Improve readability and usability for BI tools and analysts.
================================================================*/

SELECT
ci.cst_firstname AS First_Name,
ci.cst_lastname AS Last_Name,
ci.cst_marital_status AS Marital_Status
FROM silver.crm_cust_info ci;
