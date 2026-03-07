/*===============================================================
  GOLD LAYER - DATA WAREHOUSE
  ===============================================================
  This script creates the analytical layer of the data warehouse.

  The Gold Layer provides business-ready datasets organized in a 
  star schema format for reporting and analytics.

  Tables included:
  1. dim_customers  - Customer dimension
  2. dim_products   - Product dimension
  3. fact_sales     - Sales fact table

  These views transform and combine cleaned data from the 
  Silver Layer into structures optimized for BI tools.
================================================================*/


/*===============================================================
  CUSTOMER DIMENSION
  ===============================================================
  Purpose:
  Provides descriptive information about customers to support 
  customer-based analytics such as demographics, purchasing 
  behavior, and regional sales analysis.
================================================================*/

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS

-- Combine customer information from CRM and ERP systems
SELECT 
    -- Generate surrogate key for the customer dimension
    ROW_NUMBER() OVER (ORDER BY cst_id) AS Customer_Key,

    ci.cst_id AS Customer_id,
    ci.cst_key AS Customer_number,

    ci.cst_firstname AS First_Name,
    ci.cst_lastname AS Last_Name,

    la.cntry AS Country,

    ci.cst_marital_status AS Marital_Status,

    -- Gender logic:
    -- CRM system is treated as the master source.
    -- If gender is unavailable, fallback to ERP data.
    CASE 
        WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender
        ELSE COALESCE(ca.gen, 'n/a')
    END AS Gender,

    ca.bdate AS Birthdate,

    ci.cst_create_date AS Create_Date

FROM silver.crm_cust_info AS ci

-- Join ERP system for additional demographic data
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid

-- Join location table for country information
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;

GO


/*===============================================================
  PRODUCT DIMENSION
  ===============================================================
  Purpose:
  Provides detailed information about products including their 
  categories and classifications to support product-level 
  analytics and sales reporting.
================================================================*/

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS

-- Combine product and category information
SELECT 

    -- Generate surrogate key for product dimension
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS Product_Key,

    pn.prd_id AS Product_id,
    pn.prd_key AS Product_Number,

    pn.prd_nm AS Product_Name,

    pn.cat_id AS Category_id,
    pc.cat AS Category,
    pc.subcat AS Subcategory,

    pc.maintenance,

    pn.prd_cost AS Cost,

    pn.prd_line AS Product_Line,

    pn.prd_start_dt AS Start_Date

FROM silver.crm_prd_info pn

-- Join product category reference table
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id

-- Exclude historical products that are no longer active
WHERE prd_end_dt IS NULL;

GO


/*===============================================================
  SALES FACT TABLE
  ===============================================================
  Purpose:
  Stores transactional sales data and links to the customer and 
  product dimensions to enable analytical queries such as:

  - Total revenue
  - Sales by product category
  - Sales by customer
  - Order and shipping performance
================================================================*/

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS

-- Combine sales transactions with product and customer dimensions
SELECT  

    sd.sls_order_num AS Order_Number,

    -- Foreign keys referencing dimension tables
    pr.Product_Key,
    cu.Customer_Key,

    sd.sls_order_dt AS Order_Date,
    sd.sls_ship_dt AS Shipping_date,
    sd.sls_due_dt AS Due_Date,

    sd.sls_sales AS Sales_amount,
    sd.sls_quantity AS Quantity,
    sd.sls_price

FROM silver.crm_sales_details sd

LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.Product_Number

LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.Customer_id;

GO
