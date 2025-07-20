-- Create master key 
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<YOUR_SECURE_PASSWORD>';

-- 1) Create database scoped credential
CREATE DATABASE SCOPED CREDENTIAL cred
WITH
    IDENTITY = 'Managed Identity';

-- 2) Create external data sources for Silver and Gold layers
CREATE EXTERNAL DATA SOURCE source_silver
WITH (
    LOCATION = 'https://<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/silver',
    CREDENTIAL = cred
);

CREATE EXTERNAL DATA SOURCE source_gold
WITH (
    LOCATION = 'https://<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/gold',
    CREDENTIAL = cred
);

-- 3) Create external file format
CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

-- Create external tables using views (Gold layer)

--- External table: ext_sales
CREATE EXTERNAL TABLE gold.ext_sales
WITH
(
   LOCATION = 'ext_sales',
   DATA_SOURCE = source_gold,
   FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.sales; -- View created earlier is used

SELECT * FROM gold.ext_sales; -- Query external table


--- External table: ext_customers
CREATE EXTERNAL TABLE gold.ext_customers
WITH
(
   LOCATION = 'ext_customers',
   DATA_SOURCE = source_gold,
   FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.customers;

SELECT * FROM gold.ext_customers;


--- External table: ext_products
CREATE EXTERNAL TABLE gold.ext_products
WITH
(
   LOCATION = 'ext_products',
   DATA_SOURCE = source_gold,
   FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.products;

SELECT * FROM gold.ext_products;


--- External table: ext_product_categories
CREATE EXTERNAL TABLE gold.ext_product_categories
WITH
(
   LOCATION = 'ext_product_categories',
   DATA_SOURCE = source_gold,
   FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.product_categories;

SELECT * FROM gold.ext_product_categories;


--- External table: ext_product_subcategories
CREATE EXTERNAL TABLE gold.ext_product_subcategories
WITH
(
   LOCATION = 'ext_product_subcategories',
   DATA_SOURCE = source_gold,
   FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.product_subcategories;

SELECT * FROM gold.ext_product_subcategories;


--- External table: ext_returns
CREATE EXTERNAL TABLE gold.ext_returns
WITH
(
   LOCATION = 'ext_returns',
   DATA_SOURCE = source_gold,
   FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.returns;

SELECT * FROM gold.ext_returns;


--- External table: ext_territories
CREATE EXTERNAL TABLE gold.ext_territories
WITH
(
   LOCATION = 'ext_territories',
   DATA_SOURCE = source_gold,
   FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.territories;

SELECT * FROM gold.ext_territories;
