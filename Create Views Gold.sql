-------------------------------- calendar view
CREATE VIEW gold.calendar
AS 
SELECT *
FROM 
    OPENROWSET(
        BULK 'https://<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
        FORMAT = 'PARQUET'
    ) AS calendar_query;

-------------------------------- customer view
CREATE VIEW gold.customers
AS 
SELECT *
FROM 
    OPENROWSET(
        BULK 'https://<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/silver/AdventureWorks_Customers/',
        FORMAT = 'PARQUET'
    ) AS customers_query;

-------------------------------- product categories view
CREATE VIEW gold.product_categories
AS 
SELECT *
FROM 
    OPENROWSET(
        BULK 'https://<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/silver/AdventureWorks_Product_Categories/',
        FORMAT = 'PARQUET'
    ) AS product_categories_query;

-------------------------------- product subcategories view
CREATE VIEW gold.product_subcategories
AS 
SELECT *
FROM 
    OPENROWSET(
        BULK 'https://<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
        FORMAT = 'PARQUET'
    ) AS product_subcategories_query;

-------------------------------- products view
CREATE VIEW gold.products
AS 
SELECT *
FROM 
    OPENROWSET(
        BULK 'https://<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/silver/AdventureWorks_Products/',
        FORMAT = 'PARQUET'
    ) AS products_query;

-------------------------------- returns view
CREATE VIEW gold.returns
AS 
SELECT *
FROM 
    OPENROWSET(
        BULK 'https://<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/silver/AdventureWorks_Returns/',
        FORMAT = 'PARQUET'
    ) AS returns_query;

-------------------------------- sales view
CREATE VIEW gold.sales
AS 
SELECT *
FROM 
    OPENROWSET(
        BULK 'https://<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/silver/AdventureWorks_Sales/',
        FORMAT = 'PARQUET'
    ) AS sales_query;

-------------------------------- territories view
CREATE VIEW gold.territories
AS 
SELECT *
FROM 
    OPENROWSET(
        BULK 'https://<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/silver/AdventureWorks_Territories/',
        FORMAT = 'PARQUET'
    ) AS territories_query;
