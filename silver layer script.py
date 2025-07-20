# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Script

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Data Lake via Application

# COMMAND ----------

# Define Azure authentication details 
client_id = "<CLIENT_ID>"
client_secret = "<CLIENT_SECRET>"
tenant_id = "<TENANT_ID>"
storage_account_name = "<STORAGE_ACCOUNT_NAME>"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data

# COMMAND ----------

df_calendar = spark.read.format('csv')\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_calendar.display()

# COMMAND ----------

df_customers = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_customers.display()

# COMMAND ----------

df_product_categories = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_product_categories.display()

# COMMAND ----------

df_products = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_returns = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_returns.display()

# COMMAND ----------

df_sales_2015 = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Sales_2015")

# COMMAND ----------

df_sales_2015.display()

# COMMAND ----------

df_sales_2016 = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Sales_2016")

# COMMAND ----------

df_sales_2016.display()

# COMMAND ----------

df_sales_2017 = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Sales_2017")

# COMMAND ----------

df_sales_2017.display()

# COMMAND ----------

df_territories = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

df_territories.display()

# COMMAND ----------

df_product_subcategories = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

df_product_subcategories.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformations

# COMMAND ----------

df_calendar.display()

# COMMAND ----------

df_calendar = df_calendar.withColumn("Month", month(col("Date")))\
    .withColumn("Year", year(col("Date")))

# COMMAND ----------

df_calendar.display()

# COMMAND ----------

df_customers = df_customers.withColumn("FullName", concat_ws(" ", col("Prefix"), col("FirstName"), col("LastName")))
df_customers.display()

# COMMAND ----------

df_product_categories.display()

# COMMAND ----------

df_product_subcategories.display()

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_products = df_products.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])\
    .withColumn('ProductName', split(col('ProductName'), ' ')[0])

df_products.display()

# COMMAND ----------

df_returns.display()

# COMMAND ----------

df_sales = df_sales_2015.union(df_sales_2016).union(df_sales_2017)
df_sales.display()

# COMMAND ----------

df_sales.withColumn("StockDate", to_timestamp(col("StockDate"))).display()

# COMMAND ----------

df_sales.withColumn("OrderNumber", regexp_replace(col("OrderNumber"), "SO", "PROD")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Analysis

# COMMAND ----------

df_sales.groupby('OrderDate').agg(count('OrderNumber').alias('TotalOrders')).display()

# COMMAND ----------

df_territories.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Transformed Data to Silver Layer

# COMMAND ----------

df_calendar.write.format("parquet")\
    .mode("append")\
    .option("path", f"abfss://silver@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Calendar")\
    .save()

# COMMAND ----------

df_customers.write.format("parquet")\
    .mode("append")\
    .option("path", f"abfss://silver@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Customers")\
    .save()

# COMMAND ----------

df_product_categories.write.format("parquet")\
    .mode("append")\
    .option("path", f"abfss://silver@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Product_Categories")\
    .save()

# COMMAND ----------

df_product_subcategories.write.format("parquet")\
    .mode("append")\
    .option("path", f"abfss://silver@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Product_Subcategories")\
    .save()

# COMMAND ----------

df_products.write.format("parquet")\
    .mode("append")\
    .option("path", f"abfss://silver@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Products")\
    .save()

# COMMAND ----------

df_returns.write.format("parquet")\
    .mode("append")\
    .option("path", f"abfss://silver@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Returns")\
    .save()

# COMMAND ----------

df_territories.write.format("parquet")\
    .mode("append")\
    .option("path", f"abfss://silver@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Territories")\
    .save()

# COMMAND ----------

df_sales.write.format("parquet")\
    .mode("append")\
    .option("path", f"abfss://silver@{storage_account_name}.dfs.core.windows.net/AdventureWorks_Sales")\
    .save()
