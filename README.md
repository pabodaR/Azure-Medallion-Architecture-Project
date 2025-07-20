# Azure-Medallion-Architecture

## **Overview**
This project demonstrates a modern data engineering pipeline built on the Azure ecosystem.  
It ingests raw data from a GitHub data source, processes it through **Azure Data Factory (ADF)**, **Azure Data Lake (ADLS)**, and **Databricks**, and stores curated datasets in **Azure Synapse Analytics** using a **medallion architecture (Bronze → Silver → Gold)**.  

The **final gold layer** is exposed for reporting and analytics (Power BI connected to Synapse).

---

## **Architecture**
![Architecture Diagram](images/architecture.png)  

---

## **Tech Stack**
- **Azure Data Factory (ADF)** – Orchestrating ELT pipelines  
- **Azure Data Lake Storage (ADLS)** – Bronze & Silver storage layers  
- **Azure Databricks (PySpark)** – Data transformations (raw → transformed)  
- **Azure Synapse Analytics** – External tables, views for the Gold layer 
- **GitHub** – Data source for ingestion and version control  

---

## **Project Workflow**

### **1. Data Ingestion (Bronze Layer)**
- Data is fetched directly from a public GitHub repository using **ADF pipelines**.
- Raw data is stored in **ADLS (Bronze)** with no transformations.
- ADF pipeline is parameterized to handle dynamic datasets.

---

### **2. Data Transformation (Silver Layer)**
- **Databricks notebooks** clean and transform data into Parquet format.
- Data quality checks, and transformations are applied.
- Processed data is stored in **ADLS Silver layer**.

---

### **3. Data Serving (Gold Layer)**
- Curated Silver datasets are loaded into **Synapse Analytics** as **external tables**.
- Business-friendly views are created to serve analytics needs.

---

