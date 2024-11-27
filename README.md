# Healthcare Data Warehouse

## Longitudinal Access and Adjudication Data

### Purpose

The objective of this project is to build a Longitudinal Access and Adjudication Data Warehouse on Azure Cloud, aimed at integrating hospital data from client cloud storage sources (e.g. flat files) into our cloud storage system on a daily basis. This warehouse will enhance decision-making through historical data availability, consolidated reporting, and improved analytics for hospital data.

### Objectives

- **Data Extraction & Integration**: Daily integration of hospital data from client cloud storage to Azure Data Lake Gen2 using Azure Data Factory.
- **Data Validation**: Apply business rules for data validation using Spark pool in Azure Synapse Analytics, notify clients of invalid records, and decide on record actions post-confirmation.
- **Data Transformation**: Implement data transformation rules using Spark pool in Azure Synapse Analytics.
- **Data Warehouse Creation**: Establish business rules for Data Warehouse creation and materialized views to support BI needs through Serverless SQL pool in Azure Synapse Analytics.

### Scope

- **Data Extraction**: Extract data from source systems (e.g. .csv, .txt) to Azure Data Lake Storage (ADLS) daily (bronze layer).
- **Data Validation & Transformation**: Use Azure Synapse Analytics and Spark Pool notebooks for data validation and transformation (silver layer).
- **Data Loading**: Load data into the HealthCare Data Warehouse (gold layer) using Serverless SQL Pool.
- **Data Modeling**: Create dimension and fact tables in Azure SQL DB for analysis and reporting.

### Project Architecture Diagram

[Project Architecture]()

### Environment Setup

- Resource Group: `Data_Engineering_Project_2448_Pramod_Potghan`
  - Azure Data Factory: `ADF_SalesProject_2448_Pramod_Potghan`
  - Azure Synapse Analytics: `SYN_SalesProject_2448_Pramod_Potghan`
  - Azure Key Vault: `AKV_SalesProject_2448_Pramod_Potghan`
  - Client Azure Storage: `adlssalesprojectpp1`
  - Healthcare Azure Storage: `adlssalesprojectpp2`

### Bronze Layer

1. **Data Loading**: Load data from source storage to Bronze layer in ADLS.
   - Source Container: `Source`
   - Bronze Container: `bronze`
   - Naming: `TableName_<timestamp>.csv` or `TableName_<timestamp>.txt`

2. **Source Tables**: PRODUCT, PROVIDER, DIAGNOSIS, PROCEDURE, PLAN, RX PATIENT ACTIVITY, PATIENT MPD, PATIENT COMMERCIAL, PATIENT DEMOGRAPHICS.

3. **Validation Checks**:
   - File Count, File Name, Record Count validations.
   - Notify discrepancies and proceed post-validation.

4. **Initial Load**: Full data extraction for first run.

5. **Configuration**: Maintain source and target file details in a config file.

### Silver Layer

- **Container**: `silver`
- **Datasets**: PRODUCT, PROVIDER, DIAGNOSIS, PROCEDURE, PLAN, RX PATIENT ACTIVITY, PATIENT MPD, PATIENT COMMERCIAL, PATIENT DEMOGRAPHICS.
- **Validation Rules**: Reject duplicates and null PKs, and manage rejected records.
- **Transformation**: Use Synapse Spark notebooks for validation and transformation.
- **Format**: Store datasets in Parquet format.

### Gold Layer

- **Datasets**: Dimensions (e.g. TBL_DIM_PRODUCT, TBL_DIM_PROVIDER).
- **Storage**: Use Delta format.
- **Transformation**: Use Synapse Spark Pool.

### Synapse Views

- Create views (e.g. VIEW_DIM_PROVIDER, VIEW_DIM_PROCEDURE) in Synapse.

### Orchestration Plan

1. **Master Pipeline**: Develop in ADF to coordinate bronze, silver, and gold layers.
2. **Scheduling**: Trigger Master Pipeline daily at 9:00 AM IST.
3. **Retry Logic**: Implement retry policy for resilience.
4. **Alert Mechanism**: Set up monitoring and alerts for pipeline failures.

### Conclusion

This project focuses on building an efficient Healthcare Data Warehouse using Azure Data Factory and Azure Synapse Analytics.
