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

![HealthCare Architecture diagram](https://github.com/potghanpramod13/Healthcare-Data-Warehouse-Longitudinal-Access-and-Adjudication-Data-/blob/c2b504ef9a7ff74c72c997e5829b2244cf6c87f3/HealthCare%20Architecture%20diagram.jpg)


### Environment Setup

- Resource Group: `Data_Engineering_Project_2448_Pramod_Potghan`
  
  - Azure Data Factory: `ADF_SalesProject_2448_Pramod_Potghan`
    
  - Azure Synapse Analytics: `SYN_SalesProject_2448_Pramod_Potghan`
    
  - Azure Key Vault: `AKV_SalesProject_2448_Pramod_Potghan`
    
  - Client Azure Storage: `adlssalesprojectpp1`
    
  - Healthcare Azure Storage: `adlssalesprojectpp2`
    
    ![Environment](https://github.com/user-attachments/assets/b6a6eb94-0d6a-403b-a285-57206b672466)


### Bronze Layer

1. **Data Loading**: Load data from source storage to Bronze layer in ADLS.
   - Source Container: `Source`
     
     ![Source container](https://github.com/user-attachments/assets/2c7155b0-6561-45b5-9dcf-0d811181e4f9)

   - Bronze Container: `bronze`
     
   - Naming: `TableName_<timestamp>.csv` or `TableName_<timestamp>.txt`
     
     ![Bronze container](https://github.com/user-attachments/assets/ba366751-c85b-41a3-8272-d0bf73f16c87)


2. **Source Tables**: CONTROL FILES, PRODUCT, PROVIDER, DIAGNOSIS, PROCEDURE, PLAN, RX PATIENT ACTIVITY, PATIENT MPD, PATIENT COMMERCIAL, PATIENT DEMOGRAPHICS.

3. **Validation Checks**:
   - File Count, File Name, Record Count validations from CONTROL FILES.csv.
     
   - Notify discrepancies and proceed post-validation.
     
     ![Synapse Notebook Activity](https://github.com/user-attachments/assets/384f4110-a3db-4c51-8390-3941e5dd023c)
     
     Also below JSON code of Above Pipeline as follow
     
     ![PL_SYN_NOTE_VALIDATION_SRC.json](https://github.com/potghanpramod13/Healthcare-Data-Warehouse-Longitudinal-Access-and-Adjudication-Data-/blob/180d66ba84a3fb714cf3b6d2443abdc521d7fb3d/PL_SYN_NOTE_VALIDATION_SRC.json)
     
   - Validation done in Synapse spark pool notebook linked below,
     
     ![Synapse_NB_Data_Validation_src_to_bronze.ipynb](https://github.com/potghanpramod13/Healthcare-Data-Warehouse-Longitudinal-Access-and-Adjudication-Data-/blob/5e8f02c3b9c5399fd845b6d5ced78f231a35d71e/Synapse_NB_Data_Validation_src_to_bronze.ipynb)
     
   
4. **Initial Load**: Full data extraction for first run.

   Below Screenshots shows flow of Pipeline to copy all files from Client ADLS to Our ADLS in Bronze container
   
   ![Source to ADLS Bronze](https://github.com/user-attachments/assets/1a082564-d98c-4a3f-9213-7bae6f70eabb)
   
   ![Inside for loop activity](https://github.com/user-attachments/assets/e6fb9cd3-1002-4d67-8946-ee3487cc5171)
   
   ![Inside of epipe activity](https://github.com/user-attachments/assets/3c3071f2-90c9-4180-95e2-64cd2caafb99)
   
   ![Inside for loop](https://github.com/user-attachments/assets/d27816e9-6bd3-4d76-8142-38a89a446726)
   
   ![Inside If condition](https://github.com/user-attachments/assets/d3051878-8c1a-4f40-a343-df59e8325870)

   Also below JSON code of Above Pipeline as follow

   ![PL_ADLS_SRC_TO_BRONZE.json](https://github.com/potghanpramod13/Healthcare-Data-Warehouse-Longitudinal-Access-and-Adjudication-Data-/blob/180d66ba84a3fb714cf3b6d2443abdc521d7fb3d/PL_ADLS_SRC_TO_BRONZE.json)

### Silver Layer

- **Container**: `silver
  
  ![Silver Container](https://github.com/user-attachments/assets/95223a76-ca3e-45bb-975a-c416b130c532)
  
- **Datasets**: PRODUCT, PROVIDER, DIAGNOSIS, PROCEDURE, PLAN, RX PATIENT ACTIVITY, PATIENT MPD, PATIENT COMMERCIAL, PATIENT DEMOGRAPHICS.
  
- **Validation Rules**: Reject duplicates and null PKs, and manage rejected records.
  
- **Transformation**: Use Synapse Spark notebooks for validation and transformation.
  
 ![Notebook Activity](https://github.com/user-attachments/assets/2f0c8e07-657a-452f-9c17-7f0a381366dd)

Also below JSON code of Above Pipeline as follow

![PL_SYN_NOTE_VALIDATION_BRONZE_TO_SILVER.json](https://github.com/potghanpramod13/Healthcare-Data-Warehouse-Longitudinal-Access-and-Adjudication-Data-/blob/180d66ba84a3fb714cf3b6d2443abdc521d7fb3d/PL_SYN_NOTE_VALIDATION_BRONZE_TO_SILVER.json)

- **Format**: Store datasets in Parquet format.

 Also below notebook of Data validation from bronze to silver layer
 
  [Synapse_NB_Data_Validation_bronze_to_silver.ipynb](https://github.com/potghanpramod13/Healthcare-Data-Warehouse-Longitudinal-Access-and-Adjudication-Data-/blob/5e8f02c3b9c5399fd845b6d5ced78f231a35d71e/Synapse_NB_Data_Validation_bronze_to_silver.ipynb)
  
### Gold Layer

- **Datasets**: Dimensions (e.g. TBL_DIM_PRODUCT, TBL_DIM_PROVIDER).
  
  ![COnfig file for views](https://github.com/user-attachments/assets/d3e0b82b-8de2-4510-a80f-e161c13348cd)
  
- **Storage**: Use Delta format.
  
  ![GOLD container](https://github.com/user-attachments/assets/ec0faa83-7602-491c-a48f-65ae490ce260)
  
- **Transformation**: Use Synapse Spark Pool.
  
  ![SCD Notebook Activity](https://github.com/user-attachments/assets/9dd856f1-5182-40fc-85c0-9fa728753a53)
  
  Also below JSON code of Above Pipeline as follow
  
  ![PL_SYN_NOTE_VALIDATION_SILVER_TO_GOLD.json](https://github.com/potghanpramod13/Healthcare-Data-Warehouse-Longitudinal-Access-and-Adjudication-Data-/blob/180d66ba84a3fb714cf3b6d2443abdc521d7fb3d/PL_SYN_NOTE_VALIDATION_SILVER_TO_GOLD.json)
  
  Also below notebook of scd for silver to gold layer
  
  ![Synapse_NB_SCD_DIM_TBL_silver_to_gold.ipynb](https://github.com/potghanpramod13/Healthcare-Data-Warehouse-Longitudinal-Access-and-Adjudication-Data-/blob/5e8f02c3b9c5399fd845b6d5ced78f231a35d71e/Synapse_NB_SCD_DIM_TBL_silver_to_gold.ipynb)

### Synapse Views

- Create views (e.g. VIEW_DIM_PROVIDER, VIEW_DIM_PROCEDURE) in Synapse.
- below screenshots shows flow of pipeline to create views
  
  ![create views pipeline](https://github.com/user-attachments/assets/30ed7e5b-f0e7-4a7c-9556-e935beff3c84)
  
  ![for loop activity](https://github.com/user-attachments/assets/4e1c0d3b-b792-43ca-9f82-85fa5360a85e)
  
  ![Inside for loop activity](https://github.com/user-attachments/assets/02b194d6-077a-4cfe-8b62-0a8c1955d7de)

  below JSON file of above pipeline as follow
  
  ![PL_SYN_VIEWS_CREATE_OR_UPDATE.json](https://github.com/potghanpramod13/Healthcare-Data-Warehouse-Longitudinal-Access-and-Adjudication-Data-/blob/180d66ba84a3fb714cf3b6d2443abdc521d7fb3d/PL_SYN_VIEWS_CREATE_OR_UPDATE.json)

  Also below sql script for create stored procedure for create views on delta tables
  
  ![SQL STORED PROCEDURE FOR VIEWS.sql](https://github.com/potghanpramod13/Healthcare-Data-Warehouse-Longitudinal-Access-and-Adjudication-Data-/blob/b288d802d3484a1c1cdfbd3d0717369ee70c5b70/SQL%20STORED%20PROCEDURE%20FOR%20VIEWS.sql)

### Orchestration Plan

1. **Master Pipeline**: Develop in ADF to coordinate bronze, silver, and gold layers.

   list of all pipelines as below
   
   ![List Of All Pipelines](https://github.com/user-attachments/assets/ec0298fd-ef7a-4c71-9142-d1201262a531)

   list of all datasets as below
   
   ![List Of All Datasets](https://github.com/user-attachments/assets/5dab4963-3eb1-48f8-906f-bc520934ab38)

   Master Pipeline of this projects as below
   
   ![Master Pipeline](https://github.com/user-attachments/assets/df03d525-96c7-4f55-9de8-6f35b2b23e8c)

   also JSON file as below
   
   ![PL_MASTER_ADF_AND_SYN.json](https://github.com/potghanpramod13/Healthcare-Data-Warehouse-Longitudinal-Access-and-Adjudication-Data-/blob/180d66ba84a3fb714cf3b6d2443abdc521d7fb3d/PL_MASTER_ADF_AND_SYN.json)

3. **Scheduling**: Trigger Master Pipeline daily at 9:00 AM IST.
   
4. **Retry Logic**: Implement retry policy for resilience.
   
5. **Alert Mechanism**: Set up monitoring and alerts for pipeline failures.

### Conclusion

This project focuses on building an efficient Healthcare Data Warehouse using Azure Data Factory and Azure Synapse Analytics.
