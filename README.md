# Air-Flow_Apache_Spark_Chess_Data_Engineering
## ETL project using Apache Airflow,Docker,Pyspark and Power BI


## Table Of Contents

- [ Project Overview ](#Project-Overview)
- [ Air-Flow_Apache_Spark_Chess_Data_Engineering_Achitecture ](#Air-Flow_Apache_Spark_Chess_Data_Engineering_Achitecture)
- [ Data Source ](#Data-Source)
- [ Tools ](#Tools)
- [ Azure Resources ](#Azure-Resources)
- [ Docker ](#Docker)
- [ Data Extraction and Cleaning  ](#Data-Extraction-(Bronze-Layer))
- [ Data Reporting ](#Data-Cleaning-(Silver-Layer))


### Project Overview

This project seek to show the extraction of raw data into meaningfull sql views for data analytics 


## Air-Flow_Apache_Spark_Chess_Data_Engineering_Achitecture
<img width="1012" height="504" alt="image" src="https://github.com/user-attachments/assets/f8d52b34-dd66-4edc-b89c-73751a6e1e93" />




### Data Source
API :Chess.com

### Tools
- API
- Docker
- Apache Airflow
- Apache Spark
- Power BI

  
### Azure Resources
-SQL Database
  
### Docker
1. Compose a docker image which countains spark, airflow,postgress
2. Use port 8080 to have access to arflow and configure the spark connection
3. Use Aiflow to trigger the job and monitor the job
   
### Data Extraction and Cleaning
1. Call out various Api cals from chess.com
2. Convert the data to pandas to spark dataframes
3. Remove unecesary columns and add the countries column
4. Extract it to the SQL Server

### Data Reporting
1. Conect  from Azure SQL Server Power BI
2. Design a template for your report and create the necesary Dax functions
3. Publish the report as a webbased report

## Power BI Report
<img width="1563" height="673" alt="image" src="https://github.com/user-attachments/assets/eab47ff5-3341-49d5-b6af-bc314967fdb8" />


 #  The End
