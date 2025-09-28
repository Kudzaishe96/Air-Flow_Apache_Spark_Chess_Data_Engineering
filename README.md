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
- [ Gold Layer](#Gold-Layer)


### Project Overview

This project seek to show the extraction of raw data into meaningfull sql views for data analytics 


## Air-Flow_Apache_Spark_Chess_Data_Engineering_Achitecture




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
1. Coonect your data to power bi
2. Design a template for your report
3. Publish the report as a webbased report


   

 #  The End
