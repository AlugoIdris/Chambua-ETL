# ETL Data Project Readme
This project is an ETL (Extract, Transform, Load) data pipeline that processes data from a ***S3*** bucket source, applies transformations to it, and loads it into a ***Redshift*** staging datawarehouse. There after, some analysis is done on the tables in the datawarehouse and the analyzed data exported back to the ***S3*** bucket. The goal of this project is to provide clean, organized, and accurate data for analysis and decision-making.

# Getting Started
To run this project, you will need to have the following software installed:

Python 3
Pandas library
PostgreSQL database
You will also need to set up a PostgreSQL database with the necessary tables and columns to store the data. Make sure to update the `config.cfg` file with the correct database connection details.

# Infrastructure Setup

Infrastructure as Code (IaC) is used for configuration of the hardware resources for the project using `boto3`. The hardware setup revolves around two main resources which include setting up an IAM Role and spinning a Redshift cluster. An IAM Role is required to delegate permission to an AWS resource which in this case is a Redshift cluster. An IAM Role requires a trust relationship policy(JSON object) that defines which entity can assume this role and a permission policy that defines what the entity is allowed to do. 

Following setting up the IAM Role, a Redshift cluster needs to get setup to a host a database which would hold both the staging tables and the data warehouse. The specifications of the database and configuration details are read from `config.cfg` and the cluster is created.  The configuration details contain information such as the `arn`(amazon role name) for the IAM role which connects to the instance. Once the cluster is active a TCP port is opened(through a EC2 security group) for connecting traffic to the end point of the cluster. Finally the cluster end point is saved in the `config.cfg` file in order to connect with the database. Again the pictorial representation below brings in more clarity.

The infrastructure describe above is set up running the following command
```bash
python etl.py
```

This will execute the entire ETL pipeline, from data extraction to data loading. The transformed data will be stored in the transformed_data directory, and the log files will be stored in the logs directory.

# Relations Creation
A total of three tables are created in the database hosted in Redshift. They include `orders`, `reviews` & `shipment_deliveries` tables. There are already four other tables existing in the datawarehouse. The `dim_customers`, `dim_products`, `dim_dates`, `dim_addresses`. The names of the tables along with their attributes are self explanatory about the information they hold. The primary key of each table is highlighted for clarity.

In order to create these tables the following command is used within the etl pipeline
```bash
python create_tables.py
``` 

# ETL Pipeline
The ETL pipeline consists of the following steps:

1. Extraction: the raw data is read from the source data files, S3 using pandas dataframe and downloaded into `logs` directory

2. Transformation: the data is cleaned as needed to prepare it for analysis. This includes data validation, data type conversions, etc.

3. Loading: the transformed data is written to the staging database tables using ***Pandas and psycopg2***. The data is loaded into the staging tables and stored in the `transformed_data` directory using the following pseudo-code.

```SQL
INSERT INTO <table_name_star_schema>(<column_names>)
SELECT <column_names> FROM <table_name_staging_table>;
```

In order to extract, transform and load (ETL) use the following command.
```bash
python etl.py
```
