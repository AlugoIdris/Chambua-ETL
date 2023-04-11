# ETL Data Project Readme
This project is an ETL (Extract, Transform, Load) data pipeline that processes data from a ***S3*** bucket source, applies transformations to it, and loads it into a ***RDS*** staging datawarehouse. There after, some analysis is done on the tables in the datawarehouse and the analyzed data exported back to the ***S3*** bucket. The goal of this project is to provide clean, organized, and accurate data for analysis and decision-making.

# Getting Started
To run this project, you will need to have the following software installed:

Python 3
Pandas library
Access to the S3 bucket and RDS data warehouse database
You will also need to set up a PostgreSQL database with the necessary tables and columns to store the data. Make sure to update the `config.cfg` file with the correct database connection details.

# Project Structure

The project is structured as follows:

* config.cfg: contains configuration variables such as database connection details
* etl.py: contains the ETL pipeline code
* sql_quries.py: contains the table creation, deletion and insertion codes
* create_tables.py: contains the table generation and loading code
* connection.py: contains code to trigger connection to the bucket and datawarehouse


# Relations Creation
A total of three tables are created in the database hosted in Redshift. They include `orders`, `reviews` & `shipment_deliveries` tables. There are already four other tables existing in the datawarehouse. The `dim_customers`, `dim_products`, `dim_dates`, `dim_addresses`. The names of the tables along with their attributes are self explanatory about the information they hold. The primary key of each table is highlighted for clarity.

<p align="center">
  <img src="https://github.com/AlugoIdris/idrialug9071_d2b_project/blob/main/images/Data%20Model.png">
</p>


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


# Running the Pipeline

To run the pipeline, execute the following command in your terminal:
```bash
python etl.py
```

This will execute the entire ETL pipeline, from data extraction to data loading. The transformed data will be loaded to the RDS staging warehouse `idrialug9071_staging`, and the computed files will be loaded back to the S3 bucket directory `idrialug9071_analytics`.


# Extra: Data Visualization
Modelling the dimension tables in the data warehouse, alongside the loaded data from S3 bucket, a report was created using Microsoft Power BI to see how the produccts are doing with respect to reviews, sales, orders, etc. The visualization is shown below:

<p align="center">
  <img src="https://github.com/AlugoIdris/idrialug9071_d2b_project/blob/main/images/Chambua%20Inc.png">
</p>
