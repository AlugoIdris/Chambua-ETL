import configparser
import boto3
from botocore.client import Config
from botocore import UNSIGNED
import psycopg2
import psycopg2.pool


config = configparser.ConfigParser()
config.read_file(open('cluster.config'))

HOST=config.get('DWH', 'HOST')
DWH_DB=config.get('DWH', 'DWH_DB')
DWH_DB_USER=config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD=config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT=config.get('DWH', 'DWH_PORT')
STAGING_SCHEMA=config.get('DWH', 'STAGING_SCHEMA')
ANALYTICS_SCHEMA=config.get('DWH', 'ANALYTICS_SCHEMA')
S3_BUCKET_NAME=config.get('DWH', 'S3_BUCKET_NAME')
ID=config.get('DWH', 'ID')
EXPORT=config.get('DWH', 'EXPORT')



def connect_to_s3_storage():
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    objects = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="orders_data/")
    return s3, objects


def connect_to_datawarehouse():
    POOL = psycopg2.pool.SimpleConnectionPool(
        1, 10,
        host=HOST,
        dbname=DWH_DB,
        user=DWH_DB_USER,
        password=DWH_DB_PASSWORD,
        port=DWH_PORT,
        options=f"-c search_path=dbo,{STAGING_SCHEMA}"
    )

    # def open_cursor():
    try:
        # Get a connection from the pool
        conn = POOL.getconn()

        # Open a cursor on the connection
        cur = conn.cursor()

        # Set autocommit mode
        conn.set_session(autocommit=True)

        # Return the cursor and connection

    except psycopg2.Error as e:
            # If an error occurs, log it and raise an exception
            print('Error: Could not get connection from the pool')
            print(e)
            raise
    
    return cur
    

# def close_connection():
#     cur.close()