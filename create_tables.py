import psycopg2
from sql_queries import *
from connection import *

cur = connect_to_datawarehouse()
s3, objects = connect_to_s3_storage()

def compute_agg_public_holiday():

    try:
        cur.execute(agg_public_holiday_table_drop)
    except Exception as e:
        print('Error: Issue dropping data')
        print(e)

    try:
        cur.execute(agg_public_holiday_table_create)
    except Exception as e:
        print('Error: Issue creating data')
        print(e)

    try:
        cur.execute(agg_public_holiday_table_insert)
    except Exception as e:
        print('Error: Issue inserting data')
        print(e)

    #Upload data to S3 bucket
    with open('output/agg_public_holiday.csv', 'w') as f:
        cur.copy_expert(f"COPY {ANALYTICS_SCHEMA}.agg_public_holiday TO STDOUT WITH HEADER CSV", f)
        
    s3.upload_file('output/agg_public_holiday.csv', S3_BUCKET_NAME, f'{EXPORT}/{ID}/agg_public_holiday.csv')


def compute_agg_shipments():

    try:
        cur.execute(agg_shipments_table_drop)
    except Exception as e:
        print('Error: Issue dropping data')
        print(e)


    try:
        cur.execute(agg_shipments_table_create)
    except Exception as e:
        print('Error: Issue creating data')
        print(e)

    try:
        cur.execute(agg_shipments_table_insert)
    except Exception as e:
        print('Error: Issue inserting data')
        print(e)

    with open('output/agg_shipments.csv', 'w') as f:
        cur.copy_expert(f"COPY {ANALYTICS_SCHEMA}.agg_shipments TO STDOUT WITH HEADER CSV", f)
        
    s3.upload_file('output/agg_shipments.csv', S3_BUCKET_NAME, f'{EXPORT}/{ID}/agg_shipments.csv')


def compute_best_performing_product():

    try:
        cur.execute(best_performing_product_table_drop)
    except Exception as e:
        print('Error: Issue dropping data')
        print(e)


    try:
        cur.execute(best_performing_product_table_create)
    except Exception as e:
        print('Error: Issue creating data')
        print(e)


    try:
        cur.execute(best_performing_product_table_insert)
    except Exception as e:
        print('Error: Issue inserting data')
        print(e)

    with open('output/best_performing_product.csv', 'w') as f:
        cur.copy_expert(f"COPY {ANALYTICS_SCHEMA}.best_performing_product TO STDOUT WITH HEADER CSV", f)
        
    s3.upload_file('output/best_performing_product.csv', S3_BUCKET_NAME, f'{EXPORT}/{ID}/best_performing_product.csv')
