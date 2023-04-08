import psycopg2
from sql_queries import *




def compute_agg_public_holiday():
    with cur:
        try:
            cur.execute(agg_public_holiday_table_drop)
        except Exception as e:
            print('Error: Issue dropping data')
            print(e)
    
    with cur:
        try:
            cur.execute(agg_public_holiday_table_create)
        except Exception as e:
            print('Error: Issue creating data')
            print(e)

    with cur:
        try:
            cur.execute(agg_public_holiday_table_insert)
        except Exception as e:
            print('Error: Issue inserting data')
            print(e)

    #Upload data to S3 bucket
    with open('agg_public_holiday.csv', 'w') as f:
        cur.copy_expert(f"COPY {ANALYTICS_SCHEMA}.agg_public_holiday TO STDOUT WITH HEADER CSV", f)
        
    s3.upload_file('agg_public_holiday.csv', S3_BUCKET_NAME, f'{EXPORT}/{ID}/agg_public_holiday.csv')


def compute_agg_shipments():
    with cur:
        try:
            cur.execute(agg_shipments_table_drop)
        except Exception as e:
            print('Error: Issue dropping data')
            print(e)
    
    with cur:
        try:
            cur.execute(agg_shipments_table_create)
        except Exception as e:
            print('Error: Issue creating data')
            print(e)

    with cur:
        try:
            cur.execute(agg_shipments_table_insert)
        except Exception as e:
            print('Error: Issue inserting data')
            print(e)

    with open('agg_shipments.csv', 'w') as f:
        cur.copy_expert(f"COPY {ANALYTICS_SCHEMA}.agg_shipments TO STDOUT WITH HEADER CSV", f)
        
    s3.upload_file('agg_shipments.csv', S3_BUCKET_NAME, f'{EXPORT}/{ID}/agg_shipments.csv')


def compute_best_performing_product():
    with cur:
        try:
            cur.execute(best_performing_product_table_drop)
        except Exception as e:
            print('Error: Issue dropping data')
            print(e)
    
    with cur:
        try:
            cur.execute(best_performing_product_table_create)
        except Exception as e:
            print('Error: Issue creating data')
            print(e)

    with cur:
        try:
            cur.execute(best_performing_product_table_insert)
        except Exception as e:
            print('Error: Issue inserting data')
            print(e)

    with open('best_performing_product.csv', 'w') as f:
        cur.copy_expert(f"COPY {ANALYTICS_SCHEMA}.best_performing_product TO STDOUT WITH HEADER CSV", f)
        
    s3.upload_file('best_performing_product.csv', S3_BUCKET_NAME, f'{EXPORT}/{ID}/best_performing_product.csv')