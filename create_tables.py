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
