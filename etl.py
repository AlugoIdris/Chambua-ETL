import boto3
from botocore import UNSIGNED
from botocore.client import Config
import psycopg2
import pandas as pd
import numpy as np
from psycopg2.extras import execute_values
import configparser

from sql_queries import *
from connection import *
from create_tables import *


config = configparser.ConfigParser()
config.read_file(open('cluster.config'))


cur = connect_to_datawarehouse()

s3, objects = connect_to_s3_storage()

def process_orders_data(cur, filename):
   
    def download_and_load_query_results(filename):
        object_lists = []
        for obj in objects['Contents'][1:]:
            object_lists.append(obj['Key'].split('/')[1].split('.')[0])
            
        if filename in object_lists:
            path = f'{filename}.csv'
            name = obj['Key'].split('/')[1].split('.')[0]
        s3.download_file(
            S3_BUCKET_NAME,
            Key = f"orders_data/{path}",
            Filename  = path,
        )

        return pd.read_csv(path)

    orders = download_and_load_query_results(filename)    
    data_orders = orders.values.tolist()

    try:
        cur.execute(orders_table_create)

    except psycopg2.Error as e:
        print('Error: Issue creating/loading table')
        print(e)
    
    try:
        execute_values(cur, orders_table_insert, data_orders, page_size=1000)

    except psycopg2.Error as e:
        print('Error: Issue Inserting Data')
        print(e)



def process_reviews_data(cur, filename):
   
    def download_and_load_query_results(filename):
        object_lists = []
        for obj in objects['Contents'][1:]:
            object_lists.append(obj['Key'].split('/')[1].split('.')[0])
            
        if filename in object_lists:
            path = f'{filename}.csv'
            name = obj['Key'].split('/')[1].split('.')[0]
        s3.download_file(
            S3_BUCKET_NAME,
            Key = f"orders_data/{path}",
            Filename  = path,
        )

        return pd.read_csv(path)
    
    reviews = download_and_load_query_results(filename)    
    data_reviews = reviews.values.tolist()

    try:
        cur.execute(reviews_table_create)

    except psycopg2.Error as e:
        print('Error: Issue creating/loading table')
        print(e)

    try:
        execute_values(cur, reviews_table_insert, data_reviews, page_size=1000)

    except psycopg2.Error as e:
        print('Error: Issue inserting table')
        print(e)


def process_shipment_data(cur, filename):

    def download_and_load_query_results(filename):
        object_lists = []
        for obj in objects['Contents'][1:]:
            object_lists.append(obj['Key'].split('/')[1].split('.')[0])
            
        if filename in object_lists:
            path = f'{filename}.csv'
            name = obj['Key'].split('/')[1].split('.')[0]
        s3.download_file(
            S3_BUCKET_NAME,
            Key = f"orders_data/{path}",
            Filename  = path,
        )

        return pd.read_csv(path)

    shipment_deliveries = download_and_load_query_results(filename)
    shipment_deliveries['shipment_date'] = pd.to_datetime(shipment_deliveries['shipment_date'], format= '%Y-%m-%d').dt.date
    shipment_deliveries['delivery_date'] = pd.to_datetime(shipment_deliveries['delivery_date'], format= '%Y-%m-%d').dt.date
    shipment_deliveries = shipment_deliveries.replace({np.NaN: None})   
    data_shipment_deliveries = shipment_deliveries.values.tolist()

    try:
        cur.execute(shipment_table_create)

    except psycopg2.Error as e:
        print('Error: Issue creating/loading table')
        print(e)

    try:
        execute_values(cur, shipment_table_insert, data_shipment_deliveries, page_size=1000)

    except psycopg2.Error as e:
        print('Error: Issue inserting Data')
        print(e)



if __name__ == "__main__":

    process_orders_data(cur, 'orders')
    process_reviews_data(cur, 'reviews')
    process_shipment_data(cur, 'shipment_deliveries')


    # Create agg_public_holiday table schema
    compute_agg_public_holiday()

    # Create compute_agg_shipments table schema
    compute_agg_shipments()

    # Create compute_best_performing_product table schema
    compute_best_performing_product()


