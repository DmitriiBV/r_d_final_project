import logging
from datetime import date
import json
import os
import requests
import sys
from airflow.hooks.base_hook import BaseHook
from hdfs import InsecureClient
import psycopg2
import shutil

import pyspark.sql.functions as F
from pyspark.sql import SparkSession



def load_to_greenplum(**kwargs):
    logging.info(f"Loading DATA to greenplumDB")

    ds = kwargs.get('ds', str(date.today()))
    gp_conn = BaseHook.get_connection('greenplum_olap')

    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/{gp_conn.schema}"
    gp_properties = {"user": gp_conn.login, "password": gp_conn.password}

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.3.1.jar') \
        .master('local') \
        .appName('load_to_bronze') \
        .getOrCreate()

    API_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'API', 'out_of_stock.json'))

    API_df.write.jdbc(gp_url, table='outOfStock', properties=gp_properties)

    aisles_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'aisles'))

    aisles_df.write.jdbc(gp_url, table='aisles', properties=gp_properties)

    clients_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'clients'))

    clients_df.write.jdbc(gp_url, table='clients', properties=gp_properties)

    departments_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'departments'))

    departments_df.write.jdbc(gp_url, table='departments', properties=gp_properties)

    location_areas_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'location_areas'))

    location_areas_df.write.jdbc(gp_url, table='location_areas', properties=gp_properties)

    order_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'orders'))

    order_df.write.jdbc(gp_url, table='orders', properties=gp_properties)

    products_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'products'))

    products_df.write.jdbc(gp_url, table='products', properties=gp_properties)

    store_types_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'store_types'))

    store_types_df.write.jdbc(gp_url, table='store_types', properties=gp_properties)

    stores_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'stores'))

    stores_df.write.jdbc(gp_url, table='stores', properties=gp_properties)
