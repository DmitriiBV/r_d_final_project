import logging
import os
import sys
from airflow.hooks.base_hook import BaseHook
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

    logging.info(f"Read 'out_of_stock'")
    API_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'API', 'out_of_stock.json'))
    logging.info(f"Write 'out_of_stock' to greenplumDB")
    API_df.write.jdbc(gp_url, table='outOfStock', properties=gp_properties)
    logging.info(f"Loading 'out_of_stock' to greenplumDB Completed")

    logging.info(f"Read 'aisles'")
    aisles_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'aisles'))
    logging.info(f"Write 'aisles' to greenplumDB")
    aisles_df.write.jdbc(gp_url, table='aisles', properties=gp_properties)
    logging.info(f"Loading 'aisles' to greenplumDB Completed")

    logging.info(f"Read 'clients'")
    clients_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'clients'))
    logging.info(f"Write 'clients' to greenplumDB")
    clients_df.write.jdbc(gp_url, table='clients', properties=gp_properties)
    logging.info(f"Loading 'clients' to greenplumDB Completed")

    logging.info(f"Read 'departments'")
    departments_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'departments'))
    logging.info(f"Write 'departments' to greenplumDB")
    departments_df.write.jdbc(gp_url, table='departments', properties=gp_properties)
    logging.info(f"Loading 'departments' to greenplumDB Completed")

    logging.info(f"Read 'location_areas'")
    location_areas_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'location_areas'))
    logging.info(f"Write 'location_areas' to greenplumDB")
    location_areas_df.write.jdbc(gp_url, table='location_areas', properties=gp_properties)
    logging.info(f"Loading 'location_areas' to greenplumDB Completed")

    logging.info(f"Read 'orders'")
    order_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'orders'))
    logging.info(f"Write 'orders' to greenplumDB")
    order_df.write.jdbc(gp_url, table='orders', properties=gp_properties)
    logging.info(f"Loading 'orders' to greenplumDB Completed")

    logging.info(f"Read 'products'")
    products_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'products'))
    logging.info(f"Write 'products' to greenplumDB")
    products_df.write.jdbc(gp_url, table='products', properties=gp_properties)
    logging.info(f"Loading 'products' to greenplumDB Completed")

    logging.info(f"Read 'store_types'")
    store_types_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'store_types'))
    logging.info(f"Write 'store_types' to greenplumDB")
    store_types_df.write.jdbc(gp_url, table='store_types', properties=gp_properties)
    logging.info(f"Loading 'store_types' to greenplumDB Completed")

    logging.info(f"Read 'stores'")
    stores_df = spark \
        .read \
        .parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'stores'))
    logging.info(f"Write 'stores' to greenplumDB")
    stores_df.write.jdbc(gp_url, table='stores', properties=gp_properties)
    logging.info(f"Loading 'stores' to greenplumDB Completed")
