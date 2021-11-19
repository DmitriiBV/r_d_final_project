import logging
import os
import sys
from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from airflow.hooks.base_hook import BaseHook
import json

file_path = f"{os.path.abspath(os.path.dirname(__file__))}/"
file_directory_path = os.path.dirname(os.path.dirname(file_path))
config_directory_path = "configurations"
config_path = "config.yaml"
config_abspath = os.path.join(file_directory_path, config_directory_path, config_path)
sys.path.append(file_path)
sys.path.append(config_abspath)

from config import Config


def load_to_silver_API(**kwargs):
    ds = kwargs.get('ds', str(date.today()))
    config = Config(config_abspath)
    cfg_save = config.get_config('SAVE')
    save_file = f"{cfg_save['file_name']}.json"

    spark = SparkSession.builder \
        .master('local') \
        .appName('load_to_silver') \
        .getOrCreate()

    logging.info(f"Writing file {save_file} to Silver")

    json_df = spark.read \
        .option("multiline", "true") \
        .json(os.path.join('/', 'datalake', 'bronze', 'API', save_file, ds))

    json_df.write.parquet(os.path.join('/', 'datalake', 'silver', 'API', save_file),
                          mode='overwrite')

    logging.info(f"Loading file {save_file} to Silver Completed")


def load_to_silver(table, **kwargs):
    ds = kwargs.get('ds', str(date.today()))

    spark = SparkSession.builder \
        .master('local') \
        .appName('load_to_silver') \
        .getOrCreate()

    logging.info(f"Loading table {table} to Silver")

    table_df = spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .csv(os.path.join('/', 'datalake', 'bronze', 'pagila', table, ds))

    table_df = table_df.dropDuplicates()

    table_df.write.parquet(os.path.join('/', 'datalake', 'silver', 'pagila', table),
                           mode='overwrite')

    logging.info(f"Loading table {table} to Silver Completed")


def transform_and_load_to_silver(**kwargs):
    ds = kwargs.get('ds', str(date.today()))

    logging.info(f"Loading DATA to Silver")

    spark = SparkSession.builder \
        .master('local') \
        .appName('load_to_silver') \
        .getOrCreate()

    api_conn = BaseHook.get_connection('out_of_stock_api')
    extra = json.loads(api_conn.extra)
    save_file = f"{extra['filename']}.json"

    schemaApi = StructType() \
        .add('product_id', IntegerType(), True) \
        .add('date', TimestampType(), True)
    logging.info(f"Read {save_file}")
    Api_df = spark.read \
        .option("multiline", "true") \
        .schema(schemaApi) \
        .json(os.path.join('/', 'datalake', 'bronze', 'API', save_file, ds))
    Api_df = Api_df.dropDuplicates()
    logging.info(f"Write to silver {save_file}")
    Api_df.write.parquet(os.path.join('/', 'datalake', 'silver', 'API', save_file),
                         mode='overwrite')
    logging.info(f"Loading {save_file} to Silver Completed")

    schemaProducts = StructType() \
        .add('product_id', IntegerType(), True) \
        .add('product_name', StringType(), True) \
        .add('aisle_id', IntegerType(), True) \
        .add('department_id', IntegerType(), True)
    logging.info(f"Read table 'products'")
    products_df = spark \
        .read \
        .schema(schemaProducts) \
        .option("header", "True") \
        .csv(os.path.join('/', 'datalake', 'bronze', 'pagila', 'products', ds))
    products_df = products_df.dropDuplicates()
    logging.info(f"Write table 'products' to silver")
    products_df.write.parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'products'),
                              mode='overwrite')
    logging.info(f"Loading table 'products' to Silver Completed")

    aisles_schema = StructType() \
        .add('aisle_id', IntegerType(), True) \
        .add('aisle', StringType(), True)
    logging.info(f"Read table 'aisles'")
    aisles_df = spark \
        .read \
        .schema(aisles_schema) \
        .option("header", "True") \
        .csv(os.path.join('/', 'datalake', 'bronze', 'pagila', 'aisles', ds))
    aisles_df = aisles_df.dropDuplicates()
    logging.info(f"Write table 'aisles' to silver")
    aisles_df.write.parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'aisles'),
                            mode='overwrite')
    logging.info(f"Loading table 'aisles' to Silver Completed")

    clients_schema = StructType() \
        .add('id', IntegerType(), True) \
        .add('fullname', StringType(), True) \
        .add('location_area_id', IntegerType(), True)
    logging.info(f"Read table 'clients'")
    clients_df = spark \
        .read \
        .schema(clients_schema) \
        .option("header", "True") \
        .csv(os.path.join('/', 'datalake', 'bronze', 'pagila', 'clients', ds))
    clients_df = clients_df.dropDuplicates()
    logging.info(f"Write table 'clients' to silver")
    clients_df.write.parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'clients'),
                             mode='overwrite')
    logging.info(f"Loading table 'clients' to Silver Completed")

    departments_schema = StructType() \
        .add('department_id', IntegerType(), True) \
        .add('department', StringType(), True)
    logging.info(f"Read table 'departments'")
    departments_df = spark \
        .read \
        .schema(departments_schema) \
        .option("header", "True") \
        .csv(os.path.join('/', 'datalake', 'bronze', 'pagila', 'departments', ds))
    departments_df = departments_df.dropDuplicates()
    logging.info(f"Write table 'departments' to silver")
    departments_df.write.parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'departments'),
                                 mode='overwrite')
    logging.info(f"Loading table 'departments' to Silver Completed")

    location_areas_schema = StructType() \
        .add('area_id', IntegerType(), True) \
        .add('area', StringType(), True)
    logging.info(f"Read table 'location_areas'")
    location_areas_df = spark \
        .read \
        .schema(location_areas_schema) \
        .option("header", "True") \
        .csv(os.path.join('/', 'datalake', 'bronze', 'pagila', 'location_areas', ds))
    location_areas_df = location_areas_df.dropDuplicates()
    logging.info(f"Write table 'location_areas' to silver")
    location_areas_df.write.parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'location_areas'),
                                    mode='overwrite')
    logging.info(f"Loading table 'location_areas' to Silver Completed")

    orders_schema = StructType() \
        .add('order_id', IntegerType(), True) \
        .add('product_id', IntegerType(), True) \
        .add('client_id', IntegerType(), True) \
        .add('store_id', IntegerType(), True) \
        .add('quantity', IntegerType(), True) \
        .add('order_date', TimestampType(), True)
    logging.info(f"Read table 'orders'")
    orders_df = spark \
        .read \
        .schema(orders_schema) \
        .option("header", "True") \
        .csv(os.path.join('/', 'datalake', 'bronze', 'pagila', 'orders', ds))
    orders_df = orders_df.dropDuplicates()
    logging.info(f"Write table 'orders' to silver")
    orders_df.write.parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'orders'),
                            mode='overwrite')
    logging.info(f"Loading table 'orders' to Silver Completed")

    store_types_schema = StructType() \
        .add('store_type_id', IntegerType(), True) \
        .add('type', StringType(), True)
    logging.info(f"Read table 'store_types'")
    store_types_df = spark \
        .read \
        .schema(store_types_schema) \
        .option("header", "True") \
        .csv(os.path.join('/', 'datalake', 'bronze', 'pagila', 'store_types', ds))
    store_types_df = store_types_df.dropDuplicates()
    logging.info(f"Write table 'store_types' to silver")
    store_types_df.write.parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'store_types'),
                                 mode='overwrite')
    logging.info(f"Loading table 'store_types' to Silver Completed")

    stores_schema = StructType() \
        .add('store_id', IntegerType(), True) \
        .add('location_area_id', IntegerType(), True) \
        .add('store_type_id', IntegerType(), True)
    logging.info(f"Read table 'stores'")
    stores_df = spark \
        .read \
        .schema(stores_schema) \
        .option("header", "True") \
        .csv(os.path.join('/', 'datalake', 'bronze', 'pagila', 'stores', ds))
    stores_df = stores_df.dropDuplicates()
    logging.info(f"Write table 'stores' to silver")
    stores_df.write.parquet(os.path.join('/', 'datalake', 'silver', 'pagila', 'stores'),
                            mode='overwrite')
    logging.info(f"Loading table 'stores' to Silver Completed")

    logging.info(f"LOADING 'DATA' to Silver 'COMPLETED'")
