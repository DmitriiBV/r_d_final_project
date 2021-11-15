import logging
import os
import sys
from datetime import date
from pyspark.sql import SparkSession

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

    json_df = spark.read\
        .option("multiline", "true")\
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
