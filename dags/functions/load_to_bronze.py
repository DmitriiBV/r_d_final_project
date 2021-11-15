import logging
from datetime import date
import json
import os
import requests
import sys
from airflow.hooks.base_hook import BaseHook
from hdfs import InsecureClient

file_path = f"{os.path.abspath(os.path.dirname(__file__))}/"
file_directory_path = os.path.dirname(os.path.dirname(file_path))
config_directory_path = "configurations"
config_path = "config.yaml"
config_abspath = os.path.join(file_directory_path, config_directory_path, config_path)
sys.path.append(file_path)
sys.path.append(config_abspath)

from config import Config
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def authorization(cfg):
    url = f"{cfg['url']}{cfg['endpoint']}"
    headers = {'content-type': cfg['content_type']}
    data = {'username': cfg['username'], 'password': cfg['password']}
    r = requests.post(url, headers=headers, data=json.dumps(data), timeout=10)
    return r.json()[cfg['token']]


def get_data(cfg, token, dt):
    url = f"{cfg['url']}{cfg['endpoint']}"
    headers = {"content-type": cfg['content_type'], "Authorization": f"{cfg['auth']} {token}"}
    data = {"date": dt}
    r = requests.get(url, headers=headers, data=json.dumps(data), timeout=10)
    return r.json()


def load_to_bronze_API(**kwargs):
    ds = kwargs.get('ds', str(date.today()))
    config = Config(config_abspath)
    cfg_api = config.get_config('API')
    logging.info(f"cfg_api = {cfg_api}")
    token = authorization(config.get_config('AUTH'))
    logging.info(f"token = {token}")
    data = get_data(cfg_api, token, ds)
    logging.info(f"data = {data}")
    cfg_save = config.get_config('SAVE')
    save_file = f"{cfg_save['file_name']}.json"
    url = f"{cfg_api['url']}{cfg_api['endpoint']}"
    logging.info(f"Writing file {save_file} from {url} to Bronze")
    logging.info(f"client_data = {config.get_config('HDFS_WEB_CLIENT')}")
    client = InsecureClient(**config.get_config('HDFS_WEB_CLIENT'))
    with client.write(os.path.join('/', 'datalake', 'bronze', 'API', save_file, ds), encoding='utf-8') as json_file:
        json.dump(data, json_file)

    logging.info(f"'File {save_file}' Successfully loaded to Bronze")


def load_to_bronze_spark(table, **kwargs):
    logging.info(f"Loading table '{table}' to Bronze")

    ds = kwargs.get('ds', str(date.today()))

    pg_conn = BaseHook.get_connection('oltp_postgres')

    pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
    pg_properties = {"user": pg_conn.login, "password": pg_conn.password}

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.3.1.jar') \
        .master('local') \
        .appName('load_to_bronze') \
        .getOrCreate()

    logging.info(f"Writing table {table} from {pg_conn.host} to Bronze")

    table_df = spark.read.jdbc(pg_url, table=table, properties=pg_properties)

    table_df = table_df.select([F.col(c).cast("string") for c in table_df.columns])

    table_df.write.option("header", True).csv(
        os.path.join('/', 'datalake', 'bronze', 'pagila', table, ds),
        mode="overwrite")

    logging.info(f"'{table}' Successfully loaded to Bronze")
    logging.info(f"'{table_df.count()}' rows written")
