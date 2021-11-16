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
    url = f"{cfg.host}{cfg.extra['AUTH']['endpoint']}"
    headers = {'content-type': cfg.extra['content_type']}
    data = {'username': cfg.extra['AUTH']['username'], 'password': cfg.extra['AUTH']['password']}
    r = requests.post(url, headers=headers, data=json.dumps(data), timeout=10)
    return r.json()[cfg.extra['AUTH']['token']]


def get_data(cfg, token, dt):
    url = f"{cfg.host}{cfg.schema}"
    headers = {"content-type": cfg.extra['content_type'], "Authorization": f"{cfg.extra['API']['auth']} {token}"}
    data = {"date": dt}
    r = requests.get(url, headers=headers, data=json.dumps(data), timeout=10)
    return r.json()


def load_to_bronze_API(**kwargs):
    ds = kwargs.get('ds', str(date.today()))
    api_conn = BaseHook.get_connection('out_of_stock_api')
    hdfs_conn = BaseHook.get_connection('HDFS_WEB_CLIENT')
    logging.info(f"api_host = {api_conn.host}")
    token = authorization(api_conn)
    logging.info(f"token = {token}")
    data = get_data(api_conn, token, ds)
    logging.info(f"data = {data}")
    save_file = f"{api_conn.extra['filename']}.json"
    url = f"{api_conn.host}{api_conn.schema}"
    logging.info(f"Writing file {save_file} from {url} to Bronze")
    logging.info(f"client_data = {hdfs_conn.host}")
    client = InsecureClient(f"{hdfs_conn.host}", user=hdfs_conn.login)
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
