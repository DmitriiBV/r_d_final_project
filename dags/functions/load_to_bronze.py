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

file_path = f"{os.path.abspath(os.path.dirname(__file__))}/"
sys.path.append(file_path)


def authorization(cfg):
    extra = json.loads(cfg.extra)
    url = f"{cfg.host}{extra['AUTH']['endpoint']}"
    headers = {'content-type': extra['content_type']}
    data = {'username': extra['AUTH']['username'], 'password': extra['AUTH']['password']}
    r = requests.post(url, headers=headers, data=json.dumps(data), timeout=10)
    return r.json()[extra['AUTH']['token']]


def get_data(cfg, token, dt):
    url = f"{cfg.host}{cfg.schema}"
    extra = json.loads(cfg.extra)
    headers = {"content-type": extra['content_type'], "Authorization": f"{extra['API']['auth']} {token}"}
    data = {"date": dt}
    r = requests.get(url, headers=headers, data=json.dumps(data), timeout=10)
    return r.json()


def load_to_bronze_API(**kwargs):
    ds = kwargs.get('ds', str(date.today()))
    api_conn = BaseHook.get_connection('out_of_stock_api')
    hdfs_conn = BaseHook.get_connection('HDFS_WEB_CLIENT')
    extra = json.loads(api_conn.extra)
    token = authorization(api_conn)
    data = get_data(api_conn, token, ds)
    save_file = f"{extra['filename']}.json"
    url = f"{api_conn.host}{api_conn.schema}"
    logging.info(f"Writing file {save_file} from {url} to Bronze")
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


def load_to_bronze_postgreSQL(table, **kwargs):
    logging.info(f"Loading table '{table}' to Bronze")
    hdfs_conn = BaseHook.get_connection('HDFS_WEB_CLIENT')
    client = InsecureClient(f"{hdfs_conn.host}", user=hdfs_conn.login)
    ds = kwargs.get('ds', str(date.today()))
    pg_con = BaseHook.get_connection('oltp_postgres')

    pg_creds = {
        'host': pg_con.host,
        'port': pg_con.port,
        'user': pg_con.login,
        'password': pg_con.password,
        'database': pg_con.schema
    }

    with psycopg2.connect(**pg_creds) as pg_conn:
        logging.info(f"pg_conn 'connection to postgreSQL SUCCESS'")
        cursor = pg_conn.cursor()
        os.makedirs(os.path.join('.', 'datalake', 'bronze', 'pagila', table), exist_ok=True)
        with open(os.path.join('.', 'datalake', 'bronze', 'pagila', table, ds), 'w', encoding='UTF-8') as csv_file:
            cursor.copy_expert(f"COPY (SELECT * FROM {table}) TO STDOUT WITH HEADER CSV", csv_file)
            logging.info(f"{table}' Successfully loaded to filesystem")
        client.upload(os.path.join('/', 'datalake', 'bronze', 'pagila', table, ds), os.path.join('.', 'datalake', 'bronze', 'pagila', table, ds))
        logging.info(f"'{table}' Successfully loaded to Bronze")
        shutil.rmtree(os.path.join('.', 'datalake'))
        logging.info(f"'directory {os.path.join('.', 'datalake')}' 'DELETED'")
