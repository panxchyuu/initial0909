#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import json
from pyspark.sql import functions as func
from pyspark.sql import SparkSession
import math
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import logging
import requests
import subprocess


def get_spark_session(app_name, over_pt=False):
    partitionOverwriteMode = 'dynamic'
    if over_pt:
        partitionOverwriteMode = "static"
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hive.mapred.supports.subdirectories", "true") \
        .config("hive.exec.orc.split.strategy", "ETL") \
        .config('spark.sql.orc.mergeSchema', True) \
        .config('spark.sql.hive.convertMetastoreOrc', False) \
        .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", partitionOverwriteMode) \
        .config("spark.sql.crossJoin.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .config("spark.sql.adaptiveBroadcastJoinThreshold", -1) \
        .config("spark.sql.broadcastTimeout", 600) \
        .config("spark.sql.shuffle.partitions", "500") \
        .config("spark.shuffle.consolidateFiles", "true") \
        .config("spark.sql.mergeSmallFileSize", "67108864") \
        .config("spark.sql.orc.filterPushdown", "true") \
        .config("spark.sql.hive.verifyPartitionPath", "true") \
        .config("spark.sql.session.timeZone", "UTC") \
        .enableHiveSupport().getOrCreate()


def get_ods_config(config_path):
    basic_info_dict = json.load(open(config_path))
    return basic_info_dict


def get_vault_info(env_name, vault_folder):
    logging.info("Now in {} environment".format(env_name))
    role_id = os.environ['VAULT_ROLE_ID']
    secret_id = os.environ['VAULT_SECRET_ID']
    if env_name == 'dev':
        vault_login_url = os.environ['VAULT_LOGIN_URL']
        get_secret_url = os.environ['VAULT_SECRET_URL']
    else:
        vault_login_url = "https://vault-china.dktapp.cloud/v1/auth/approle/login"
        get_secret_url = "https://vault-china.dktapp.cloud/v1/secret/data/cs-bi-asia/cndp"
    logging.info("Get vault role id and vault secret id success")
    data = {"role_id": role_id, "secret_id": secret_id}
    r = requests.post(vault_login_url, data=data)
    token = r.json()["auth"]["client_token"]
    header = {'X-Vault-Token': token}
    get_url = "{prefix}/{env_name}/{vault_folder}".format(prefix=get_secret_url, env_name=env_name,
                                                          vault_folder=vault_folder)
    logging.info("Send request for url {}".format(get_url))
    vault_info = requests.get(get_url, headers=header)
    if int(vault_info.status_code) != 200:
        raise Exception(
            "the response status code is {}, please check the secret name in vault.".format(vault_info.status_code))
    else:
        connection_info = vault_info.json()["data"]["data"]
    return connection_info


def get_db_info(conn_info_dict):
    dict_keys = conn_info_dict.keys()
    database = conn_info_dict[[item for item in dict_keys if str(item).endswith("dbname")][0]]
    user = conn_info_dict[[item for item in dict_keys if str(item).endswith("username")][0]]
    password = conn_info_dict[[item for item in dict_keys if str(item).endswith("password")][0]]
    host = conn_info_dict[[item for item in dict_keys if str(item).endswith("host")][0]]
    port = conn_info_dict[[item for item in dict_keys if str(item).endswith("port")][0]]
    return database, user, password, host, port


def get_oss_info(conn_info_dict):
    dict_keys = conn_info_dict.keys()
    endpoint = conn_info_dict[[item for item in dict_keys if str(item).endswith("endpoint")][0]]
    bucket = conn_info_dict[[item for item in dict_keys if str(item).endswith("bucket")][0]]
    access_key = conn_info_dict[[item for item in dict_keys if str(item).endswith("access_key")][0]]
    secret_key = conn_info_dict[[item for item in dict_keys if str(item).endswith("secret_key")][0]]
    path = conn_info_dict[[item for item in dict_keys if str(item).endswith("path")][0]]
    return endpoint, bucket, access_key, secret_key, path


def get_target_info(spark, target_table):
    ods_pt_name = str(spark.sql("show tblproperties {}('pt_key')".format(target_table)).collect()[0][0])
    source_condition = str(spark.sql("show tblproperties {}('pt_value')".format(target_table)).collect()[0][0])
    load_mode = str(spark.sql("show tblproperties {}('mode')".format(target_table)).collect()[0][0])
    print("-----osd_pt_name is: {}-----".format(ods_pt_name))
    print("-----pt condition value is: {}-----".format(source_condition))
    print("-----ingestion load mode is: {}-----".format(load_mode))
    return ods_pt_name, load_mode, source_condition


def source_rds_pgs(load_mode, source_table, source_condition, pt_val):
    print("------------the load mode is: {}------------".format(load_mode))
    load_query = ""
    if load_mode == "full":
        load_query = "(select * from {}) t".format(source_table)
    elif load_mode == "inc":
        load_query = "(select * from {} where {}='{}') t".format(source_table, source_condition, pt_val)
    print("--------------source load query is--------------\n{}".format(load_query))
    return load_query


def source_ingestion(config_path, dag_name, pt_val):
    basic_info_dict = get_ods_config(config_path)
    env_name = os.environ["ENV_NAME"]
    source_type = basic_info_dict["source_type"]
    source_table = basic_info_dict["source_table"]["table_name"]
    target_table = basic_info_dict["target_table"]["table_name"]
    vault_folder = basic_info_dict["source_table"]["vault_folder"]
    conn_info_dict = get_vault_info(env_name, vault_folder)
    spark = get_spark_session(target_table.split('.')[1])
    ods_pt_name, load_mode, source_condition = get_target_info(spark, target_table)
    if str(source_type).lower().strip() in ["postgresql", "mysql"]:
        database, user, password, host, port = get_db_info(conn_info_dict)
        source_schema_name = source_table.split('.')[0]
        user = user
        password = password
        # properties = {"user": user, "password": password}
        db_type = str(source_type).lower().strip()
        print(db_type)
        if db_type == "postgresql":
            url = 'jdbc:postgresql://{}:{}/{}?searchpath={}'.format(host, port, database, source_schema_name)
            print("the DB link information is {}".format(url))
            driver = "org.postgresql.Driver"
            print("the DB driver is {}".format(driver))
        elif db_type == 'mysql':
            url = "jdbc:mysql://{}:{}/{}?useSSL=false".format(host, port, database)
            print("the DB link information is {}".format(url))
            driver = "com.mysql.jdbc.Driver"
            print("the DB driver is {}".format(driver))
        else:
            raise Exception("------{} is not in framework, please contact admin to add this DB type------".format(db_type))
        load_query = source_rds_pgs(load_mode, source_table, source_condition, pt_val)
        if not load_query:
            raise Exception("-----please check the load mode in ods ddl, if it not in ('full', 'inc'), please check it-----")
        options = {
            "url": url,
            "dbtable": load_query,
            "user": user,
            "password": password,
            "driver": driver,
            "numPartitions": 10
        }
        source_df = spark.read.format("jdbc").options(**options).load()
        # source_df = spark.read.jdbc(url=url, table=load_query, properties=properties)
    elif str(source_type).lower().strip() == 'oss':
        endpoint, bucket, access_key, secret_key, path = get_oss_info(conn_info_dict)
        para = basic_info_dict["source_table"]["para"]
        file_type = para["file_type"]
        para.pop("file_type")
        para["header"] = True
        print("-----the options para is {}-----".format(para))
        options = para
        input_path = "oss://{ak}:{sk}@{bucket}.{endpoint}/{path}/{table_name}"\
            .format(ak=access_key, sk=secret_key, bucket=bucket, endpoint=endpoint, path=path,
                    table_name=str(source_table).format(execution_date=pt_val))
        print("-----the source data folder is: {}-----".format(input_path))
        add_col_list = ["hive_create_at", "hive_create_by", "hive_update_at", "hive_update_by", "batch_id", "pt"]
        ods_schema = spark.sql("select * from {} where 0=1".format(target_table)).drop(*add_col_list).schema
        source_df = spark.read.format(file_type).options(**options).schema(ods_schema).load(input_path)
    # elif str(source_type).lower().strip() == 'sftp':
    #     source_df = ''
    else:
        raise Exception("-----the source type is not in dataframe, please check the ods config file or contact admim to add this type-----")

    ods_df = source_df \
        .withColumn("hive_create_at", func.current_timestamp()) \
        .withColumn("hive_create_by", func.lit(dag_name)) \
        .withColumn("hive_update_at", func.current_timestamp()) \
        .withColumn("hive_update_by", func.lit(dag_name)) \
        .withColumn("batch_id", func.lit('NULL')) \
        .withColumn("{}".format(ods_pt_name), func.lit(pt_val))
    hive_schema = spark.sql("select * from {} where 0=1".format(target_table)).dtypes
    df_schema = ods_df.dtypes
    if hive_schema == df_schema:
        final_df = ods_df
    else:
        select_expr = [func.col(c).cast(t) for c, t in hive_schema]
        final_df = ods_df.select(*select_expr)
    ods_df.show(10)
    counter = int(math.ceil(final_df.count() / 2000000)) + 1
    sub_folder = target_table.split('.')[1].split('_{}'.format(source_table.split('.')[1]))[0]
    print("-----the sub folder is {}".format(sub_folder))
    write_path = 'oss://decathlon-{}-cndp-ods/{}/{}'.format(env_name, sub_folder, target_table.split('.')[1])
    print("-----the write path is {}-----".format(write_path))
    final_df.repartition(counter).write.partitionBy(ods_pt_name).mode('overwrite').orc(write_path)
    subprocess.check_call("hive -e 'MSCK REPAIR TABLE {} sync partitions'".format(target_table), shell=True)
    spark.stop()


if __name__ == '__main__':
    source_ingestion(sys.argv[1], sys.argv[2], sys.argv[3])