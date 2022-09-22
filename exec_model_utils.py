#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import logging
import os
import random
import datetime
import subprocess


# ods
def exec_ods_utils(**kwargs):
    table_name = kwargs.get("table_name")
    print(table_name)
    path_root = '/mnt/disk1/cicd/cn_data_platform/cndp-pipeline/etl/pyspark/ods_conf/'
    json_file = '{}{}.json'.format(path_root, table_name)
    print(json_file)
    dag_name = kwargs.get('dag_name')
    print(dag_name)
    execution_time_utc = kwargs['execution_date']
    logging.info("below is UTC execution time")
    logging.info(execution_time_utc)
    execution_time = execution_time_utc + datetime.timedelta(hours=8)
    execution_date = execution_time.strftime('%Y%m%d')
    pt_val = execution_date
    print(pt_val)
    spark_cmd = "spark-submit " \
                "--master yarn " \
                "--deploy-mode client " \
                "--queue cdp " \
                "--num-executors 5 " \
                "--executor-memory 8G " \
                "--executor-cores 4 " \
                "--driver-memory 3G " \
                "/mnt/disk1/cicd/cn_data_platform/cndp-pipeline/etl/pyspark/utils/ods_ingestion_utils.py {} {} {}".format(
        json_file, dag_name, pt_val)
    gtw_server_list = os.environ['GTW_SERVER_LIST'].split(',')
    gtw = gtw_server_list[random.randint(0, len(gtw_server_list) - 1)]
    cmd = "ssh cndp@{gtw} 'bash -l {spark_cmd}'".format(spark_cmd=spark_cmd, gtw=gtw)
    print(cmd)

    popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out_data, err_data = [it.strip() for it in popen.communicate()]
    if type(err_data) == bytes:
        err_data = err_data.decode('utf8').strip()
        logging.info("""pyspark job return code is {}, and the error message is: \n {}""".format(int(popen.returncode),
                                                                                                 str(err_data)))
    if type(out_data) == bytes:
        out_data = out_data.decode('utf8').strip()
        logging.info(
            """pyspark job return code is {}, and out data is: \n {}""".format(int(popen.returncode), str(out_data)))
    if int(popen.returncode) != 0:
        raise Exception("pyspark job executed failed")


# dpd1
def exec_dpd_utils(**kwargs):
    table_name = kwargs.get("table_name")
    print(table_name)
    path_root = '/mnt/disk1/cicd/cn_data_platform/cndp-pipeline/etl/pyspark/dpd_conf/'
    json_file = '{}{}.json'.format(path_root, table_name)
    print(json_file)
    dag_name = kwargs.get('dag_name')
    print(dag_name)
    execution_time_utc = kwargs['execution_date']
    logging.info("below is UTC execution time")
    logging.info(execution_time_utc)
    execution_time = execution_time_utc + datetime.timedelta(hours=8)
    execution_date = execution_time.strftime('%Y%m%d')
    pt_val = execution_date
    print(pt_val)
    spark_cmd = "spark-submit " \
                "--master yarn " \
                "--deploy-mode client " \
                "--queue cdp " \
                "--num-executors 5 " \
                "--executor-memory 8G " \
                "--executor-cores 4 " \
                "--driver-memory 3G " \
                "/mnt/disk1/cicd/cn_data_platform/cndp-pipeline/etl/pyspark/utils/dpd_inc_utils.py {} {} {}".format(
        json_file, dag_name, pt_val)
    gtw_server_list = os.environ['GTW_SERVER_LIST'].split(',')
    gtw = gtw_server_list[random.randint(0, len(gtw_server_list) - 1)]
    cmd = "ssh cndp@{gtw} 'bash -l {spark_cmd}'".format(spark_cmd=spark_cmd, gtw=gtw)
    print(cmd)

    popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out_data, err_data = [it.strip() for it in popen.communicate()]
    if type(err_data) == bytes:
        err_data = err_data.decode('utf8').strip()
        logging.info("""pyspark job return code is {}, and the error message is: \n {}""".format(int(popen.returncode),
                                                                                                 str(err_data)))
    if type(out_data) == bytes:
        out_data = out_data.decode('utf8').strip()
        logging.info(
            """pyspark job return code is {}, and out data is: \n {}""".format(int(popen.returncode), str(out_data)))
    if int(popen.returncode) != 0:
        raise Exception("pyspark job executed failed")


def exec_dpd_model(**kwargs):
    table_name = kwargs.get("table_name")
    print(table_name)
    path_root = '/mnt/disk1/cicd/cn_data_platform/cndp-pipeline/etl/pyspark/dpd_conf/'
    json_file = '{}{}.json'.format(path_root, table_name)
    print(json_file)
    dag_name = kwargs.get('dag_name')
    print(dag_name)
    execution_time_utc = kwargs['execution_date']
    logging.info("below is UTC execution time")
    logging.info(execution_time_utc)
    execution_time = execution_time_utc + datetime.timedelta(hours=8)
    execution_date = execution_time.strftime('%Y%m%d')
    pt_val = execution_date
    print(pt_val)
    spark_cmd = "spark-submit " \
                "--master yarn " \
                "--deploy-mode client " \
                "--queue cdp " \
                "--num-executors 5 " \
                "--executor-memory 8G " \
                "--executor-cores 4 " \
                "--driver-memory 3G " \
                "/mnt/disk1/cicd/cn_data_platform/cndp-pipeline/etl/pyspark/utils/dpd_inc_utils.py {} {} {}".format(
        json_file, dag_name, pt_val)
    gtw_server_list = os.environ['GTW_SERVER_LIST'].split(',')
    gtw = gtw_server_list[random.randint(0, len(gtw_server_list) - 1)]
    cmd = "ssh cndp@{gtw} 'bash -l {spark_cmd}'".format(spark_cmd=spark_cmd, gtw=gtw)
    print(cmd)

    popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out_data, err_data = [it.strip() for it in popen.communicate()]
    if type(err_data) == bytes:
        err_data = err_data.decode('utf8').strip()
        logging.info("""pyspark job return code is {}, and the error message is: \n {}""".format(int(popen.returncode),
                                                                                                 str(err_data)))
    if type(out_data) == bytes:
        out_data = out_data.decode('utf8').strip()
        logging.info(
            """pyspark job return code is {}, and out data is: \n {}""".format(int(popen.returncode), str(out_data)))
    if int(popen.returncode) != 0:
        raise Exception("pyspark job executed failed")


def exec_ods_model(**kwargs):
    table_name = kwargs.get("table_name")
    print(table_name)
    path_root = '/mnt/disk1/cicd/cn_data_platform/cndp-pipeline/etl/pyspark/ods_conf/'
    json_file = '{}{}.json'.format(path_root, table_name)
    print(json_file)
    dag_name = kwargs.get('dag_name')
    print(dag_name)
    execution_time_utc = kwargs['execution_date']
    logging.info("below is UTC execution time")
    logging.info(execution_time_utc)
    execution_time = execution_time_utc + datetime.timedelta(hours=8)
    execution_date = execution_time.strftime('%Y%m%d')
    pt_val = execution_date
    print(pt_val)
    spark_cmd = "spark-submit " \
                "--master yarn " \
                "--deploy-mode client " \
                "--queue cdp " \
                "--num-executors 5 " \
                "--executor-memory 8G " \
                "--executor-cores 4 " \
                "--driver-memory 3G " \
                "/mnt/disk1/cicd/cn_data_platform/cndp-pipeline/etl/pyspark/utils/ods_ingestion_csv_utils.py {} {} {}".format(
        json_file, dag_name, pt_val)
    gtw_server_list = os.environ['GTW_SERVER_LIST'].split(',')
    gtw = gtw_server_list[random.randint(0, len(gtw_server_list) - 1)]
    cmd = "ssh cndp@{gtw} 'bash -l {spark_cmd}'".format(spark_cmd=spark_cmd, gtw=gtw)
    print(cmd)

    popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out_data, err_data = [it.strip() for it in popen.communicate()]
    if type(err_data) == bytes:
        err_data = err_data.decode('utf8').strip()
        logging.info("""pyspark job return code is {}, and the error message is: \n {}""".format(int(popen.returncode),
                                                                                                 str(err_data)))
    if type(out_data) == bytes:
        out_data = out_data.decode('utf8').strip()
        logging.info(
            """pyspark job return code is {}, and out data is: \n {}""".format(int(popen.returncode), str(out_data)))
    if int(popen.returncode) != 0:
        raise Exception("pyspark job executed failed")