#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from airflow.operators.python import PythonOperator
import sys
sys.path.append("/mnt/disk1/cicd/cn_data_platform/cndp-pipeline/etl/pyspark/utils")
from exec_model_utils import exec_ods_utils
from exec_model_utils import exec_dpd_utils


# ods
def ods_task(table_name, dag):
    return PythonOperator(
        task_id=table_name,
        provide_context=True,
        python_callable=exec_ods_utils,
        op_kwargs={'table_name': table_name, 'dag_name': dag.dag_id},
        dag=dag)


# dpd
def dpd_task(table_name, dag):
    return PythonOperator(
        task_id=table_name,
        provide_context=True,
        python_callable=exec_dpd_utils,
        op_kwargs={'table_name': table_name, 'dag_name': dag.dag_id},
        dag=dag
    )