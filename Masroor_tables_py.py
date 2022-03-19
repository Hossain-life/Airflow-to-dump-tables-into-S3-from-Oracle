import datetime
import os
import sys
import cx_Oracle
import csv
import pandas as pd
from smart_open import open

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.providers.oracle.hooks.oracle  import OracleHook
from airflow.operators.dummy import DummyOperator
from kubernetes.client import models as k8s
from airflow.settings import AIRFLOW_HOME

with DAG(
    dag_id="dump_oracle_to_s3_masroornew",
    start_date=datetime.datetime(2021, 11, 24),
    schedule_interval="&#64;once",
    catchup=False,
) as dag:

    def OracleToParquetToS3(table_name, target_bucket, file_key):
        SQL= 'SELECT * FROM ' + table_name
        conn_masroornew = OracleHook(oracle_conn_id='con-ora-ebs').get_conn()

        df = pd.read_sql(SQL, conn_masroornew)
        
        s3 = S3Hook(aws_conn_id='con-s3-prod')

        ## Dump as parquet directly to S3:
        with open(f"s3://{target_bucket}/{file_key}", 'wb', transport_params={'client': s3.get_conn()}) as out_file:
            df.to_parquet(out_file, engine='pyarrow', index=False)
            
        oracle_conn.close()  
    
    start=DummyOperator(task_id="start")

    finish=DummyOperator(task_id="finish")

    dump_APPS__AP_LOOKUP_CODES = PythonOperator(task_id='dump_apps_AP_LOOKUP_CODES',
        python_callable=OracleToParquetToS3, 
        op_kwargs={
              "table_name": "APPS.AP_LOOKUP_CODES",
              "target_bucket": "bigdata-prod-dyttnieq",
              "file_key":"RAW_Data_Lake/EBS_12_2_5/AP/AP_LOOKUP_CODES.parquet"
        },
        executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    node_selector={
                        "node-group": "master"
                    },
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    )

    dump_APPS__AP_TERMS_VAL_V = PythonOperator(task_id='dump_apps_AP_TERMS_VAL_V',
        python_callable=OracleToParquetToS3, 
        op_kwargs={
              "table_name": "APPS.AP_TERMS_VAL_V",
              "target_bucket": "bigdata-prod-dyttnieq",
              "file_key":"RAW_Data_Lake/EBS_12_2_5/AP/AP_TERMS_VAL_V.parquet"
        },
        executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    node_selector={
                        "node-group": "master"
                    },
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    )

    dump_APPS__CE_BANK_ACCOUNTS = PythonOperator(task_id='bankaccounts',
        python_callable=OracleToParquetToS3, 
        op_kwargs={
              "table_name": "APPS.CE_BANK_ACCOUNTS",
              "target_bucket": "bigdata-prod-dyttnieq",
              "file_key":"RAW_Data_Lake/EBS_12_2_5/CE/CE_BANK_ACCOUNTS.parquet"
        },
        executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    node_selector={
                        "node-group": "master"
                    },
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    )
    
    dump_APPS__CE_BANK_BRANCHES_V = PythonOperator(task_id='dump__APPS_CE_BANK_BRANCHES_V',
        python_callable=OracleToParquetToS3, 
        op_kwargs={
              "table_name": "APPS.CE_BANK_BRANCHES_V",
              "target_bucket": "bigdata-prod-dyttnieq",
              "file_key":"RAW_Data_Lake/EBS_12_2_5/CE/CE_BANK_BRANCHES_V.parquet"
        },
        executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    node_selector={
                        "node-group": "master"
                    },
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    )
    dump_APPS__GL_SETS_OF_BOOKS = PythonOperator(task_id='dumm__apps_GL_SETS_OF_BOOKS',
          python_callable=OracleToParquetToS3,
          op_kwargs={
              "table_name": "APPS.GL_SETS_OF_BOOKS",
              "target_bucket": "bigdata-prod-dyttnieq",
              "file_key":"RAW_Data_Lake/EBS_12_2_5/GL/GL_SETS_OF_BOOKS.parquet"
          },
          executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    node_selector={
                        "node-group": "master"
                    },
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    
    
    
    
    
    
    
    )
    dump_APPS__HR_OPERATING_UNITS = PythonOperator(task_id='dumm__apps_HR_OPERATING_UNITS',
          python_callable=OracleToParquetToS3,
          op_kwargs={
              "table_name": "APPS.HR_OPERATING_UNITS",
              "target_bucket": "bigdata-prod-dyttnieq",
              "file_key":"RAW_Data_Lake/EBS_12_2_5/GL/HR_OPERATING_UNITS.parquet"
          },
          executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    node_selector={
                        "node-group": "master"
                    },
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    
    
    
    
    
    
    
    )
    dump_APPS__mtl_categories_b_kfv = PythonOperator(task_id='dumm__apps_mtl_categories_b_kfv',
          python_callable=OracleToParquetToS3,
          op_kwargs={
              "table_name": "APPS.mtl_categories_b_kfv",
              "target_bucket": "bigdata-prod-dyttnieq",
              "file_key":"RAW_Data_Lake/EBS_12_2_5/INV/HR_OPERATING_UNITS.parquet"
          },
          executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    node_selector={
                        "node-group": "master"
                    },
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    
    
    
    
    
    
    
    )
    dump_APPS__mtl_categories_v = PythonOperator(task_id='dumm__apps_mtl_categories_v',
          python_callable=OracleToParquetToS3,
          op_kwargs={
              "table_name": "APPS.mtl_categories_v",
              "target_bucket": "bigdata-prod-dyttnieq",
              "file_key":"RAW_Data_Lake/EBS_12_2_5/INV/mtl_categories_v.parquet"
          },
          executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    node_selector={
                        "node-group": "master"
                    },
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    
    
    
    
    
    
    
    )
    dump_APPS__mtl_category_sets_tl = PythonOperator(task_id='dumm__apps_mtl_category_sets_tl',
          python_callable=OracleToParquetToS3,
          op_kwargs={
              "table_name": "APPS.mtl_categories_sets_tl",
              "target_bucket": "bigdata-prod-dyttnieq",
              "file_key":"RAW_Data_Lake/EBS_12_2_5/INV/mtl_category_sets_tl.parquet"
          },
          executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    node_selector={
                        "node-group": "master"
                    },
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    
    
    
    
    
    
    
    )
    dump_APPS__mtl_item_locations_kfv = PythonOperator(task_id='dumm__apps_mtl_item_locations_kfv',
          python_callable=OracleToParquetToS3,
          op_kwargs={
              "table_name": "APPS.mtl_item_locations_kfv",
              "target_bucket": "bigdata-prod-dyttnieq",
              "file_key":"RAW_Data_Lake/EBS_12_2_5/INV/mtl_item_locations_kfv.parquet"
          },
          executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    node_selector={
                        "node-group": "master"
                    },
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    
    
    
    
    
    
    
    )
    dump_APPS__MTL_ONHAND_QUANTITIES = PythonOperator(task_id='dumm__apps_MTL_ONHAND_QUANTITIES',
          python_callable=OracleToParquetToS3,
          op_kwargs={
              "table_name": "APPS.MTL_ONHAND_QUANTITIES",
              "target_bucket": "bigdata-prod-dyttnieq",
              "file_key":"RAW_Data_Lake/EBS_12_2_5/INV/MTL_ONHAND_QUANTITIES.parquet"
          },
          executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    node_selector={
                        "node-group": "master"
                    },
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    
    
    
    
    
    
    
    )
    dump_APPS__OE_ORDER_CREDIT_CHECK_RULES = PythonOperator(task_id='dumm__apps_OE_ORDER_CREDIT_CHECK_RULES',
          python_callable=OracleToParquetToS3,
          op_kwargs={
              "table_name": "APPS.OE_ORDER_CREDIT_CHECK_RULES",
              "target_bucket": "bigdata-prod-dyttnieq",
              "file_key":"RAW_Data_Lake/EBS_12_2_5/OE/OE_ORDER_CREDIT_CHECK_RULES.parquet"
          },
          executor_config={
            "pod_template_file": os.path.join(AIRFLOW_HOME, "kubernetes/pod_templates/default_template.yaml"),
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    node_selector={
                        "node-group": "master"
                    },
                    containers=[
                        k8s.V1Container(
                            name="base",
                        ),
                    ],
                )
            ),
        },
    
    
    start >> [dump_APPS__AP_LOOKUP_CODES,dump_APPS__AP_TERMS_VAL_V,dump_APPS__CE_BANK_ACCOUNTS,dump_APPS__CE_BANK_BRANCHES_V, dump_APPS__GL_SETS_OF_BOOKS,  dump_APPS__HR_OPERATING_UNITS, dump_APPS__mtl_categories_b_kfv,dump_APPS__MTL_ONHAND_QUANTITIES,dump_APPS__OE_ORDER_CREDIT_CHECK_RULES]
    
    [dump_APPS__AP_LOOKUP_CODES,dump_APPS__AP_TERMS_VAL_V,dump_APPS__CE_BANK_ACCOUNTS,dump_APPS__CE_BANK_BRANCHES_V, dump_APPS__GL_SETS_OF_BOOKS,  dump_APPS__HR_OPERATING_UNITS, dump_APPS__mtl_categories_b_kfv,dump_APPS__MTL_ONHAND_QUANTITIES,dump_APPS__OE_ORDER_CREDIT_CHECK_RULES] >> finish
