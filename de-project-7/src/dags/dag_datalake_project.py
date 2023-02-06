import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
        'owner': 'airflow',
        'start_date':datetime(2022, 1, 1)
        }

dag_spark = DAG(
        dag_id = "datalake_project",
        default_args=default_args,
        schedule_interval="@daily"
        )

user_mart = SparkSubmitOperator(
        task_id='user_mart',
        dag=dag_spark,
        application ='/scripts/user_mart.py' ,
        conn_id= 'yarn_spark',
        application_args = ["2022-05-31", "10", "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/danilal146/data/prod"],
        conf={
        "spark.driver.maxResultSize": "20g",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.executorIdleTimeout": "60s",
        "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",    
        "spark.driver.memory": "2g"    
        },
        executor_cores = 2,
        executor_memory = '5g'
        )


geo_zone_mart = SparkSubmitOperator(
        task_id='geo_zone_mart',
        dag=dag_spark,
        application ='/scripts/geo_zone_mart.py' ,
        conn_id= 'yarn_spark',
        application_args = ["2022-05-31", "10", "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/danilal146/data/prod"],
        conf={
        "spark.driver.maxResultSize": "20g",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.executorIdleTimeout": "60s",
        "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",    
        "spark.driver.memory": "2g"    
        },
        executor_cores = 2,
        executor_memory = '5g'
        )

recommendation_mart = SparkSubmitOperator(
        task_id='recommendation_mart',
        dag=dag_spark,
        application ='/scripts/recommendation_mart.py' ,
        conn_id= 'yarn_spark',
        application_args = ["2022-05-31", "10", "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/danilal146/data/prod"],        
        conf={
        "spark.driver.maxResultSize": "20g",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.executorIdleTimeout": "60s",
        "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",    
        "spark.driver.memory": "2g"    
        },
        executor_cores = 2,
        executor_memory = '5g'
        )

user_mart >> geo_zone_mart >> recommendation_mart