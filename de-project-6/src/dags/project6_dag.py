from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.models import Variable
from datetime import datetime
import boto3
import vertica_python
import csv

def fetch_s3_file(bucket: str, key: str):
    AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )

def load_data_to_stg(file_name: str):
    conn_info = {'host': '51.250.75.20',
                'port': 5433,
                'user': Variable.get('USER'),
                'password': Variable.get('PASS'),
                'database': 'dwh',
                'autocommit': False
    }
    fn = file_name[:-4]
    with open(f'/data/{file_name}', 'r') as f:
        reader = csv.reader(f)
        #cols = next(reader)
        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(f"""
                    copy DANILAKOVRIGINYANDEXRU__STAGING.{fn}
                    FROM LOCAL '/data/{file_name}'
                    DELIMITER ',' ENCLOSED BY '"' REJECTMAX 1000
                    REJECTED DATA AS TABLE DANILAKOVRIGINYANDEXRU__STAGING.{fn}_rej;
                    """)

@dag(schedule_interval=None, start_date=datetime(2022, 1, 1))

def project6_dag_get_data():
    task1  = PythonOperator(
        task_id=f'group_log.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'}
    )
    
    load_data_task1  = PythonOperator(
        task_id=f'load_to_stg_group_log.csv',
        python_callable=load_data_to_stg,
        op_kwargs={'file_name': 'group_log.csv'}
    )
        
dag = project6_dag_get_data()