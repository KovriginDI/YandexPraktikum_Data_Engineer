import requests
import pandas as pd
import pendulum
import os
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

dag = DAG(
	"project4_dag", 
        schedule_interval='0/5 * * * *',
        start_date=pendulum.datetime(2022, 8, 25, tz="UTC"),
        catchup=False,
        tags=['project4', 'example'],
        is_paused_upon_creation=False
        )

postgres_conn_id = 'PG_WAREHOUSE_CONNECTION'

url = Variable.get('API_URL')
url_rest = f"{url}/restaurants" 
url_couriers = f"{url}/couriers"
url_deliveries = f"{url}/deliveries"

def api(url, sort_direction, sort_field, offset):
    payload = {"sort_field": f"{sort_field}", "sort_direction": f"{sort_direction}", "limit": "50", "offset": f"{offset}"}
    headers = {'X-Nickname': Variable.get('X-NICKNAME'), 'X-Cohort': Variable.get('X-COHORT'), 'X-API-KEY': Variable.get('X-API-KEY')}
    r=requests.get(url=url, params=payload, headers=headers)
    df = pd.DataFrame(r.json())
    return df
    
def load_stg(url, sort_direction, sort_field, table):
    last_id = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_pandas_df(f'select id from {table};')

    N = 10000
    out = pd.DataFrame()
    for i in range(0, N):
        df = api(url, sort_direction, sort_field, i)
        out = pd.concat([out, df])
        if len(df) < 50:
            break
    df = out.drop_duplicates()
    df = df.rename(columns={'_id':'id'})
    last_id_list = list(last_id['id'])
    df = df.query('id not in @last_id_list')
    PostgresHook(postgres_conn_id = 'PG_WAREHOUSE_CONNECTION').insert_rows(table, df.values, target_fields=df.columns.tolist())

def load_stg_deliveries(url, sort_direction, sort_field, table):
    max_offset_prev = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_first(f'''
    select coalesce(max(max_offset), 0) as max_ts_prev from stg.offset_settings
    where src_name = '{table}';
    ''')
    max_offset_prev = max_offset_prev[0]

    last_id = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_pandas_df(f'select order_id from {table};')
    
    N = 100000
    out = pd.DataFrame()
    for i in range(max_offset_prev, N, 50):
        df = api(url, sort_direction, sort_field, i)
        out = pd.concat([out, df])
        if len(df) < 50:
            break
    df = out.drop_duplicates(subset='order_id')

    last_id_list = list(last_id['order_id'])
    df = df.query('order_id not in @last_id_list')
    PostgresHook(postgres_conn_id = 'PG_WAREHOUSE_CONNECTION').insert_rows(table, df.values, target_fields=df.columns.tolist())

    dict1 = {'src_name': table, 'max_offset': i, 'load_dttm': str(datetime.datetime.now())}
    df1 = pd.DataFrame.from_dict(dict1, orient='index').T
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.offset_settings', df1.values, target_fields=df1.columns.tolist())

sql_dds_rest = open(f'{CUR_DIR}/sql/sql_dds_rest.sql').read()

sql_dds_couriers = open(f'{CUR_DIR}/sql/sql_dds_couriers.sql').read()

sql_dds_orders = open(f'{CUR_DIR}/sql/sql_dds_orders.sql').read()

sql_dds_deliveries = open(f'{CUR_DIR}/sql/sql_dds_deliveries.sql').read()

sql_mart = open(f'{CUR_DIR}/sql/sql_mart.sql').read()

def load_to_dds(sql):
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(sql)

def load_mart(sql):
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run('truncate table cdm.dm_courier_ledger;')
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(sql)

load_rest = PythonOperator(task_id='load_rest',
                           python_callable=load_stg,
                           op_kwargs={'url': url_rest, 'sort_direction': "asc", 'sort_field': "name", 'table':'stg.dm_restaurants'},
                            dag=dag)

load_couriers = PythonOperator(task_id='load_couriers',
                           python_callable=load_stg,
                           op_kwargs={'url': url_couriers, 'sort_direction': "asc", 'sort_field': "name", 'table':'stg.dm_couriers'},
                            dag=dag)

load_deliveries = PythonOperator(task_id='load_deliveries',
                           python_callable=load_stg_deliveries,
                           op_kwargs={'url': url_deliveries, 'sort_direction': "asc", 'sort_field': "order_ts", 'table':'stg.dm_deliveries'},
                            dag=dag)

load_dds_rest = PythonOperator(task_id='load_dds_rest',
                           python_callable=load_to_dds,
                           op_kwargs={'sql': sql_dds_rest},
                            dag=dag)

load_dds_couriers = PythonOperator(task_id='load_dds_couriers',
                           python_callable=load_to_dds,
                           op_kwargs={'sql': sql_dds_couriers},
                            dag=dag)

load_dds_orders = PythonOperator(task_id='load_dds_orders',
                           python_callable=load_to_dds,
                           op_kwargs={'sql': sql_dds_orders},
                            dag=dag)

load_dds_deliveries = PythonOperator(task_id='load_dds_deliveries',
                           python_callable=load_to_dds,
                           op_kwargs={'sql': sql_dds_deliveries},
                            dag=dag)

load_cdm_mart = PythonOperator(task_id='load_cdm_mart',
                           python_callable=load_mart,
                           op_kwargs={'sql': sql_mart},
                            dag=dag)

load_rest >> load_couriers >> load_deliveries >> load_dds_rest >> load_dds_couriers >> load_dds_orders >> load_dds_deliveries >> load_cdm_mart
