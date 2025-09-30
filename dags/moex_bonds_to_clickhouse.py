from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

import requests
import pandas as pd
import logging
import clickhouse_connect

logger = logging.getLogger(__name__)

def extract_bonds():
    try:
        logger.info('Извлечение данных с MOEX')

        url = 'https://iss.moex.com/iss/engines/stock/markets/bonds/securities.json'

        params = {
            'iss.meta': 'off'
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        logger.info(f'HTTP статус: {response.status_code}')

        data = response.json()

        df_securities = pd.DataFrame(data['securities']['data'],
                                     columns=data['securities']['columns'])
        df_market = pd.DataFrame(data['marketdata']['data'],
                                 columns=data['marketdata']['columns'])

        df = pd.merge(df_securities, df_market, on='SECID')

        logger.info('Извлечение данных прошло успешно')

        return df


    except Exception as e:
        logger.error(f'Не удалось извлечь данные, ошибка: {e}')
        raise

def transform_bonds(**context):
    logger.info('Преобразование данных')
    ti = context['ti']
    df_trans = ti.xcom_pull(task_ids='extract_bonds')

    df_trans = df_trans.reindex(columns=['SECID', 'BOARDID_x', 'PREVPRICE',
                                         'SHORTNAME', 'SECNAME',
                                         'LAST', 'YIELD',
                                         'MATDATE', 'OPEN', 'HIGH', 'LOW'])

    numeric_columns = ['LAST', 'YIELD', 'PREVPRICE', 'OPEN', 'HIGH', 'LOW']

    date_columns = ['MATDATE']

    mask = df_trans['MATDATE'] != '0000-00-00'
    df_trans = df_trans[mask]

    for col in numeric_columns:
        if not pd.api.types.is_numeric_dtype(df_trans[col]):
            df_trans[col] = pd.to_numeric(df_trans[col], format='mixed', errors='coerce')

    for col in date_columns:
        df_trans[col] = pd.to_datetime(df_trans[col])

    logger.info('Конец преобразования данных')

    return df_trans

def load_bonds(**context):
    try:
        ti = context['ti']
        df = ti.xcom_pull(task_ids='transform_bonds')


        logger.info('Загрузка данных в Clickhouse')

        if df is None:
            logger.error('Нет данных для загрузки')
            return 1

        connection = {
            'host': 'localhost',
            'port': 8123,
            'user': 'admin',
            'password': 'password',
            'database': 'analytics'
        }

        client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='admin',
                                               password='password', database='analytics')

        client.command('TRUNCATE TABLE analytics.moex_bonds')

        client.insert_df(df = df, table = 'moex_bonds', database='analytics')

        logger.info('Загрузка завершена')
        return 0

    except Exception as e:
        logger.error(f'Ошибка при загрузке данных: {e}')

with DAG(
        'etl_moex_bonds_dag',
        start_date=datetime(2024, 1, 1),
        schedule=timedelta(hours=1),
) as dag:
    extract_task = PythonOperator(
        task_id='extract_bonds',
        python_callable=extract_bonds,
    )
    transform_task = PythonOperator(
        task_id='transform_bonds',
        python_callable=transform_bonds,
    )
    load_task = PythonOperator(
        task_id='load_bonds',
        python_callable=load_bonds,
    )
extract_task >> transform_task >> load_task