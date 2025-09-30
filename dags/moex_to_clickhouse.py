from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

import requests
import pandas as pd
import logging
import clickhouse_connect


logger = logging.getLogger(__name__)

def extract():
    try:
        logger.info('Извлечение данных с MOEX')

        url = 'https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json'

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

def transform(**context):
    logger.info('Преобразование данных')
    ti = context['ti']
    df_trans = ti.xcom_pull(task_ids='extract')

    df_trans = df_trans.reindex(columns=['SECID', 'BOARDID_x', 'PREVPRICE',
                                         'SHORTNAME', 'SECNAME',
                                         'LOTSIZE', 'LAST',
                                         'OPEN', 'LOW', 'HIGH'])

    numeric_columns = ['PREVPRICE', 'LOTSIZE', 'LAST', 'OPEN', 'LOW', 'HIGH']

    for col in numeric_columns:
        if pd.api.types.is_numeric_dtype(df_trans[col]) == False:
            df_trans.astype(float)

    df_trans = df_trans.dropna()

    logger.info('Конец преобразования данных')

    return df_trans

def load(**context):
    try:
        logger.info('Преобразование данных')
        ti = context['ti']
        df = ti.xcom_pull(task_ids='transform')


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

        client.insert_df(df = df, table = 'moex', database='analytics')

        logger.info('Загрузка завершена')
        return 0

    except Exception as e:
        logger.error(f'Ошибка при загрузке данных: {e}')

with DAG(
        'etl_moex_dag',
        start_date=datetime(2024, 1, 1),
        schedule=timedelta(hours=1),
) as dag:
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
extract_task >> transform_task >> load_task