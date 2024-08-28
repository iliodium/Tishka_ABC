import asyncio
import os
import json

from datetime import datetime

import gspread
import pika
from airflow.models import DAG
from airflow.operators.python import PythonOperator

KEY_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'abc-dev-415713-a2a407ac569b.json')

KEY = None
url_to_price_sheet = None
FILE = None

GC = gspread.service_account(KEY_PATH)

RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_DNS = os.environ['RABBITMQ_DNS']


def get_message():
    sheet = FILE.worksheet("Загрузка цены")
    return True if sheet.cell(1, 2).value == 'Да' and sheet.cell(1, 4).value == 'Нет' else False


def change_price(shop_name):
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(RABBITMQ_DNS,
                                           5672,
                                           '/',
                                           credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    body = {
        'current_task':'change_price',
        'seller':shop_name
        }
    channel.basic_publish(exchange='', routing_key='services', body=json.dumps(body, indent=4).encode('utf-8'))
    channel.basic_publish(exchange='', routing_key='messages', body=f'<b>{shop_name}</b> Цены изменены')

    connection.close()


async def main(shop_name):
    if get_message():
        change_price(shop_name)


def start_dag(shop_name):
    asyncio.run(main(shop_name))


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 27),
}

with DAG(dag_id='auto_price_change_Tishka',
         schedule='30 20 * * *',
         tags=['Tishka'],
         default_args=default_args) as dag:
    
    KEY = os.environ['GOOGLESHEET_KEY']
    FILE = GC.open_by_key(KEY)

    shop_name = 'Tishka'
    url_to_price_sheet = f'https://docs.google.com/spreadsheets/d/{KEY}'

    dice = PythonOperator(
        task_id='auto_price_change_Tishka',
        python_callable=start_dag,
        op_kwargs={
        'shop_name':shop_name,
        },
        dag=dag)
    
with DAG(dag_id='auto_price_change_Future_Milf',
         schedule='30 20 * * *',
         tags=['Future_milf'],
         default_args=default_args) as dag:
    
    KEY = os.environ['GOOGLESHEET_KEY_FUTURE_MILF']
    FILE = GC.open_by_key(KEY)

    shop_name = 'Future Milf'
    url_to_price_sheet = f'https://docs.google.com/spreadsheets/d/{KEY}'

    dice = PythonOperator(
        task_id='auto_price_change_FUTURE_MILF',
        python_callable=start_dag,
        op_kwargs={
        'shop_name':shop_name,
        },
        dag=dag)

if __name__ == '__main__':
    start_dag()
