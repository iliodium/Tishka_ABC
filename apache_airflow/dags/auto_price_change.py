import asyncio
import os
from datetime import datetime

import gspread
import pika
from airflow.models import DAG
from airflow.operators.python import PythonOperator

KEY_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'abc-dev-415713-a2a407ac569b.json')

KEY = os.environ['GOOGLESHEET_KEY']

GC = gspread.service_account(KEY_PATH)
FILE = GC.open_by_key(KEY)

RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_DNS = os.environ['RABBITMQ_DNS']


def get_message():
    sheet = FILE.worksheet("Загрузка цены")
    return True if sheet.cell(1, 2).value == 'Да' and sheet.cell(1, 4).value == 'Нет' else False


def change_price():
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(RABBITMQ_DNS,
                                           5672,
                                           '/',
                                           credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.basic_publish(exchange='', routing_key='services', body='change')

    connection.close()


async def main():
    if get_message():
        change_price()


def start_dag():
    asyncio.run(main())


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 3),
}

with DAG(dag_id='auto_price_change',
         schedule='30 20 * * *',
         default_args=default_args) as dag:
    dice = PythonOperator(
        task_id='auto_price_change',
        python_callable=start_dag,
        dag=dag)

if __name__ == '__main__':
    start_dag()
