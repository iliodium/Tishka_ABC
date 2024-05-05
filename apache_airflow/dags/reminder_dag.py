import asyncio
import os
from datetime import datetime

import gspread
import pika
from airflow.models import DAG
from airflow.operators.python import PythonOperator

KEY_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'abc-dev-415713-a2a407ac569b.json')

KEY = os.environ['GOOGLESHEET_KEY']

url_to_price_sheet = f'https://docs.google.com/spreadsheets/d/{KEY}'

GC = gspread.service_account(KEY_PATH)
FILE = GC.open_by_key(KEY)

RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_DNS = os.environ['RABBITMQ_DNS']


def get_message():
    sheet = FILE.worksheet("Загрузка цены")
    flag = True if sheet.cell(1, 2).value == 'Нет' else False
    message = 'Решение по цене принято?'
    if flag:
        return message


def send_message_to_queue(message):
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(RABBITMQ_DNS,
                                           5672,
                                           '/',
                                           credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.basic_publish(exchange='', routing_key='messages', body=message)

    connection.close()


async def main():
    message = get_message()
    if message:
        send_message_to_queue(message)


def start_dag():
    asyncio.run(main())


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 5),
}

with DAG(dag_id='reminder_dag',
         schedule='0 * * * *',
         default_args=default_args) as dag:
    dice = PythonOperator(
        task_id='reminder',
        python_callable=start_dag,
        dag=dag)

if __name__ == '__main__':
    start_dag()
