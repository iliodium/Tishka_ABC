import asyncio
import os
from datetime import datetime

import gspread
import pika
from airflow.models import DAG
from airflow.operators.python import PythonOperator

KEY_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'abc-dev-415713-a2a407ac569b.json')

url_to_price_sheet = None
FILE = None

GC = gspread.service_account(KEY_PATH)


RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_DNS = os.environ['RABBITMQ_DNS']


def get_message(shop_name):
    sheet = FILE.worksheet("Загрузка цены")
    if sheet.cell(1, 2).value == 'Нет':
        return f'<b>{shop_name}</b> Решение по цене принято?'


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


async def main(shop_name):
    message = get_message(shop_name)
    if message:
        send_message_to_queue(message)


def start_dag(shop_name):
    global FILE, url_to_price_sheet
    if shop_name == 'Tishka':
        KEY = os.environ['GOOGLESHEET_KEY']
    elif shop_name == 'Future milf':
        KEY = os.environ['GOOGLESHEET_KEY_FUTURE_MILF']
    FILE = GC.open_by_key(KEY)
    url_to_price_sheet = f'https://docs.google.com/spreadsheets/d/{KEY}'

    asyncio.run(main(shop_name))


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 27),
}

with DAG(dag_id='reminder_about_accept_price_Tishka',
         schedule='0 * * * *',
         tags=['Tishka'],
         default_args=default_args) as dag:
    
    shop_name = 'Tishka'

    dice = PythonOperator(
        task_id='reminder_about_accept_price_Tishka',
        python_callable=start_dag,
        op_kwargs={
        'shop_name':shop_name,
        },
        dag=dag)


with DAG(dag_id='reminder_about_accept_price_Future_Milf',
         schedule='0 * * * *',
         tags=['Future_milf'],
         default_args=default_args) as dag:
    
    shop_name = 'Future milf'

    dice = PythonOperator(
        task_id='reminder_about_accept_price_Future_Milf',
        python_callable=start_dag,
        op_kwargs={
        'shop_name':shop_name,
        },
        dag=dag)
    
if __name__ == '__main__':
    start_dag()
