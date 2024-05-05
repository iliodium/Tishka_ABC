import os
import sys

import pika
import requests

RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_DNS = os.environ['RABBITMQ_DNS']

APACHE_WEBSERVER_IP_ADDRESS = os.environ['APACHE_WEBSERVER_IP_ADDRESS']
APACHE_WEBSERVER_EXTERNAL_PORT = os.environ['APACHE_WEBSERVER_EXTERNAL_PORT']

APACHE_AIRFLOW_USER_USERNAME = os.environ['APACHE_AIRFLOW_USER_USERNAME']
APACHE_AIRFLOW_USER_PASSWORD = os.environ['APACHE_AIRFLOW_USER_PASSWORD']

url_manual_trigger_dag = 'http://{}:{}/api/v1/dags/{}/dagRuns'


def manual_trigger_dag(dag_id: str):
    url = url_manual_trigger_dag.format(APACHE_WEBSERVER_IP_ADDRESS, APACHE_WEBSERVER_EXTERNAL_PORT, dag_id)
    result = requests.post(url, json={}, auth=(APACHE_AIRFLOW_USER_USERNAME, APACHE_AIRFLOW_USER_PASSWORD))

    if result.status_code != 200:
        raise Exception


def main():
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(RABBITMQ_DNS,
                                           5672,
                                           '/',
                                           credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue='services', durable=True)

    error_message = 'Не удалось'

    def callback_messages(ch, method, properties, body):
        if body == b'accept':
            try:
                manual_trigger_dag('accept_price')
                channel.basic_publish(exchange='', routing_key='messages', body='Цены подтверждены')
            except Exception as e:
                channel.basic_publish(exchange='', routing_key='messages', body=f'{error_message} подтвердить')

        elif body == b'change':
            try:
                manual_trigger_dag('change_price')
                channel.basic_publish(exchange='', routing_key='messages', body='Цены изменены')
            except Exception as e:
                channel.basic_publish(exchange='', routing_key='messages', body=f'{error_message} изменить цену')

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='services', on_message_callback=callback_messages)

    print('services server running')

    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('services server interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
