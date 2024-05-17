import json
import os
import sys

import pika
import toml

# from dotenv import load_dotenv
# '172.21.0.2'
# load_dotenv('../envs/.env.rabbitmq_dns')
# load_dotenv('../envs/.env.rabbitmq_user_log_pass')

RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_DNS = os.environ['RABBITMQ_DNS']

dir_config = 'configs'

CONNECTION_RABBITMQ = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_DNS,
                                                                        5672,
                                                                        '/',
                                                                        pika.PlainCredentials(RABBITMQ_USERNAME,
                                                                                              RABBITMQ_PASSWORD)))
CHANNEL_RABBITMQ = CONNECTION_RABBITMQ.channel()

ERROR_MESSAGE = 'Не удалось'


def send_tg_message(message):
    CHANNEL_RABBITMQ.basic_publish(exchange='', routing_key='messages', body=message)


def abc_config_post(body):
    try:
        with open(f'{dir_config}//ABC_config.toml', 'r') as f:
            config = toml.load(f)

        for old_parameter in body['data'].keys():

            parameter = 'ABC_' + old_parameter.replace(
                " ", "_").replace(
                "Ебитда", "EBITDA").replace(
                "Оборачиваемость", "TURNOVER").replace(
                '+', 'PLUS')

            value = body['data'][old_parameter]
            if "EBITDA" in parameter:
                config["EBITDA"][parameter] = value
            elif "TURNOVER" in parameter:
                config["TURNOVER"][parameter] = value

        with open(f'{dir_config}//ABC_config.toml', 'w', encoding='utf-8') as f:
            toml.dump(config, f)

        send_tg_message('Конфигурация изменена')

    except Exception as e:
        send_tg_message(f'{ERROR_MESSAGE} изменить конфигурацию\n{e}')


def abc_config_get_config(ch, properties):
    try:
        with open(f'{dir_config}//ABC_config.toml', 'r') as f:
            config = toml.load(f)

        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                         body=json.dumps(config, indent=4).encode('utf-8'))

    except Exception as e:
        send_tg_message(f'{ERROR_MESSAGE} получить конфигурацию\n{e}')


def abc_config_get_config_message(ch, properties):
    try:
        with open(f'{dir_config}//ABC_config.toml', 'r') as f:
            config = toml.load(f)
        data_str = []
        for k1, name in zip(config.keys(), ('Ебитда', 'Оборачиваемость')):
            t_config = config[k1]
            data_str.append(name)
            for k in t_config.keys():
                data_str.append(f'{k} = {t_config[k]}')
        message_config = '\n'.join(data_str).replace(
            "ABC_", " " * 5).replace(
            "EBITDA", "Ебитда").replace(
            "TURNOVER", "Оборачиваемость").replace(
            "_", " ").replace(
            "PLUS", "+").strip()
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                         body=message_config.encode('utf-8'))

    except Exception as e:
        send_tg_message(f'{ERROR_MESSAGE} отобразить конфигурацию')


def main():
    queue_name = 'queue_config'
    CHANNEL_RABBITMQ.queue_declare(queue=queue_name, durable=True)

    def callback_messages(ch, method, properties, body):
        try:
            body = json.loads(body)
        except Exception as e:
            send_tg_message('Проблема с json файлом')

        if body.get('config') == 'ABC_config':
            if body.get('method') == 'POST':
                abc_config_post(body)

            elif body.get('method') == 'GET_config':
                abc_config_get_config(ch, properties)

            elif body.get('method') == 'GET_message':
                abc_config_get_config_message(ch, properties)

            else:
                send_tg_message('Команда не найдена')

        else:
            send_tg_message('Конфигурация не найдена')

        ch.basic_ack(delivery_tag=method.delivery_tag)

    CHANNEL_RABBITMQ.basic_qos(prefetch_count=1)
    CHANNEL_RABBITMQ.basic_consume(queue=queue_name, on_message_callback=callback_messages)

    print('config server running')

    CHANNEL_RABBITMQ.start_consuming()


if __name__ == '__main__':
    try:
        main()
        CONNECTION_RABBITMQ.close()
    except KeyboardInterrupt:
        print('config server interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
