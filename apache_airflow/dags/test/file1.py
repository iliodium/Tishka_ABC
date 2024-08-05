import pika

CONNECTION_RABBITMQ = pika.BlockingConnection(pika.ConnectionParameters('172.21.0.2',
                                                                        5672,
                                                                        '/',
                                                                        pika.PlainCredentials('user_worker',
                                                                                              'JDKNjk423478njfsd')))
CHANNEL_RABBITMQ = CONNECTION_RABBITMQ.channel()


def send_message_to_queue(message):
    CHANNEL_RABBITMQ.basic_publish(exchange='', routing_key='messages', body=message)


def main(message):
    if message == 'error':
        1/0
    send_message_to_queue(message)
    CONNECTION_RABBITMQ.close()
    return {'message': 'это сообщение от 1 таска',
            'data':[i for i in range(100000)],
            }


if __name__ == '__main__':
    main('1')