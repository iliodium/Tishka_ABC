import asyncio
import os
import sys
import time

import aio_pika
import aio_pika.abc
import pika
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters.command import Command

import keyboards as kb

TOKEN = os.environ['TG_BOT_TOKEN']
ID_CHAT = os.environ['TG_BOT_ID_CHAT']
RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_DNS = os.environ['RABBITMQ_DNS']

RABBITMQ_URL = f"amqp://{RABBITMQ_USERNAME}:{RABBITMQ_PASSWORD}@{RABBITMQ_DNS}/"

bot = Bot(TOKEN)
dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer('Выберите действие', reply_markup=kb.keyboard)


@dp.message(Command("healthcheck"))
async def cmd_start(message: types.Message):
    await message.answer('Я работаю')


def send_message_to_queue(message):
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(RABBITMQ_DNS,
                                           5672,
                                           '/',
                                           credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.basic_publish(exchange='', routing_key='services', body=message)

    connection.close()


@dp.message(F.text == "Подтвердить цены")
@dp.message(F.text == "Поменять цены сейчас")
async def with_puree(message: types.Message):
    text = message.text
    if text == "Подтвердить цены":
        current_task = 'accept'
        answer = 'Хорошо, цена будет загружена сегодня в 23:30'

    else:
        current_task = 'change'
        answer = 'Изменяю цену'

    send_message_to_queue(current_task)
    await message.answer(answer)


async def send_message(message):
    await bot.send_message(ID_CHAT, message)


async def run_tg_bot():
    t = asyncio.create_task(dp.start_polling(bot))
    await t


async def run_rabbitmq(loop):
    # Connecting with the given parameters is also possible.
    # aio_pika.connect_robust(host="host", login="login", password="password")
    # You can only choose one option to create a connection, url or kw-based params.
    connection = await aio_pika.connect_robust(RABBITMQ_URL, loop=loop)

    async with connection:
        queue_name = "messages"

        # Creating channel
        channel: aio_pika.abc.AbstractChannel = await connection.channel()

        # Declaring queue
        queue: aio_pika.abc.AbstractQueue = await channel.declare_queue(queue_name, durable=True)

        async with queue.iterator() as queue_iter:
            # Cancel consuming after __aexit__
            async for message in queue_iter:
                async with message.process():
                    await send_message(message.body)

                    if queue.name in message.body.decode():
                        break


async def start(loop):
    await asyncio.gather(run_tg_bot(), run_rabbitmq(loop))


if __name__ == '__main__':
    while True:
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(start(loop))
            loop.close()
        except KeyboardInterrupt:
            print('interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
        except Exception as e:
            time.sleep(3)
            print(e)
