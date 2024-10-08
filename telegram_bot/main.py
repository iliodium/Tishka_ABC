import asyncio
import json
import os
import sys
import time
import uuid

import aio_pika
import aio_pika.abc
import pika
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters.command import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

import keyboards as kb

# from dotenv import load_dotenv

# load_dotenv()
# load_dotenv('../envs/.env.rabbitmq_dns')
# load_dotenv('../envs/.env.rabbitmq_user_log_pass')

TOKEN = os.environ['TG_BOT_TOKEN']
ID_CHAT = os.environ['TG_BOT_ID_CHAT']
RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_DNS = os.environ['RABBITMQ_DNS']
# RABBITMQ_DNS = '172.21.0.2'
RABBITMQ_URL = f"amqp://{RABBITMQ_USERNAME}:{RABBITMQ_PASSWORD}@{RABBITMQ_DNS}/"

bot = Bot(TOKEN)
dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer('Выберите действие', reply_markup=kb.keyboard)


@dp.message(Command("healthcheck"))
async def cmd_healthcheck(message: types.Message):
    await message.answer('Я работаю')


class ConfigForm(StatesGroup):
    PARAMETER = State()

class SellerForm(StatesGroup):
    SELLER = State()

SELLER = None



def send_message_to_queue(body, routing_key='services'):
    CONNECTION_RABBITMQ = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_DNS,
                                                                            5672,
                                                                            '/',
                                                                            pika.PlainCredentials(
                                                                                RABBITMQ_USERNAME,
                                                                                RABBITMQ_PASSWORD)))
    CHANNEL_RABBITMQ = CONNECTION_RABBITMQ.channel()
    CHANNEL_RABBITMQ.basic_publish(exchange='', routing_key=routing_key, body=json.dumps(body, indent=4).encode('utf-8'))
    CONNECTION_RABBITMQ.close()


def get_config_abc_message():
    CONNECTION_RABBITMQ = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_DNS,
                                                                            5672,
                                                                            '/',
                                                                            pika.PlainCredentials(
                                                                                RABBITMQ_USERNAME,
                                                                                RABBITMQ_PASSWORD)))
    CHANNEL_RABBITMQ = CONNECTION_RABBITMQ.channel()
    response = None
    corr_id = str(uuid.uuid4())

    def on_response(ch, method, props, body):
        nonlocal response
        if corr_id == props.correlation_id:
            response = body

    result = CHANNEL_RABBITMQ.queue_declare(queue='', exclusive=True)
    callback_queue = result.method.queue
    CHANNEL_RABBITMQ.basic_consume(
        queue=callback_queue,
        on_message_callback=on_response,
        auto_ack=True)

    b = {
        'config': 'ABC_config',
        'method': "GET_message",
        'data': {
        }
    }
    queue_name = 'queue_config'

    CHANNEL_RABBITMQ.basic_publish(exchange='', routing_key=queue_name, properties=pika.BasicProperties(
        reply_to=callback_queue,
        correlation_id=corr_id,
    ), body=json.dumps(b, indent=4).encode('utf-8'))

    while response is None:
        CONNECTION_RABBITMQ.process_data_events(time_limit=5)
    CONNECTION_RABBITMQ.close()

    return response


@dp.message(F.text == 'Назад')
async def step_back(message: types.Message):
    await message.answer('Выберите действие', reply_markup=kb.keyboard)


@dp.message(F.text == "Изменить конфигурацию")
async def handler(message: types.Message):
    await message.answer("Выберите параметр для изменения", reply_markup=kb.config_keybord)

@dp.message(lambda message: message.text in kb.all_config_words)
async def set_parameter(message: types.Message, state: FSMContext):
    await state.update_data(PARAMETER=message.text)
    await message.answer(text="Введите новое значение")
    await state.set_state(ConfigForm.PARAMETER)


@dp.message(ConfigForm.PARAMETER)
async def set_new_value(message: types.Message, state: FSMContext):
    data = await state.get_data()
    try:
        b = {
            'config': 'ABC_config',
            'method': "POST",
            'data': {
                data['PARAMETER']: int(message.text)
            }
        }

        send_message_to_queue(json.dumps(b, indent=4).encode('utf-8'), 'queue_config')
        await message.answer(text=f"{data['PARAMETER']} = {message.text}")
    except ValueError:
        await message.answer(text=f"Неверное значение\nValueError")
    finally:
        await state.clear()

@dp.message(F.text == "Отобразить конфигурацию")
async def user_action(message: types.Message):
    await message.answer(get_config_abc_message())

@dp.message(F.text == "Tishka")
@dp.message(F.text == "Future Milf")
async def seller_choice(message: types.Message, state: FSMContext):
    await state.update_data(SELLER=message.text)
    await message.answer('Выберите действие', reply_markup=kb.price_keyboard)
    await state.set_state(SellerForm.SELLER)



@dp.message(SellerForm.SELLER)
async def price_action_choice(message: types.Message, state: FSMContext):
    text = message.text
    data = await state.get_data()
    seller = data['SELLER']

    if text == "Подтвердить цены":
        current_task = 'accept_price'
        answer = f'<b>{seller}</b> Хорошо, цена будет загружена сегодня в 23:30'
    else:
        current_task = 'change_price'
        answer = f'<b>{seller}</b> Изменяю цену'

    body = {
        'current_task':current_task,
        'seller':seller
        }

    await message.answer(answer, reply_markup=kb.keyboard, parse_mode="HTML")
    send_message_to_queue(body)
    await state.clear()



# from aiogram.types import InputFile
# from aiogram.types import FSInputFile


async def send_message(message,reply_markup=None):
    await bot.send_message(ID_CHAT, message, parse_mode="HTML", reply_markup=reply_markup)
    # with open('../audio_2024-05-21_22-36-53.ogg', 'rb') as audio_file:
    # audio_input = FSInputFile('../audio_2024-05-21_22-36-53.ogg')
    # await bot.send_voice(ID_CHAT, audio_input)


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
                    body = message.body
                    try:
                        text = body.decode()
                        await send_message(text)
                    except Exception as e:
                        print(e)

                    if queue.name in body.decode():
                        break


async def run_tg_bot():
    t = asyncio.create_task(dp.start_polling(bot))
    await t


async def start(loop):
    await asyncio.gather(run_tg_bot(), run_rabbitmq(loop))


if __name__ == '__main__':
    while True:      
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
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
