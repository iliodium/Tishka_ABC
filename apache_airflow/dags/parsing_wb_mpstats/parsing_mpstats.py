import os
import time
from datetime import timedelta, datetime, date
import gspread
import pika
import requests

KEY_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), './../abc-dev-415713-a2a407ac569b.json')

GC = gspread.service_account(KEY_PATH)

KEY = None
url_to_price_sheet = None
FILE = None
token_mpstats = None

RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_DNS = os.environ['RABBITMQ_DNS']


TIME_SLEEP = 1

headers_temp_mpstat = lambda token: {
    "Content-Type": "application/json",
    "X-Mpstats-TOKEN": token,
}

# year month day
url_temp_mpstat: str = 'https://mpstats.io/api/wb/get/seller?d1={}&d2={}&path={}'


def get_dates():
    current_date = date.today()

    df_c = current_date - timedelta(weeks=1)
    dt_c = current_date - timedelta(days=1)

    df_p = df_c - timedelta(weeks=1)
    dt_p = dt_c - timedelta(weeks=1)

    date_pattern = "%Y-%m-%d"

    str_df_c = datetime.strftime(df_c, date_pattern)
    str_dt_c = datetime.strftime(dt_c, date_pattern)

    str_df_p = datetime.strftime(df_p, date_pattern)
    str_dt_p = datetime.strftime(dt_p, date_pattern)

    return str_df_c, str_dt_c, str_df_p, str_dt_p


def write_to_google_sheet(data):
    sheet_headers = [
        'id',
        'thumb',
        'start_price',
        'final_price',
        'sku_first_date',
    ]
    sheet = FILE.worksheet("Выгрузка МПСТАТС")

    sheet.clear()
    sheet.resize(len(data) + 1, len(sheet_headers))

    sheet.update(range_name='A1', values=[sheet_headers])
    sheet.update(range_name='A2', values=data)


def get_data_from_mpstats():
    request_body = {
        'startRow': 0,
        'endRow': 5000,
        'filterModel': {},
        'sortModel': [],
    }

    df_c, dt_c, _, _ = get_dates()

    url = url_temp_mpstat.format(df_c, dt_c, 'Tiшka')

    request = requests.post(url, headers=headers_temp_mpstat(token_mpstats), json=request_body)
    while request.status_code != 200:
        request = requests.post(url, headers=headers_temp_mpstat(token_mpstats), json=request_body)
        time.sleep(TIME_SLEEP)

    r = request.json()

    nmids = []
    to_sheet = []
    for data in r['data']:
        nmids.append(data['id'])

        to_sheet.append([])
        to_sheet[-1].append(data['id'])
        to_sheet[-1].append('https:' + data['thumb'])
        to_sheet[-1].append(data['start_price'])
        to_sheet[-1].append(data['final_price'])
        to_sheet[-1].append(data['sku_first_date'])

    return nmids, to_sheet


def send_message_to_queue(message):
    parameters = pika.ConnectionParameters(RABBITMQ_DNS,
                                           5672,
                                           '/',
                                           pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD))
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.basic_publish(exchange='', routing_key='messages', body=message)

    connection.close()


def main(_key, _token_mpstats, _shop_name):
    try:
        global KEY, url_to_price_sheet, FILE, token_mpstats
        KEY = _key
        url_to_price_sheet = f'https://docs.google.com/spreadsheets/d/{KEY}'
        FILE = GC.open_by_key(KEY)
        token_mpstats = _token_mpstats

        nmids, data = get_data_from_mpstats()
        write_to_google_sheet(data)

        send_message_to_queue(f'<b>{_shop_name}</b> Сбор данных с mpstats прошел успешно')
        return {
            'nmids': nmids
        }
    except Exception as e:
        send_message_to_queue(f'<b>{_shop_name}</b> Ошибка при сборе данных с mpstats')
        raise Exception
