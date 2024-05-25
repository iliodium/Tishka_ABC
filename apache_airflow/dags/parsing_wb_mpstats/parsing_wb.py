import os
import time
from datetime import timedelta, datetime, date

import gspread
import pika
import requests

KEY_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), './../abc-dev-415713-a2a407ac569b.json')

KEY = os.environ['GOOGLESHEET_KEY']

url_to_price_sheet = f'https://docs.google.com/spreadsheets/d/{KEY}'

GC = gspread.service_account(KEY_PATH)
FILE = GC.open_by_key(KEY)

RABBITMQ_USERNAME = os.environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_PASSWORD']
RABBITMQ_DNS = os.environ['RABBITMQ_DNS']

token_wb = os.environ['TOKEN_WB']

TIME_SLEEP = 1

headers_temp_wb = lambda token: {
    "Content-Type": "application/json",
    "Authorization": token,
}

request_body_temp = lambda nmids_list, begin, end: {
    "nmIDs": nmids_list,
    "period": {
        "begin": begin,
        "end": end
    },
    "timezone": "Europe/Moscow",
    "aggregationLevel": "day"
}

url_temp_report: str = 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history'
url_temp_stocks: str = 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={}'


def get_history(nmids_list: list[int], date_from: str, date_to: str):
    request_body = request_body_temp(nmids_list, date_from, date_to)
    headers = headers_temp_wb(token_wb)
    request = requests.post(url_temp_report, headers=headers, json=request_body)
    while request.status_code != 200:
        if request.status_code == 400:
            raise Exception
        request = requests.post(url_temp_report, headers=headers, json=request_body)
        time.sleep(TIME_SLEEP)

    rq = request.json()
    data = {}
    for i in rq['data']:
        data[i['nmID']] = {
            'vendorCode': i['vendorCode'],
            'imtName': i['imtName'],
            'history': i['history'],
        }

    return data


def batch(iterable, batch_size=1):
    l = len(iterable)
    for ndx in range(0, l, batch_size):
        yield iterable[ndx:min(ndx + batch_size, l)]


def get_stocks():
    # "%Y-%m-%dT%H:%M:%S"

    url = url_temp_stocks.format(datetime.strftime(datetime.now(), "%Y-%m-%dT"))
    headers = headers_temp_wb(token_wb)
    request = requests.get(url, headers=headers)
    while request.status_code != 200:
        request = requests.get(url, headers=headers)
        time.sleep(TIME_SLEEP)

    rj = request.json()
    d = {k['nmId']: 0 for k in rj}
    for stock in rj:
        d[stock['nmId']] += stock['quantity']
    return d


def transform_data(nmids_list, data_current, data_previous) -> list:
    data_out = []
    stocks = get_stocks()

    for nmid in nmids_list:
        row = []
        row.append(data_current[nmid]['vendorCode'])
        row.append(nmid)
        row.extend([
            0,  # Заказали, шт
            0,  # Заказали, шт (предыдущий период)

            0,  # Выкупили, шт
            0,  # Выкупы, шт (предыдущий период)

            0,  # Процент выкупа
            0,  # Процент выкупа (предыдущий период)

            0,  # Заказали на сумму, руб
            0,  # Заказали на сумму, руб (предыдущий период)

            0,  # Средняя цена, руб
            0,  # Средняя цена, руб (предыдущий период)

            0,  # Остатки склад, шт
        ])

        for ch, ph in zip(data_current[nmid]['history'], data_previous[nmid]['history']):
            row[2] += ch['ordersCount']
            row[3] += ph['ordersCount']

            row[4] += ch['buyoutsCount']
            row[5] += ph['buyoutsCount']

            row[6] += ch['buyoutPercent']
            row[7] += ph['buyoutPercent']

            row[8] += ch['ordersSumRub']
            row[9] += ph['ordersSumRub']

        row[6] /= 6
        row[7] /= 7
        try:
            row[10] = row[8] / row[2]
        except ZeroDivisionError:
            row[10] = 0
        try:
            row[11] = row[9] / row[3]
        except ZeroDivisionError:
            row[11] = 0

        try:
            row[12] = stocks[row[1]]
        except KeyError:
            ...

        for ind_row in (6, 7, 10, 11):
            row[ind_row] = round(row[ind_row])
        data_out.append(row)

    return data_out


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
        'Артикул продавца',
        'Номенклатура',
        'Заказали, шт',
        'Заказали, шт (предыдущий период)',
        'Выкупили, шт',
        'Выкупы, шт (предыдущий период)',
        'Процент выкупа',
        'Процент выкупа (предыдущий период)',
        'Заказали на сумму, руб',
        'Заказали на сумму, руб (предыдущий период)',
        'Средняя цена, руб',
        'Средняя цена, руб (предыдущий период)',
        'Остатки склад, шт',
    ]

    sheet = FILE.worksheet("Выгрузка К")
    sheet.clear()
    sheet.resize(len(data) + 1, len(sheet_headers))

    sheet.update(range_name='A1', values=[sheet_headers])
    sheet.update(range_name='A2', values=data)


def send_message_to_queue(message):
    parameters = pika.ConnectionParameters(RABBITMQ_DNS,
                                           5672,
                                           '/',
                                           pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD))
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.basic_publish(exchange='', routing_key='messages', body=message)

    connection.close()


def get_data_from_wb(nmids_list):
    df_c, dt_c, df_p, dt_p = get_dates()

    data_all_nmids_current = {}
    data_all_nmids_previous = {}

    for nmids in batch(nmids_list, 20):
        data_all_nmids_current.update(get_history(nmids, df_c, dt_c))
        data_all_nmids_previous.update(get_history(nmids, df_p, dt_p))

    return data_all_nmids_current, data_all_nmids_previous


def main(nmids):
    try:
        data_all_nmids_current, data_all_nmids_previous = get_data_from_wb(nmids)
        data = transform_data(nmids, data_all_nmids_current, data_all_nmids_previous)
        write_to_google_sheet(data)

        send_message_to_queue('Сбор данных с wb прошел успешно')
    except Exception as e:
        send_message_to_queue('Ошибка при сборе данных с wb')
        raise Exception
