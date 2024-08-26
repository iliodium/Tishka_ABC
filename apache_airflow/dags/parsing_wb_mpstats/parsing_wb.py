import os
import time
import uuid
import zipfile
from datetime import timedelta, datetime, date
from io import BytesIO

import gspread
import pandas as pd
import pika
import requests

KEY_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), './../abc-dev-415713-a2a407ac569b.json')

KEY = os.environ['GOOGLESHEET_KEY']

url_to_price_sheet = f'https://docs.google.com/spreadsheets/d/{KEY}'
url_generate_report = 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/downloads'

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
HEADERS = headers_temp_wb(token_wb)

request_body_temp = lambda nmids_list, begin, end: {
    "nmIDs": nmids_list,
    "period": {
        "begin": begin,
        "end": end
    },
    "timezone": "Europe/Moscow",
    "aggregationLevel": "day"
}

request_body_generate_report = lambda uuid_report, nmids_list, startDate, endDate: {
    'id': uuid_report,
    'reportType': 'DETAIL_HISTORY_REPORT',
    'params': {
        'nmIDs': nmids_list,
        'startDate': startDate,
        'endDate': endDate,
        'timezone': 'Europe/Moscow',
        'aggregationLevel': 'day',
        "skipDeletedNm": True
    }
}

url_temp_report: str = 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history'
url_temp_stocks: str = 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={}'


def make_request_to_wb(url: str, method: str, json: dict = dict()):
    method = {
        'GET': requests.get,
        'POST': requests.post,
    }[method]

    response = method(url, headers=HEADERS, json=json)
    if response.status_code not in (200, 429, 500):
        raise Exception(response, response.content)
    else:
        while response.status_code in (429, 500):
            if response.status_code not in (429, 500):
                raise Exception(response, response.content)
            response = method(url, headers=HEADERS, json=json)
            time.sleep(TIME_SLEEP)

    return response


# старый метод, использовался до джема
def get_history(nmids_list: list[int], date_from: str, date_to: str):
    request_body = request_body_temp(nmids_list, date_from, date_to)
    response = make_request_to_wb(url_temp_report, 'POST', request_body)
    # response = requests.post(url_temp_report, headers=HEADERS, json=request_body)
    # if response.status_code not in (200, 429, 500):
    #     raise Exception
    # else:
    #     while response.status_code in (429, 500):
    #         if response.status_code not in (429, 500):
    #             raise Exception
    #         response = requests.post(url_temp_report, headers=HEADERS, json=request_body)
    #         time.sleep(TIME_SLEEP)

    rq = response.json()
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

    url = url_temp_stocks.format(datetime.strftime(datetime.now(), "%Y-%m-%d"))

    response = make_request_to_wb(url, 'GET')

    # response = requests.get(url, headers=HEADERS)
    # if response.status_code not in (200, 429, 500):
    #     raise Exception
    # else:
    #     while response.status_code in (429, 500):
    #         if response.status_code not in (429, 500):
    #             raise Exception
    #         response = requests.get(url, headers=HEADERS)
    #         time.sleep(TIME_SLEEP)

    rj = response.json()
    d = {k['nmId']: 0 for k in rj}
    for stock in rj:
        d[stock['nmId']] += stock['quantity']
    return d


def transform_data(nmids_list, data_current, data_previous) -> list:
    data_out = []
    stocks = get_stocks()

    for nmid in nmids_list:
        # проверка на то есть ли товар в 2 неделях
        # чтобы не было ошибок если товар продается только 1 неделю
        if not (nmid in data_current and nmid in data_previous):
            continue
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

        for ch in data_current[nmid]['history']:
            row[2] += ch['ordersCount']
            row[4] += ch['buyoutsCount']
            row[6] += ch['buyoutPercent']
            row[8] += ch['ordersSumRub']

        for ph in data_previous[nmid]['history']:
            row[3] += ph['ordersCount']
            row[5] += ph['buyoutsCount']
            row[7] += ph['buyoutPercent']
            row[9] += ph['ordersSumRub']

        # for ch, ph in zip(data_current[nmid]['history'], data_previous[nmid]['history']):
        #     row[2] += ch['ordersCount']
        #     row[3] += ph['ordersCount']
        #
        #     row[4] += ch['buyoutsCount']
        #     row[5] += ph['buyoutsCount']
        #
        #     row[6] += ch['buyoutPercent']
        #     row[7] += ph['buyoutPercent']
        #
        #     row[8] += ch['ordersSumRub']
        #     row[9] += ph['ordersSumRub']

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


# новый метод используется при работе с джемом
def get_detail_history_report(nmids_list: list[int], date_from: str, date_to: str):
    # проверка на то существует ли нужный отчет
    uuid_report = None
    response = make_request_to_wb(url_generate_report, 'GET').json()
    data = response['data']
    for report in data:
        if report['startDate'] == date_from and report['endDate'] == date_to:
            uuid_report = report['id']
            break

    if uuid_report is None:
        uuid_report = str(uuid.uuid4())

        request_body = request_body_generate_report(uuid_report, nmids_list, date_from, date_to)
        response = make_request_to_wb(url_generate_report, 'POST', request_body).json()
        if response['error']:
            print(response['errorText'])
            raise Exception
        print(6)
        print(url_generate_report)
        flag = True
        while flag:
            print(8)
            response = make_request_to_wb(url_generate_report, 'GET').json()
            print(response)
            if response['error']:
                print(response['errorText'])
                raise Exception
            data = response['data']
            for report in data:
                if report['status'] == 'SUCCESS' and report['id'] == uuid_report:
                    flag = False
                    break
            time.sleep(5)

    print(7)
    url = f'{url_generate_report}/file/{uuid_report}'
    response = make_request_to_wb(url, 'GET')

    zip_data = BytesIO(response.content)
    # Открываю ZIP-архив
    with zipfile.ZipFile(zip_data, 'r') as zip_ref:
        # Получите список файлов в архиве
        file_list = zip_ref.namelist()

        csv_file_name = file_list[0]

        # Читаю CSV файл в DataFrame
        with zip_ref.open(csv_file_name) as csv_file:
            df = pd.read_csv(csv_file)

    data = {}
    for index, row in df.iterrows():
        nmid = row['nmID']
        data_nmid = data.setdefault(nmid, {
            'vendorCode': '',
            'imtName': '',
            'history': []
        })
        row = row.to_dict()
        row.pop('nmID', None)
        data_nmid['history'].append(row)

    return data


def get_data_from_wb(nmids_list):
    df_c, dt_c, df_p, dt_p = get_dates()

    # тк в джеме нет vendorCode и imtName получаю их из старого метода
    history_for_vendorcode = {}
    for nmids in batch(nmids_list, 20):
        history_for_vendorcode.update(get_history(nmids, df_c, dt_c))
    print(4)
    data_all_nmids_previous = get_detail_history_report(nmids_list, df_p, dt_p)
    data_all_nmids_current = get_detail_history_report(nmids_list, df_c, dt_c)
    print(5)
    print(data_all_nmids_previous)
    print(data_all_nmids_current)
    print(history_for_vendorcode)
    for nmid, data in history_for_vendorcode.items():
        if nmid in data_all_nmids_previous:
            data_all_nmids_previous[nmid]['vendorCode'] = data['vendorCode']
            data_all_nmids_previous[nmid]['imtName'] = data['imtName']

        if nmid in data_all_nmids_current:
            data_all_nmids_current[nmid]['vendorCode'] = data['vendorCode']
            data_all_nmids_current[nmid]['imtName'] = data['imtName']

    # for nmids in batch(nmids_list, 20):
    #     data_all_nmids_current.update(get_history(nmids, df_c, dt_c))
    #     data_all_nmids_previous.update(get_history(nmids, df_p, dt_p))

    return data_all_nmids_current, data_all_nmids_previous


def main(nmids):
    try:
        data_all_nmids_current, data_all_nmids_previous = get_data_from_wb(nmids)
        print(1)
        data = transform_data(nmids, data_all_nmids_current, data_all_nmids_previous)
        print(2)
        write_to_google_sheet(data)

        send_message_to_queue('Сбор данных с wb прошел успешно')
    except Exception as e:
        send_message_to_queue('Ошибка при сборе данных с wb')
        raise Exception
