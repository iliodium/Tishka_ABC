import datetime
import os
import time

import gspread
import pika
import requests
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

# https://docs.google.com/spreadsheets/d/1RKUol_KBPRPl-wTTNUBl5m7Wgq-5Gh_yUIXCh-2Z2CA

token_wb = os.environ['TOKEN_WB']
token_mpstat = os.environ['TOKEN_MPSTAT']

headers_temp_mpstat = lambda token: {
    "Content-Type": "application/json",
    "X-Mpstats-TOKEN": token,
}

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

# year month day
url_temp_mpstat: str = 'https://mpstats.io/api/wb/get/seller?d1={}&d2={}&path={}'


def get_history(nmids_list: list[int], date_from: str, date_to: str):
    request_body = request_body_temp(nmids_list, date_from, date_to)
    headers = headers_temp_wb(token_wb)

    request = requests.post(url_temp_report, headers=headers, json=request_body)
    while request.status_code in (400, 403, 429, 500):
        request = requests.post(url_temp_report, headers=headers, json=request_body)
        time.sleep(0.2)

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


def get_nmids_wb():
    url = url_temp_stocks.format(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%dT"))
    headers = headers_temp_wb(token_wb)
    request = requests.get(url, headers=headers)
    while request.status_code in (400, 403, 429, 500):
        request = requests.get(url, headers=headers)
        time.sleep(0.2)

    return [k['nmId'] for k in request.json()]


def get_nmids_mpstat():
    seller_name = 'Tiшka'
    request_body = {
        'startRow': 0,
        'endRow': 5000,
        'filterModel': {},
        'sortModel': [],
    }

    df_c, dt_c, df_p, dt_p = get_dates()

    url = url_temp_mpstat.format(df_c, dt_c, seller_name)

    request = requests.post(url, headers=headers_temp_mpstat(token_mpstat), json=request_body)
    while request.status_code in (202, 429, 500):
        request = requests.post(url, headers=headers_temp_mpstat(token_mpstat), json=request_body)

        time.sleep(0.2)

    return [data['id'] for data in request.json()['data']]


def get_union_nmids():
    nmids_wb = get_nmids_wb()
    nmids_mpstat = get_nmids_mpstat()
    return list(set(nmids_mpstat).intersection(nmids_wb))


def get_stocks():
    # "%Y-%m-%dT%H:%M:%S"

    url = url_temp_stocks.format(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%dT"))
    headers = headers_temp_wb(token_wb)
    request = requests.get(url, headers=headers)
    while request.status_code in (400, 403, 429, 500):
        request = requests.get(url, headers=headers)
        time.sleep(0.2)

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
    current_date = datetime.date.today()

    df_c = current_date - datetime.timedelta(weeks=1)
    dt_c = current_date - datetime.timedelta(days=1)

    df_p = df_c - datetime.timedelta(weeks=1)
    dt_p = dt_c - datetime.timedelta(weeks=1)

    date_pattern = "%Y-%m-%d"

    str_df_c = datetime.datetime.strftime(df_c, date_pattern)
    str_dt_c = datetime.datetime.strftime(dt_c, date_pattern)

    str_df_p = datetime.datetime.strftime(df_p, date_pattern)
    str_dt_p = datetime.datetime.strftime(dt_p, date_pattern)

    return str_df_c, str_dt_c, str_df_p, str_dt_p


def get_nmids_list():
    sheet_nmids = FILE.worksheet('Артикулы')
    return list(map(int, sheet_nmids.col_values(1)))


def write_to_google_sheet(api: str, data):
    if api == 'wb':
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
    elif api == 'mpstat':
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


def write_data_from_wb(nmids_list) -> None:
    df_c, dt_c, df_p, dt_p = get_dates()

    data_all_nmids_current = {}
    data_all_nmids_previous = {}

    for nmids in batch(nmids_list, 20):
        data_all_nmids_current.update(get_history(nmids, df_c, dt_c))
        data_all_nmids_previous.update(get_history(nmids, df_p, dt_p))

    data = transform_data(nmids_list, data_all_nmids_current, data_all_nmids_previous)

    write_to_google_sheet('wb', data)


def write_data_from_mpstats(nmids) -> None:
    seller_name = 'Tiшka'
    request_body = {
        'startRow': 0,
        'endRow': 5000,
        'filterModel': {},
        'sortModel': [],
    }

    df_c, dt_c, df_p, dt_p = get_dates()

    url = url_temp_mpstat.format(df_c, dt_c, seller_name)

    request = requests.post(url, headers=headers_temp_mpstat(token_mpstat), json=request_body)
    while request.status_code in (202, 429, 500):
        request = requests.post(url, headers=headers_temp_mpstat(token_mpstat), json=request_body)

        time.sleep(0.2)

    r = request.json()

    to_sheet = []
    for data in r['data']:
        if data['id'] in nmids:
            to_sheet.append([])
            to_sheet[-1].append(data['id'])
            to_sheet[-1].append('https:' + data['thumb'])
            to_sheet[-1].append(data['start_price'])
            to_sheet[-1].append(data['final_price'])
            to_sheet[-1].append(data['sku_first_date'])

    write_to_google_sheet('mpstat', to_sheet)


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


def start_dag():
    try:
        nmids_list = get_union_nmids()
        write_data_from_mpstats(nmids_list)
        write_data_from_wb(nmids_list)
        send_message_to_queue('Сбор данных прошел успешно')
    except Exception as e:
        send_message_to_queue('Ошибка при сборе данных')
        raise Exception


default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2024, 5, 5),
}

with DAG(dag_id='parser_dag',
         schedule='0 6 * * *',
         default_args=default_args,
         ) as dag:
    dice = PythonOperator(
        task_id='parser',
        python_callable=start_dag,
        dag=dag,
        retries=10,
    )

if __name__ == '__main__':
    start_dag()
