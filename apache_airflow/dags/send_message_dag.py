import json
import math
import uuid
from os import environ, path
from datetime import datetime, date, timedelta
from itertools import chain

import gspread
import pika
from airflow.models import DAG
from airflow.operators.python import PythonOperator

RABBITMQ_USERNAME = environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = environ['RABBITMQ_PASSWORD']
RABBITMQ_DNS = environ['RABBITMQ_DNS']
KEY = environ['GOOGLESHEET_KEY']

url_to_price_sheet = f'https://docs.google.com/spreadsheets/d/{KEY}'

GC = gspread.service_account(path.join(path.abspath(path.dirname(__file__)), 'abc-dev-415713-a2a407ac569b.json'))
FILE = GC.open_by_key(KEY)

CONFIG_ABC = None

CONNECTION_RABBITMQ = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_DNS,
                                                                        5672,
                                                                        '/',
                                                                        pika.PlainCredentials(RABBITMQ_USERNAME,
                                                                                              RABBITMQ_PASSWORD)))
CHANNEL_RABBITMQ = CONNECTION_RABBITMQ.channel()

VENDORCODES_UNION = []  # глобальная переменная для всех общих вендор кодов


def get_config_abc():
    b = {
        'config': 'ABC_config',
        'method': "GET_config",
        'data': {
        }
    }
    a = json.dumps(b, indent=4).encode('utf-8')

    response = None
    corr_id = str(uuid.uuid4())

    def on_response(ch, method, props, body):
        nonlocal response
        if corr_id == props.correlation_id:
            response = body

    result = CHANNEL_RABBITMQ.queue_declare(queue=str(uuid.uuid4()), exclusive=True)
    callback_queue = result.method.queue
    CHANNEL_RABBITMQ.basic_consume(
        queue=callback_queue,
        on_message_callback=on_response,
        auto_ack=True)

    queue_name = 'queue_config'
    CHANNEL_RABBITMQ.basic_publish(exchange='', routing_key=queue_name, properties=pika.BasicProperties(
        reply_to=callback_queue,
        correlation_id=corr_id,
    ), body=a)

    while response is None:
        CONNECTION_RABBITMQ.process_data_events(time_limit=5)

    global CONFIG_ABC
    CONFIG_ABC = json.loads(response)


def get_vendor_codes_by_period(nmids):
    vendor_codes = [
        [],  # 7
        [],  # 14
        [],  # 21
        [],  # 28
        [],  # 28+
    ]

    current_date = date.today() - timedelta(days=2)

    date_pattern = "%Y-%m-%d"
    for i in nmids.values():

        vendor_date = datetime.strptime(i['sku_first_date'], date_pattern).date()
        dt = (current_date - vendor_date).days
        if dt == 7:
            vendor_codes[0].append(i["vendorCode"])
        elif dt == 14:
            vendor_codes[1].append(i["vendorCode"])
        elif dt == 21:
            vendor_codes[2].append(i["vendorCode"])
        elif dt == 28:
            vendor_codes[3].append(i["vendorCode"])
        elif dt > 28:
            vendor_codes[4].append(i["vendorCode"])

    return vendor_codes


def get_message(vendor_codes):
    headers_message = [
        '7 день продаж наступил у:\n',
        '14 день продаж наступил у:\n',
        '21 день продаж наступил у:\n',
        '28 день продаж наступил у:\n',
        '28+ день продаж наступил у:\n',
    ]

    message = ''

    for head, vend in zip(headers_message, vendor_codes):
        if vend:
            message += head + '\n'.join(vend) + '\n'

    return message + url_to_price_sheet if message else 'Дни продаж не наступили'


def get_data_from_spreadsheet():
    sheet_mpstat = FILE.worksheet("Выгрузка МПСТАТС")
    sheet_wb = FILE.worksheet("Выгрузка К")
    sheet_ebitda = FILE.worksheet("EBITDA")
    sheet_ABC = FILE.worksheet("ABC")

    nmids_list_mpstat = sheet_mpstat.col_values(1)[1:]
    first_date_list = sheet_mpstat.col_values(5)[1:]

    data_from_mpstat = {nmid: {'sku_first_date': ven} for nmid, ven in zip(nmids_list_mpstat, first_date_list)}

    vendorCode_list_wb = sheet_wb.col_values(1)[1:]
    nmids_list_wb = sheet_wb.col_values(2)[1:]
    ordered_list = list(map(float, sheet_wb.col_values(3)[1:]))
    percent_buyouts_list = list(map(float, sheet_wb.col_values(7)[1:]))
    remains_list = list(map(float, sheet_wb.col_values(13)[1:]))

    data_from_wb = {
        ven: {
            'remains': rem,
            'ordered': order,
            'percent_buyouts': percent,
        }
        for nmid, ven, order, percent, rem in
        zip(nmids_list_wb, vendorCode_list_wb, ordered_list, percent_buyouts_list, remains_list)
    }

    vendorCode_list_ebitda = sheet_ebitda.col_values(1)[2:]
    prime_cost_list_ebitda = sheet_ebitda.col_values(2)[2:]  # себестоимость
    kosti_list_ebitda = sheet_ebitda.col_values(3)[2:]  # косты
    logistics_list_ebitda = sheet_ebitda.col_values(4)[2:]  # логистика
    price_list_ebitda = sheet_ebitda.col_values(5)[2:]  # цена
    ebitda_list_ebitda = sheet_ebitda.col_values(6)[2:]  # ebitda
    # margin = sheet_ebitda.col_values(7)[2:]  # маржа
    data_from_ebitda = {
        ven: {
            'prime_cost': float(pr_cs),
            'kosti': float(kos),
            'logistics': float(log),
            'price': float(price),
            'ebitda': float(ebitda),
        }
        for ven, pr_cs, kos, log, price, ebitda in
        zip(vendorCode_list_ebitda, prime_cost_list_ebitda, kosti_list_ebitda, logistics_list_ebitda, price_list_ebitda,
            ebitda_list_ebitda)
        if 'НЕТ В ТАБЛИЦЕ' not in (pr_cs, kos, log, price, ebitda) and all([ven, pr_cs, kos, log, price, ebitda])
    }

    vendorCode_ebitda_wb = set(vendorCode_list_ebitda).intersection(set(vendorCode_list_wb))
    nmids_ebitda_wb = [nmids_list_wb[vendorCode_list_wb.index(ven)] for ven in vendorCode_ebitda_wb]

    all_nmids = set(nmids_ebitda_wb).intersection(set(nmids_list_mpstat))
    nmid_vendorCode = {nmid: vendorCode_list_wb[nmids_list_wb.index(nmid)] for nmid in all_nmids}

    good_nmid_vendorCode = {nmid: ven for nmid, ven in nmid_vendorCode.items() if ven in data_from_ebitda}

    nmids_dict = {nmid: {'sku_first_date': data_from_mpstat[nmid]['sku_first_date'],
                         'vendorCode': ven
                         } for nmid, ven in good_nmid_vendorCode.items()}
    vendorCode_dict = {ven: {'percent_buyouts': data_from_wb[ven]['percent_buyouts'],
                             'ordered': data_from_wb[ven]['ordered'] / 7,
                             'remains': data_from_wb[ven]['remains'],
                             'prime_cost': data_from_ebitda[ven]['prime_cost'],
                             'kosti': data_from_ebitda[ven]['kosti'],
                             'logistics': data_from_ebitda[ven]['logistics'],
                             'price': data_from_ebitda[ven]['price'],
                             'ebitda': data_from_ebitda[ven]['ebitda'],
                             } for nmid, ven in good_nmid_vendorCode.items()}

    vendorCode_lst_ABC = sheet_ABC.col_values(1)[1:]

    categories_previous_day = {
        7: {k: v for k, v in zip(vendorCode_lst_ABC, sheet_ABC.col_values(2)[1:])},
        14: {k: v for k, v in zip(vendorCode_lst_ABC, sheet_ABC.col_values(4)[1:])},
        21: {k: v for k, v in zip(vendorCode_lst_ABC, sheet_ABC.col_values(6)[1:])},
        28: {k: v for k, v in zip(vendorCode_lst_ABC, sheet_ABC.col_values(8)[1:])},

    }

    return nmids_dict, vendorCode_dict, categories_previous_day


def calculate_ebitda_per_day(vendorCode_dict):
    ebitda_per_day = {}
    for vendor_code, item in vendorCode_dict.items():
        ebitda_per_day[vendor_code] = item['ebitda'] * item['ordered'] * item['percent_buyouts'] / 100

    return ebitda_per_day


def calculate_turnover(vendorCode_dict):
    turnover = {}
    for vendor_code, item in vendorCode_dict.items():
        turnover[vendor_code] = item['remains'] / item['ordered']

    return turnover


def enumerate2(xs, start=0, step=1):
    for x in xs:
        yield (start, x)
        start += step


def write_categories_to_google_sheet(categories, vendorCode_dict):
    sheet_ABC = FILE.worksheet("ABC")

    headers = [
        'Артикул продавца',
        'АБС 7 день',
        'Рекомендованная цена',
        'АБС 14 день',
        'Рекомендованная цена',
        'АБС 21 день',
        'Рекомендованная цена',
        'АБС 28 день',
        'Рекомендованная цена',
    ]

    rec_price_func = lambda prime_cost, logistics, kosti, margin: (prime_cost + logistics + kosti) / (0.69 - margin)

    category_margin = {
        'BC10': 0.1,
        'BC20': 0.2,
        'BC30': 0.3,
        'A': 0.4,
        'B': 0.3,
        'C': 0.1
    }
    len_headers = len(headers) - 1
    list_ABC = sheet_ABC.get_all_values()[1:]
    dict_ABC = {i[0]: i for i in list_ABC}

    for ven in VENDORCODES_UNION:
        if not ven in dict_ABC:
            dict_ABC[ven] = [ven] + [None] * len_headers

    for ind, lst in enumerate2(categories, start=1, step=2):
        for vendor_code in lst:
            category = lst[vendor_code]
            rec_price = None
            dict_ABC[vendor_code][ind] = category

            if category in category_margin:
                rec_price = rec_price_func(vendorCode_dict[vendor_code]['prime_cost'],
                                           vendorCode_dict[vendor_code]['logistics'],
                                           vendorCode_dict[vendor_code]['kosti'],
                                           category_margin[category])

            if rec_price is not None:
                dict_ABC[vendor_code][ind + 1] = int(math.ceil(rec_price))

    sheet_ABC.clear()
    sheet_ABC.resize(len(VENDORCODES_UNION) + 1, len(headers))
    sheet_ABC.update(range_name='A1', values=[headers])
    sheet_ABC.update(range_name='A2', values=list(dict_ABC.values()))

    sheet_ABC.format('F:', {
        "backgroundColor": {
            "red": 1,
            "green": 1,
            "blue": 1
        }})

    for ind, val in enumerate(sheet_ABC.col_values(6), start=1):
        if val == 'BC30':
            sheet_ABC.format(f'F{ind}', {
                "backgroundColor": {
                    "red": 1,
                    "green": 0,
                    "blue": 0
                }})


def calculate_ABC(vendor_codes_by_period, vendorCode_dict, categories_14_day):
    all_vendor_codes = chain.from_iterable(vendor_codes_by_period)
    ebitda_per_day = calculate_ebitda_per_day({k: vendorCode_dict[k] for k in all_vendor_codes})

    turnover = calculate_turnover(
        {k: vendorCode_dict[k] for k in vendor_codes_by_period[1] + vendor_codes_by_period[2]})

    # get_config_abc()
    def ABC_7(vendor_codes):
        categories = {}
        for ven in vendor_codes:
            if ebitda_per_day[ven] >= CONFIG_ABC['EBITDA']["ABC_EBITDA_7_A"]:
                categories[ven] = 'A'
            elif ebitda_per_day[ven] >= CONFIG_ABC['EBITDA']["ABC_EBITDA_7_B"]:
                categories[ven] = 'B'
            else:
                categories[ven] = 'BC30'

        return categories

    def ABC_14(vendor_codes):
        categories = {}
        for ven in vendor_codes:
            if ebitda_per_day[ven] >= CONFIG_ABC['EBITDA']["ABC_EBITDA_14_A"]:
                categories[ven] = 'A'
            elif ebitda_per_day[ven] >= CONFIG_ABC['EBITDA']["ABC_EBITDA_14_B"]:
                categories[ven] = 'B'
            elif turnover[ven] <= CONFIG_ABC['EBITDA']["ABC_TURNOVER_14"]:
                categories[ven] = 'BC30'
            else:
                categories[ven] = 'BC10'

        return categories

    def ABC_21(vendor_codes):
        categories = {}
        for ven in vendor_codes:
            if ebitda_per_day[ven] >= CONFIG_ABC['EBITDA']['ABC_EBITDA_21_A']:
                categories[ven] = 'A'
            elif ebitda_per_day[ven] >= CONFIG_ABC['EBITDA']['ABC_EBITDA_21_B']:
                categories[ven] = 'B'
            elif turnover[ven] >= CONFIG_ABC['TURNOVER']['ABC_TURNOVER_21'] and categories_14_day[ven] == 'BC10':
                categories[ven] = 'C'
            else:
                categories[ven] = 'BC30'

        return categories

    def ABC_28(vendor_codes):
        categories = {}
        for ven in vendor_codes:
            if ebitda_per_day[ven] >= CONFIG_ABC['EBITDA']['ABC_EBITDA_28_A']:
                categories[ven] = 'A'
            elif ebitda_per_day[ven] >= CONFIG_ABC['EBITDA']['ABC_EBITDA_28_B']:
                categories[ven] = 'B'
            else:
                categories[ven] = 'C'

        return categories

    def ABC_28plus(vendor_codes):
        categories = {}
        for ven in vendor_codes:
            if ebitda_per_day[ven] >= CONFIG_ABC['EBITDA']['ABC_EBITDA_28_A']:
                categories[ven] = 'A'
            elif ebitda_per_day[ven] >= CONFIG_ABC['EBITDA']['ABC_EBITDA_28_B']:
                categories[ven] = 'B'
            else:
                categories[ven] = 'C'

        return categories

    categories = [
        ABC_7(vendor_codes_by_period[0]),
        ABC_14(vendor_codes_by_period[1]),
        ABC_21(vendor_codes_by_period[2]),
        ABC_28(vendor_codes_by_period[3]),
        ABC_28plus(vendor_codes_by_period[4]),
    ]

    write_categories_to_google_sheet(categories, vendorCode_dict)


def write_recommended_price_to_sheet(vendor_codes_by_period, vendorCode_dict):
    sheet = FILE.worksheet("Загрузка цены")
    sheet_ABC = FILE.worksheet("ABC")
    list_ABC = sheet_ABC.get_all_values()[1:]

    dict_ABC = {
        i[0]: {
            7: i[1],
            'recommended_price_7': i[2],
            14: i[3],
            'recommended_price_14': i[4],
            21: i[5],
            'recommended_price_21': i[6],
            28: i[7],
            'recommended_price_28': i[8],
        }
        for i in list_ABC
    }

    headers_1 = [
        'Решение принято',
        'Нет',
        'Цена выгружена',
        'Нет',
    ]

    headers_2 = [
        'Артикул продавца',
        'ABC прошлая неделя',
        'ABC текущая неделя',
        'Текущая цена',
        'Рекомендованная цена',
        'Загрузочная цена',
    ]

    sheet.clear()
    data = []

    for ind, per in enumerate(vendor_codes_by_period, start=1):
        for ven in per:
            row = []
            row.append(ven)
            week = ind * 7
            if week == 7:
                row.append('NEW')
            else:
                row.append(dict_ABC[ven][week - 7])

            row.append(dict_ABC[ven][week])
            row.append(vendorCode_dict[ven]['price'])
            row.append(dict_ABC[ven][f'recommended_price_{week}'])

            data.append(row)

    # sheet.resize(len(data) + 3, len(headers_2))
    sheet.resize(len(data) + 20, len(headers_2))
    sheet.update(range_name='A1', values=[headers_1])
    sheet.update(range_name='A3', values=[headers_2])

    if data:
        sheet.update(range_name='A4', values=data)
    else:
        sheet.update_cell(1, 2, 'Да')
        sheet.update_cell(1, 4, 'Да')


def send_message_to_queue(message):
    CHANNEL_RABBITMQ.basic_publish(exchange='', routing_key='messages', body=message)


def main():
    global VENDORCODES_UNION
    get_config_abc()

    nmids, vendorCode_dict, vendorCode_lst_ABC = get_data_from_spreadsheet()

    VENDORCODES_UNION = list(vendorCode_dict.keys())

    vendor_codes_by_period = get_vendor_codes_by_period(nmids)

    message = get_message(vendor_codes_by_period)
    calculate_ABC(vendor_codes_by_period, vendorCode_dict, vendorCode_lst_ABC[14])
    write_recommended_price_to_sheet(vendor_codes_by_period, vendorCode_dict)

    send_message_to_queue(message)


def start_dag():
    main()
    CONNECTION_RABBITMQ.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 5),
}

with DAG(dag_id='send_message_tg_bot',
         schedule='0 7 * * *',
         default_args=default_args) as dag:
    dice = PythonOperator(
        task_id='send_message',
        python_callable=start_dag,
        dag=dag,
        retries=2,
    )

if __name__ == '__main__':
    start_dag()
