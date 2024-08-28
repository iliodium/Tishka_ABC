import json
import math
import uuid
from os import environ, path
from datetime import datetime, date, timedelta
from itertools import chain
from enum import StrEnum

import gspread
import pika
from airflow.models import DAG
from airflow.operators.python import PythonOperator

RABBITMQ_USERNAME = environ['RABBITMQ_USERNAME']
RABBITMQ_PASSWORD = environ['RABBITMQ_PASSWORD']
RABBITMQ_DNS = environ['RABBITMQ_DNS']

FILE = None
url_to_price_sheet = None

GC = gspread.service_account(path.join(path.abspath(path.dirname(__file__)), 'abc-dev-415713-a2a407ac569b.json'))

CONFIG_ABC = None

CONNECTION_RABBITMQ = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_DNS,
                                                                        5672,
                                                                        '/',
                                                                        pika.PlainCredentials(RABBITMQ_USERNAME,
                                                                                              RABBITMQ_PASSWORD)))
CHANNEL_RABBITMQ = CONNECTION_RABBITMQ.channel()

VENDORCODES_UNION = []  # глобальная переменная для всех общих вендор кодов


class SaleDay(StrEnum):
    day_7 = '7 день продаж'
    day_14 = '14 день продаж'
    day_21 = '21 день продаж'
    day_28 = '28 день продаж'


class ChangeABC(StrEnum):
    change_category = 'Поменялась категория'
    empty_remains = 'Закончились остатки'
    abc_change = 'У этих товаров поменялась категория'


class GooglesheetWorksheet(StrEnum):
    mpstat = "Выгрузка МПСТАТС"
    wb = "Выгрузка К"
    ebitda = "EBITDA"
    abc = "ABC"
    update_price = "Загрузка цены"
    manual_date = 'Ручные даты'


class TempText(StrEnum):
    abc_7_day = 'АБС 7 день'
    abc_14_day = 'АБС 14 день'
    abc_21_day = 'АБС 21 день'
    abc_28_day = 'АБС 28 день'

    abc_today = 'АБС вчера'
    abc_now = 'АБС сегодня'
    abc_previous = 'АБС прошлый период'
    abc_current = 'АБС текущий период'

    current_price = 'Текущая цена'
    new_price = 'Загрузочная цена'

    nmid_seller = 'Артикул продавца'
    recommended_price = 'Рекомендованная цена'
    reason_price_change = 'Причина изменения цены'
    decision_accept = 'Решение принято'
    price_updated = 'Цена выгружена'
    no = 'Нет'
    yes = 'Да'


class MPStatColumnName(StrEnum):
    sku_first_date = 'sku_first_date'
    remains = 'remains'
    ordered = 'ordered'
    percent_buyouts = 'percent_buyouts'


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

    current_date = date.today()

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
        f'{SaleDay.day_7}',
        f'{SaleDay.day_14}',
        f'{SaleDay.day_21}',
        f'{SaleDay.day_28}',
    ]

    message = ''

    for head, vend in zip(headers_message, vendor_codes):
        if vend:
            message += f'<b>{head} наступил у:</b>\n' + '\n'.join(vend) + '\n' + '\n'

    sheet_ABC = FILE.worksheet("ABC")
    reasons = [
        (ChangeABC.change_category, []),
        (ChangeABC.empty_remains, []),
    ]

    for i in reasons:
        for k, r in zip(sheet_ABC.col_values(1)[1:], sheet_ABC.col_values(13)[1:]):
            if r == i[0]:
                i[1].append(k)

    for i in reasons:
        if i[1]:
            message += f'<b>{i[0]}:</b>\n' + '\n'.join(i[1]) + '\n' + '\n'

    change_abc_ven = []
    for ven, abc_1, abc_2, reason in zip(sheet_ABC.col_values(1)[1:],
                                         sheet_ABC.col_values(10)[1:],
                                         sheet_ABC.col_values(11)[1:],
                                         sheet_ABC.col_values(12)[1:]):
        if abc_1 != abc_2 and reason == '':
            change_abc_ven.append(ven)

    if change_abc_ven:
        message += f'<b>{ChangeABC.abc_change}:</b>\n' + '\n'.join(change_abc_ven) + '\n' + '\n'

    return message + url_to_price_sheet if message else 'Дни продаж не наступили'


def get_data_from_spreadsheet():
    sheet_mpstat = FILE.worksheet(GooglesheetWorksheet.mpstat)
    sheet_wb = FILE.worksheet(GooglesheetWorksheet.wb)
    sheet_ebitda = FILE.worksheet(GooglesheetWorksheet.ebitda)
    sheet_ABC = FILE.worksheet(GooglesheetWorksheet.abc)
    sheet_manual_date = FILE.worksheet(GooglesheetWorksheet.manual_date)

    nmids_list_mpstat = sheet_mpstat.col_values(1)[1:]
    first_date_list = sheet_mpstat.col_values(5)[1:]

    data_from_mpstat = {nmid: {MPStatColumnName.sku_first_date: ven} for nmid, ven in
                        zip(nmids_list_mpstat, first_date_list)}
    
    ven_manual_date =  {ven: dt for dt, ven in
                        zip(sheet_manual_date.col_values(1)[1:], sheet_manual_date.col_values(2)[1:])}


    vendorCode_list_wb = sheet_wb.col_values(1)[1:]
    nmids_list_wb = sheet_wb.col_values(2)[1:]
    ordered_list = list(map(float, sheet_wb.col_values(3)[1:]))
    percent_buyouts_list = list(map(float, sheet_wb.col_values(7)[1:]))
    remains_list = list(map(float, sheet_wb.col_values(13)[1:]))

    data_from_wb = {
        ven: {
            MPStatColumnName.remains: rem,
            MPStatColumnName.ordered: order,
            MPStatColumnName.percent_buyouts: percent,
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
            'prime_cost': float(pr_cs.replace(',','.').replace(' ','')),
            'kosti': float(kos.replace(',','.').replace(' ','')),
            'logistics': float(log.replace(',','.').replace(' ','')),
            'price': float(price.replace(',','.').replace(' ','')),
            'ebitda': float(ebitda.replace(',','.').replace(' ','')),
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

    nmids_dict = {nmid: {'sku_first_date': ven_manual_date[ven] if ven in ven_manual_date else data_from_mpstat[nmid]['sku_first_date'],
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
    categories_by_period = {
        7: {k: v for k, v in zip(vendorCode_lst_ABC, sheet_ABC.col_values(2)[1:])},
        14: {k: v for k, v in zip(vendorCode_lst_ABC, sheet_ABC.col_values(4)[1:])},
        21: {k: v for k, v in zip(vendorCode_lst_ABC, sheet_ABC.col_values(6)[1:])},
        28: {k: v for k, v in zip(vendorCode_lst_ABC, sheet_ABC.col_values(8)[1:])},
        '28plus_previous': {k: v for k, v in zip(vendorCode_lst_ABC, sheet_ABC.col_values(10)[1:])},
        '28plus_current': {k: v for k, v in zip(vendorCode_lst_ABC, sheet_ABC.col_values(11)[1:])},

    }

    return nmids_dict, vendorCode_dict, categories_by_period


def calculate_ebitda_per_day(vendorCode_dict):
    ebitda_per_day = {}
    for vendor_code, item in vendorCode_dict.items():
        ebitda_per_day[vendor_code] = item['ebitda'] * item['ordered'] * item['percent_buyouts'] / 100

    return ebitda_per_day


def calculate_turnover(vendorCode_dict):
    turnover = {}
    for vendor_code, item in vendorCode_dict.items():
        try:
            turnover[vendor_code] = item['remains'] / item['ordered']
        except ZeroDivisionError:
            turnover[vendor_code] = 0

    return turnover


def enumerate2(xs, start=0, step=1):
    for x in xs:
        yield (start, x)
        start += step


def round_price(price):
    return int(math.ceil(price))


def write_categories_to_google_sheet(categories, vendorCode_dict):
    sheet_ABC = FILE.worksheet("ABC")

    headers = [
        TempText.nmid_seller,
        TempText.abc_7_day,
        TempText.recommended_price,
        TempText.abc_14_day,
        TempText.recommended_price,
        TempText.abc_21_day,
        TempText.recommended_price,
        TempText.abc_28_day,
        TempText.recommended_price,
        TempText.abc_today,
        TempText.abc_now,
        TempText.recommended_price,
        TempText.reason_price_change,
    ]

    rec_price_func = lambda prime_cost, logistics, kosti, margin: (prime_cost + logistics + kosti) / (0.69 - margin)

    len_headers = len(headers)
    len_headers_m = len_headers - 1

    list_ABC = sheet_ABC.get_all_values()[1:]
    dict_ABC = {i[0]: i for i in list_ABC if i[0] in VENDORCODES_UNION}

    for vendor_code in VENDORCODES_UNION:
        if not vendor_code in dict_ABC:
            dict_ABC[vendor_code] = [vendor_code] + [None] * len_headers_m

    category_margin = {
        'BC10': 0.1,
        'BC20': 0.2,
        'BC30': 0.3,
        'A': 0.4,
        'B': 0.3,
        'C': 0.1
    }

    sale_day_reason = [
        SaleDay.day_7,
        SaleDay.day_14,
        SaleDay.day_21,
        SaleDay.day_28,
    ]
    for ind, lst in enumerate2(categories[:-1], start=1, step=2):
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
                dict_ABC[vendor_code][ind + 1] = round_price(rec_price)

            dict_ABC[vendor_code][12] = sale_day_reason[ind // 2]

    # 28+
    category = categories[-1]
    for vendor_code in category.keys():
        if dict_ABC[vendor_code][9] == '':
            dict_ABC[vendor_code][9] = category[vendor_code]['category']
            dict_ABC[vendor_code][10] = category[vendor_code]['category']
        else:
            dict_ABC[vendor_code][9] = dict_ABC[vendor_code][10]
            dict_ABC[vendor_code][10] = category[vendor_code]['category']
            dict_ABC[vendor_code][11] = round_price(rec_price_func(vendorCode_dict[vendor_code]['prime_cost'],
                                                                   vendorCode_dict[vendor_code]['logistics'],
                                                                   vendorCode_dict[vendor_code]['kosti'],
                                                                   category[vendor_code]['price'])) if category[
                vendor_code].get(
                'price') else ''
            dict_ABC[vendor_code][12] = category[vendor_code]['message'] if category[vendor_code].get('message') else ''

    sheet_ABC.clear()
    sheet_ABC.resize(len(VENDORCODES_UNION) + 1, len_headers)
    sheet_ABC.update(range_name='A1', values=[headers])
    sheet_ABC.update(range_name='A2', values=list(dict_ABC.values()))

    sheet_ABC.format('F:K', {
        "backgroundColor": {
            "red": 1,
            "green": 1,
            "blue": 1
        }})

    for ind, val in enumerate(sheet_ABC.col_values(6)[1:], start=2):
        if val == 'BC30':
            sheet_ABC.format(f'F{ind}', {
                "backgroundColor": {
                    "red": 1,
                    "green": 0,
                    "blue": 0
                }})

    for ind, val in enumerate(zip(sheet_ABC.col_values(10)[1:], sheet_ABC.col_values(11)[1:]), start=2):
        if val[0] != val[1]:
            sheet_ABC.format(f'J{ind}:K{ind}', {
                "backgroundColor": {
                    "red": 1,
                    "green": 0,
                    "blue": 0
                }})


def calculate_ABC(vendor_codes_by_period, vendorCode_dict, categories_by_period):
    all_vendor_codes = list(chain.from_iterable(vendor_codes_by_period))
    ebitda_per_day = calculate_ebitda_per_day({k: vendorCode_dict[k] for k in all_vendor_codes})
    turnover = calculate_turnover({k: vendorCode_dict[k] for k in all_vendor_codes})

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
            elif turnover[ven] <= CONFIG_ABC['TURNOVER']["ABC_TURNOVER_14"]:
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
            elif turnover[ven] >= CONFIG_ABC['TURNOVER']['ABC_TURNOVER_21'] and categories_by_period[14].get(
                    ven) == 'BC10':
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
            if not categories_by_period['28plus_previous'].get(ven):
                categories[ven] = {'category': ABC_28([ven])[ven]}
            elif ebitda_per_day[ven] >= CONFIG_ABC['EBITDA']['ABC_EBITDA_28PLUS_A']:
                if categories_by_period['28plus_previous'][ven] == 'A':
                    categories[ven] = {'category': 'A'}
                else:
                    categories[ven] = {
                        'category': 'A',
                        'price': 0.4,
                        'message': ChangeABC.change_category
                    }
            elif ebitda_per_day[ven] >= CONFIG_ABC['EBITDA']['ABC_EBITDA_28PLUS_B']:
                if categories_by_period['28plus_previous'][ven] == 'B':
                    categories[ven] = {'category': 'B'}
                else:
                    if categories_by_period['28plus_previous'][ven] == 'A' and turnover[ven] <= CONFIG_ABC['TURNOVER'][
                        'ABC_TURNOVER_28PLUS_B']:
                        categories[ven] = {
                            'price': 0.4,
                            'message': ChangeABC.empty_remains
                        }
                    else:
                        categories[ven] = {
                            'price': 0.3,
                            'message': ChangeABC.change_category
                        }
                    categories[ven]['category'] = 'B'

            elif categories_by_period['28plus_previous'][ven] == 'C':
                categories[ven] = {'category': 'C'}

            elif categories_by_period['28plus_previous'][ven] in ('A', 'B') and turnover[ven] <= CONFIG_ABC['TURNOVER'][
                'ABC_TURNOVER_28PLUS_C']:
                categories[ven] = {
                    'category': 'C',
                    'price': 0.3,
                    'message': ChangeABC.empty_remains
                }
            else:
                categories[ven] = {
                    'category': 'C',
                    'price': 0.1,
                    'message': ChangeABC.change_category
                }

        return categories

    return [
        ABC_7(vendor_codes_by_period[0]),
        ABC_14(vendor_codes_by_period[1]),
        ABC_21(vendor_codes_by_period[2]),
        ABC_28(vendor_codes_by_period[3]),
        ABC_28plus(vendor_codes_by_period[4]),
    ]


def write_recommended_price_to_sheet(vendor_codes_by_period, vendorCode_dict):
    sheet = FILE.worksheet(GooglesheetWorksheet.update_price)
    sheet_ABC = FILE.worksheet(GooglesheetWorksheet.abc)
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
            'reason': i[12],
        }
        for i in list_ABC
    }

    headers_1 = [
        TempText.decision_accept,
        TempText.no,
        TempText.price_updated,
        TempText.no,
    ]

    headers_2 = [
        TempText.nmid_seller,
        TempText.abc_previous,
        TempText.abc_current,
        TempText.current_price,
        TempText.recommended_price,
        TempText.new_price,
        TempText.reason_price_change
    ]

    sheet.clear()
    data = []

    for ind, per in enumerate(vendor_codes_by_period[:-1], start=1):
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
            row.append('')
            row.append(dict_ABC[ven]['reason'])

            data.append(row)

    for ven, prev_abc, cur_abc, rec_price, reason in zip(
            sheet_ABC.col_values(1)[1:],
            sheet_ABC.col_values(10)[1:],
            sheet_ABC.col_values(11)[1:],
            sheet_ABC.col_values(12)[1:],
            sheet_ABC.col_values(13)[1:],
    ):
        if reason:
            data.append([ven, prev_abc, cur_abc, vendorCode_dict[ven]['price'], rec_price, '', reason])

    sheet.resize(len(data) + 3, len(headers_2))
    sheet.update(range_name='A1', values=[headers_1])
    sheet.update(range_name='A3', values=[headers_2])

    if data:
        sheet.update(range_name='A4', values=data)
    else:
        sheet.update_cell(1, 2, TempText.yes)
        sheet.update_cell(1, 4, TempText.yes)


def send_message_to_queue(message):
    CHANNEL_RABBITMQ.basic_publish(exchange='', routing_key='messages', body=message)


def main(_shop_name):
    get_config_abc()

    nmids, vendorCode_dict, categories_by_period = get_data_from_spreadsheet()
    VENDORCODES_UNION = list(vendorCode_dict.keys())
    vendor_codes_by_period = get_vendor_codes_by_period(nmids)

    categories = calculate_ABC(vendor_codes_by_period, vendorCode_dict, categories_by_period)

    write_categories_to_google_sheet(categories, vendorCode_dict)
    write_recommended_price_to_sheet(vendor_codes_by_period, vendorCode_dict)

    message = get_message(vendor_codes_by_period)
    message = f'<b>{_shop_name}</b>\n{message}'
    send_message_to_queue(message)


def start_dag(_shop_name):
    global FILE, url_to_price_sheet
    if shop_name == 'Tishka':
        KEY = environ['GOOGLESHEET_KEY']
    elif shop_name == 'Future milf':
        KEY = environ['GOOGLESHEET_KEY_FUTURE_MILF']
    FILE = GC.open_by_key(KEY)
    url_to_price_sheet = f'https://docs.google.com/spreadsheets/d/{KEY}'

    main(_shop_name)
    CONNECTION_RABBITMQ.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 27),
}

with DAG(dag_id='send_message_and_calculate_ABC_Tishka',
         schedule='0 7 * * *',
        tags=['Tishka'],
         default_args=default_args) as dag:
    
    shop_name = 'Tishka'

    dice = PythonOperator(
        task_id='send_message_and_calculate_ABC_Tishka',
        python_callable=start_dag,
        op_kwargs={
        '_shop_name':shop_name,
        },
        dag=dag,
        retries=5,
        retry_delay=timedelta(minutes=1),
        max_retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True
    )

with DAG(dag_id='send_message_and_calculate_ABC_Future_Milf',
        schedule='0 7 * * *',
        tags=['Future_milf'],
        default_args=default_args) as dag:
    
    shop_name = 'Future milf'

    dice = PythonOperator(
        task_id='send_message_and_calculate_ABC_Future_Milf',
        python_callable=start_dag,
        op_kwargs={
        '_shop_name':shop_name,
        },
        dag=dag,
        retries=5,
        retry_delay=timedelta(minutes=1),
        max_retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True
    )
