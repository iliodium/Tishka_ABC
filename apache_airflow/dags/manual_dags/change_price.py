import os
import time
from datetime import datetime

import gspread
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator

KEY_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), './../abc-dev-415713-a2a407ac569b.json')

KEY = os.environ['GOOGLESHEET_KEY']

GC = gspread.service_account(KEY_PATH)
FILE = GC.open_by_key(KEY)

token_wb_price = os.environ['TOKEN_WB_PRICE']

url_price = 'https://discounts-prices-api.wb.ru/api/v2/upload/task'

headers_temp_wb = lambda token: {
    "Content-Type": "application/json",
    "Authorization": token,
}

request_body_temp = {
    "data": [
        # {
        #     "nmID": 123,
        #     "price": 999,
        #     "discount": 30
        # }
    ]
}


def change_price(nmid_price: dict):
    if nmid_price:
        headers = headers_temp_wb(token_wb_price)

        request_body = request_body_temp.copy()
        for nmid, price in nmid_price.items():
            request_body['data'].append(
                {
                    "nmID": nmid,
                    "price": price * 2,
                    "discount": 50
                }
            )

        request = requests.post(url_price, headers=headers, json=request_body)
        while request.status_code in (401, 429, 500):
            request = requests.post(url_price, headers=headers, json=request_body)
            time.sleep(0.2),


def main():
    sheet_wb = FILE.worksheet("Выгрузка К")
    sheet = FILE.worksheet("Загрузка цены")

    ven_price = {
        ven: price for ven, price in zip(sheet.col_values(1)[3:], sheet.col_values(6)[3:])
        if str(price).isdigit() and ven
    }

    ven_nmid = {
        ven: nmid for ven, nmid in zip(sheet_wb.col_values(1)[1:], sheet_wb.col_values(2)[1:])
    }

    nmid_price = {
        int(ven_nmid[ven]): int(ven_price[ven]) for ven in ven_price
    }

    change_price(nmid_price)

    sheet.update_cell(1, 4, 'Да')


def start_dag():
    main()


default_args = {
    'owner': 'airflow',
}

with DAG(dag_id='change_price',
         schedule=None,
         default_args=default_args) as dag:
    dice = PythonOperator(
        task_id='change_price',
        python_callable=start_dag,
        dag=dag)

if __name__ == '__main__':
    start_dag()
