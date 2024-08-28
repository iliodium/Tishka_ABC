import os

import gspread
from airflow.models import DAG
from airflow.operators.python import PythonOperator

KEY_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), './../abc-dev-415713-a2a407ac569b.json')

GC = gspread.service_account(KEY_PATH)
KEY = None
FILE = None


def accept_changes():
    sheet = FILE.worksheet("Загрузка цены")
    sheet.update_cell(1, 2, 'Да')


def main(shop_name):
    global FILE, url_to_price_sheet
    if shop_name == 'Tishka':
        KEY = os.environ['GOOGLESHEET_KEY']
    elif shop_name == 'Future milf':
        KEY = os.environ['GOOGLESHEET_KEY_FUTURE_MILF']
    FILE = GC.open_by_key(KEY)
    url_to_price_sheet = f'https://docs.google.com/spreadsheets/d/{KEY}'

    accept_changes()


default_args = {
    'owner': 'airflow',
}
with DAG(dag_id='accept_price_Tishka',
         schedule=None,
         default_args=default_args,
        tags=['Tishka']
        ) as dag:
    shop_name = 'Tishka'

    dice = PythonOperator(
        task_id='accept_price',
        python_callable=main,
        op_kwargs={
                'shop_name':shop_name,

        },
        dag=dag)
    
with DAG(dag_id='accept_price_Future_Milf',
         schedule=None,
         default_args=default_args,
        tags=['Future_milf']
        ) as dag:
    shop_name = 'Future milf'

    dice = PythonOperator(
        task_id='accept_price',
        python_callable=main,
        op_kwargs={
                'shop_name':shop_name,
        },
        dag=dag)

if __name__ == '__main__':
    main()
