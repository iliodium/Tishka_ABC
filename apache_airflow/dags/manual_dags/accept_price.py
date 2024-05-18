import os

import gspread
from airflow.models import DAG
from airflow.operators.python import PythonOperator

KEY_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), './../abc-dev-415713-a2a407ac569b.json')
KEY = os.environ['GOOGLESHEET_KEY']

GC = gspread.service_account(KEY_PATH)
FILE = GC.open_by_key(KEY)


def accept_changes():
    sheet = FILE.worksheet("Загрузка цены")
    sheet.update_cell(1, 2, 'Да')


def main():
    accept_changes()


def start_dag():
    main()


default_args = {
    'owner': 'airflow',
}

with DAG(dag_id='accept_price',
         schedule=None,
         default_args=default_args) as dag:
    dice = PythonOperator(
        task_id='accept_price',
        python_callable=start_dag,
        dag=dag)

if __name__ == '__main__':
    start_dag()
