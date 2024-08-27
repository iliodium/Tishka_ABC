import os
import sys
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from parsing_mpstats import main as main_mpstats
from parsing_wb import main as main_wb


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 27),
}

def main(key, token_mpstats, token_wb, shop_name):
    parsing_mpstats = PythonOperator(
        task_id='parsing_mpstats',
        python_callable=main_mpstats,
        op_kwargs={
        '_key': key,
        '_token_mpstats': token_mpstats,
        '_shop_name':shop_name,
        },
        dag=dag,
        retries=3,
        retry_delay=timedelta(seconds=30),
        max_retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True
    )


    def get_nmids_and_run_parsing_wb(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids='parsing_mpstats')
        return main_wb(nmids=result['nmids'],
                       _key=key,
                       _token_wb=token_wb,
                       _shop_name=shop_name)


    parsing_wb = PythonOperator(
        task_id='parsing_wb',
        python_callable=get_nmids_and_run_parsing_wb,
        dag=dag,
        retries=3,
        retry_delay=timedelta(seconds=30),
        max_retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True,
        provide_context=True,
    )

    return parsing_mpstats, parsing_wb

with DAG(dag_id='parsing_wb_mpstats_Tishka',
         schedule='0 6 * * *',
         default_args=default_args,
         ) as dag:

    key = os.environ['GOOGLESHEET_KEY']
    token_mpstats=os.environ['TOKEN_MPSTATS']
    token_wb=os.environ['TOKEN_WB']
    shop_name = 'Tishka'

    parsing_mpstats, parsing_wb = main(key, token_mpstats, token_wb, shop_name)

    parsing_mpstats >> parsing_wb

with DAG(dag_id='parsing_wb_mpstats_Future_milf',
         schedule='0 6 * * *',
         default_args=default_args,
         ) as dag:

    key = os.environ['GOOGLESHEET_KEY_FUTURE_MILF']
    token_mpstats=os.environ['TOKEN_MPSTATS_FUTURE_MILF']
    token_wb=os.environ['TOKEN_WB_FUTURE_MILF']
    shop_name = 'Future milf'

    parsing_mpstats, parsing_wb = main(key, token_mpstats, token_wb, shop_name)

    parsing_mpstats >> parsing_wb