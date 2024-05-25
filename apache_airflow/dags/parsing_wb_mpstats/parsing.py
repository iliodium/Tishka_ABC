import os
import sys
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 24),
}

with DAG(dag_id='parsing_wb_mpstats',
         schedule='0 6 * * *',
         default_args=default_args,
         ) as dag:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from parsing_mpstats import main as main_mpstats
    from parsing_wb import main as main_wb

    parsing_mpstats = PythonOperator(
        task_id='parsing_mpstats',
        python_callable=main_mpstats,
        dag=dag,
        retries=3,
        retry_delay=timedelta(seconds=30),
        max_retry_delay=timedelta(minutes=3),
        retry_exponential_backoff=True
    )


    def get_nmids_and_run_parsing_wb(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids='parsing_mpstats')

        return main_wb(nmids=result['nmids'])


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

    parsing_mpstats >> parsing_wb
