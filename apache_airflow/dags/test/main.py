import os
import sys

from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 20),
}

with DAG(dag_id='test_dag_main',
         schedule='0 7 * * *',
         default_args=default_args) as dag:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from file1 import main as m1

    # task1 = PythonOperator(
    #     task_id="1",
    #     python_callable=m1,
    #     dag=dag,
    #     op_kwargs={'message': '1'}
    # )
    # task2 = PythonOperator(
    #     task_id="2",
    #     python_callable=m1,
    #     dag=dag,
    #     op_kwargs={'message': '2'}
    # )
    # task3 = PythonOperator(
    #     task_id="3",
    #     python_callable=m1,
    #     dag=dag,
    #     op_kwargs={'message': '3'}
    # )
    # task4 = PythonOperator(
    #     task_id="4",
    #     python_callable=m1,
    #     dag=dag,
    #     op_kwargs={'message': '4'}
    # )
    # task5 = PythonOperator(
    #     task_id="5",
    #     python_callable=m1,
    #     dag=dag,
    #     op_kwargs={'message': '5'}
    # )
    task1 = PythonOperator(
        task_id="1",
        python_callable=m1,
        dag=dag,
        op_kwargs={'message': '1'}
    )


    def get_task1(**kwargs):
        # Продолжить выполнение task2
        ti = kwargs['ti']
        task1_result = ti.xcom_pull(task_ids='1')
        print(f"Result from task1: {task1_result}")
        return m1(message=task1_result['message'])


    task2 = PythonOperator(
        task_id="2",
        python_callable=get_task1,
        dag=dag,
        op_kwargs={'message': '2'},
        provide_context=True,
    )

    '''[task1, task2] >> task3 >> [task4, task5]'''
    task1 >> task2
'''
{'conf': <***.configuration.AirflowConfigParser object at 0x7f7d2907dc40>, 'dag': <DAG: test_dag_main>, 'dag_run': <DagRun test_dag_main @ 2024-05-24 20:20:28.775089+00:00: manual__2024-05-24T20:20:28.775089+00:00, state:running, queued_at: 2024-05-24 20:20:28.782244+00:00. externally triggered: True>, 'data_interval_end': DateTime(2024, 5, 24, 7, 0, 0, tzinfo=Timezone('UTC')), 'data_interval_start': DateTime(2024, 5, 23, 7, 0, 0, tzinfo=Timezone('UTC')), 'ds': '2024-05-24', 'ds_nodash': '20240524', 'execution_date': <Proxy at 0x7f7d1f5cbc00 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f7d1f5e7560>, 'execution_date', DateTime(2024, 5, 24, 20, 20, 28, 775089, tzinfo=Timezone('UTC')))>, 'expanded_ti_count': None, 'inlets': [], 'logical_date': DateTime(2024, 5, 24, 20, 20, 28, 775089, tzinfo=Timezone('UTC')), 'macros': <module '***.macros' from '/home/***/.local/lib/python3.12/site-packages/***/macros/__init__.py'>, 'map_index_template': None, 'next_ds': <Proxy at 0x7f7d1f433d40 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f7d1f5e7560>, 'next_ds', '2024-05-24')>, 'next_ds_nodash': <Proxy at 0x7f7d1f5cc200 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f7d1f5e7560>, 'next_ds_nodash', '20240524')>, 'next_execution_date': <Proxy at 0x7f7d1f44eb00 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f7d1f5e7560>, 'next_execution_date', DateTime(2024, 5, 24, 20, 20, 28, 775089, tzinfo=Timezone('UTC')))>, 'outlets': [], 'params': {}, 'prev_data_interval_start_success': DateTime(2024, 5, 23, 7, 0, 0, tzinfo=Timezone('UTC')), 'prev_data_interval_end_success': DateTime(2024, 5, 24, 7, 0, 0, tzinfo=Timezone('UTC')), 'prev_ds': <Proxy at 0x7f7d1f44ebc0 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f7d1f5e7560>, 'prev_ds', '2024-05-24')>, 'prev_ds_nodash': <Proxy at 0x7f7d1f47a500 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f7d1f5e7560>, 'prev_ds_nodash', '20240524')>, 'prev_execution_date': <Proxy at 0x7f7d1f478380 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f7d1f5e7560>, 'prev_execution_date', DateTime(2024, 5, 24, 20, 20, 28, 775089, tzinfo=Timezone('UTC')))>, 'prev_execution_date_success': <Proxy at 0x7f7d1f475c80 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f7d1f5e7560>, 'prev_execution_date_success', DateTime(2024, 5, 24, 20, 19, 46, 737991, tzinfo=Timezone('UTC')))>, 'prev_start_date_success': DateTime(2024, 5, 24, 20, 19, 47, 139521, tzinfo=Timezone('UTC')), 'prev_end_date_success': DateTime(2024, 5, 24, 20, 19, 49, 425792, tzinfo=Timezone('UTC')), 'run_id': 'manual__2024-05-24T20:20:28.775089+00:00', 'task': <Task(PythonOperator): 2>, 'task_instance': <TaskInstance: test_dag_main.2 manual__2024-05-24T20:20:28.775089+00:00 [running]>, 'task_instance_key_str': 'test_dag_main__2__20240524', 'test_mode': False, 'ti': <TaskInstance: test_dag_main.2 manual__2024-05-24T20:20:28.775089+00:00 [running]>, 'tomorrow_ds': <Proxy at 0x7f7d1f475040 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f7d1f5e7560>, 'tomorrow_ds', '2024-05-25')>, 'tomorrow_ds_nodash': <Proxy at 0x7f7d1f477080 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f7d1f5e7560>, 'tomorrow_ds_nodash', '20240525')>, 'triggering_dataset_events': <Proxy at 0x7f7d1f4027c0 with factory <function _get_template_context.<locals>.get_triggering_events at 0x7f7d1f5e4040>>, 'ts': '2024-05-24T20:20:28.775089+00:00', 'ts_nodash': '20240524T202028', 'ts_nodash_with_tz': '20240524T202028.775089+0000', 'var': {'json': None, 'value': None}, 'conn': None, 'yesterday_ds': <Proxy at 0x7f7d1f476fc0 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f7d1f5e7560>, 'yesterday_ds', '2024-05-23')>, 'yesterday_ds_nodash': <Proxy at 0x7f7d1f476f00 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7f7d1f5e7560>, 'yesterday_ds_nodash', '20240523')>, 'message': '2', 'templates_dict': None}
'''
