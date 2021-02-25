import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from custom_operator.convert_ndjson_to_csv import ConvertNdJsonToCsv
from src.clean_data import run as run_cleanup_data
from src.binary_tree import run as run_binary
ENV = os.getenv('BRANCH_CODE')

dag = DAG(
    dag_id='dag_result',
    default_args={
        'owner': 'Brilliant Subaweh',
        'depends_on_past': False,
        'email': ['baweh.08@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2022, 2, 24),
    schedule_interval='0 0 * * *', # in WIB: 7 on everyday
    max_active_runs=1)


######################################
# Number 1
######################################

convert_ndsjon = ConvertNdJsonToCsv(
    task_id='convert_ndjson',
    url_source='https://storage.googleapis.com/andika_dev/q1_data_source.json',
    target_directory='result_1/csv_file',
    dag=dag
)


######################################
# Number 2
######################################

cleanup_data = PythonOperator(
    task_id='cleanup_data',
    python_callable=run_cleanup_data,
    params={
        'url': 'https://storage.googleapis.com/andika_dev/q2_dataset.zip',
    },
    provide_context=True,
    dag=dag
)

######################################
# Number 4
######################################

binary_tree = PythonOperator(
    task_id='binary_tree',
    python_callable=run_binary,
    provide_context=True,
    dag=dag
)
