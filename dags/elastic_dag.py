from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.elastic.elastic_hook import ElasticHook

 
from datetime import datetime, timedelta

dag_owner = 'William Andrus'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }
 
def _print_es_info():
    hook = ElasticHook()
    print(hook.info())
 
with DAG('elastic_dag', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False, tags=['udemy'], default_args=default_args) as dag:
 
    print_es_info = PythonOperator(
        task_id='print_es_info',
        python_callable=_print_es_info
    )