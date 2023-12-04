from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.operators.bash import BashOperator
from datetime import datetime

dag_owner = 'William Andrus'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }
 
with DAG('parallel_dag', start_date=datetime(2022, 1, 1), default_args=default_args,
    schedule_interval='@daily', catchup=False,  tags=['udemy']) as dag:
 
    extract_a = BashOperator(
        task_id='extract_a',
        bash_command='sleep 10'
    )
 
    extract_b = BashOperator(
        task_id='extract_b',
        bash_command='sleep 10'
    )
 
    load_a = BashOperator(
        task_id='load_a',
        bash_command='sleep 10'
    )
 
    load_b = BashOperator(
        task_id='load_b',
        bash_command='sleep 10'
    )
 
    transform = BashOperator(
        task_id='transform',
        queue='high_cpu',
        bash_command='sleep 30'
    )
 
    extract_a >> load_a
    extract_b >> load_b
    [load_a, load_b] >> transform