from airflow import DAG
from airflow import Dataset
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

dag_owner = 'William Andrus'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
        }

my_file = Dataset("/tmp/my_file.txt")
my_file2 = Dataset("/tmp/my_file2.txt")

with DAG(dag_id='consumer',
        default_args=default_args,
        description='',
        start_date=datetime(2023,1,1),
        schedule=[my_file,my_file2],
        catchup=False,
        tags=['udemy']
):

    start = EmptyOperator(task_id='start')

    @task
    def read_dataset():
        with open(my_file.uri, "r") as f:
            print(f.read())

    end = EmptyOperator(task_id='end')

    start >> read_dataset() >> end