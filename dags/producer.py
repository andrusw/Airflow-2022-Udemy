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

with DAG(dag_id='producer',
        default_args=default_args,
        description='',
        start_date=datetime(2023,1,1),
        schedule='@daily',
        catchup=False,
        tags=['udemy']
):

    start = EmptyOperator(task_id='start')

    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, "a+") as f:
            f.write("producer update file1")

    @task(outlets=[my_file2])
    def update_dataset2():
        with open(my_file2.uri, "a+") as f:
            f.write("producer update file2")


    end = EmptyOperator(task_id='end')

    start >> update_dataset() >> update_dataset2() >> end