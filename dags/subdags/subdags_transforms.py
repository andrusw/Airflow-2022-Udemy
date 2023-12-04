from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.operators.bash import BashOperator

dag_owner = 'William Andrus'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

def subdag_transforms(parent_dag_id, child_dag_id, args):
    with DAG(f"{parent_dag_id}.{child_dag_id}",
        default_args=default_args,
        start_date=args['start_date'],
        schedule_interval=args['schedule_interval'],
        catchup=args['catchup'],
        tags=['udemy']) as dag:

        transform_a = BashOperator(
            task_id='transform_a',
            bash_command='sleep 10'
        )
    
        transform_b = BashOperator(
            task_id='transform_b',
            bash_command='sleep 10'
        )
    
        transform_c = BashOperator(
            task_id='transform_c',
            bash_command='sleep 10'
        )

    return dag