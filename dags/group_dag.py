from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdags_downloads import subdag_downloads
from subdags.subdags_transforms import subdag_transforms


dag_owner = 'William Andrus'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }


with DAG('group_dag', start_date=datetime(2023, 1, 1), default_args=default_args, tags=['udemy'],
    schedule_interval='@daily', catchup=False) as dag:

    args = {'start_date': dag.start_date, 'schedule_interval':dag.schedule_interval,'catchup':dag.catchup}
 
    downloads = SubDagOperator(
        task_id='downloads',
        subdag=subdag_downloads(dag.dag_id,'downloads',args)
    )

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transforms = SubDagOperator(
        task_id='transforms',
        subdag=subdag_transforms(dag.dag_id,'transforms',args)
    )
 

 
    [downloads] >> check_files >> [transforms]