from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json    
from airflow.operators.python import PythonOperator
from pandas import json_normalize
from airflow.providers.postgres.hooks.postgres import PostgresHook
    
dag_owner = 'William Andrus'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
        }

def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user") 
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']})
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)

def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )

with DAG(dag_id='user_processing',
        default_args=default_args,
        description='',
        start_date=datetime(2023,1,1),
        schedule='@daily',
        catchup=False,
        tags=['udemy']
) as dag:

    start = EmptyOperator(task_id='start')

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        ''')

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        request_params=None,
        headers=None,
        response_check=None,
        extra_options=None,
        tcp_keep_alive=True,
        tcp_keep_alive_idle=120,
        tcp_keep_alive_count=20,
        tcp_keep_alive_interval=30,
        )

    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        endpoint='api/',
        method='GET',
        data=None,
        headers=None,
        response_check=None,
        response_filter=lambda response: json.loads(response.text),
        extra_options=None,
        http_conn_id='user_api',
        log_response=True,
        tcp_keep_alive=True,
        tcp_keep_alive_idle=120,
        tcp_keep_alive_count=20,
        tcp_keep_alive_interval=30,
    )

    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user,
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user
    )

    end = EmptyOperator(task_id='end')

    start >> create_table >> is_api_available >> extract_user >> process_user >> store_user >> end