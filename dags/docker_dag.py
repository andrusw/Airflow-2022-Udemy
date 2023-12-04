from airflow.decorators import task
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount   
from datetime import datetime, timedelta 

dag_owner = 'William Andrus'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
        }

with DAG('docker_dag', start_date=datetime(2023, 1, 1), default_args=default_args, tags=['udemy'],
    schedule_interval='@daily', catchup=False) as dag:

    start = EmptyOperator(task_id='start')

    @task
    def task_1():
        return ''

    DockerOperator_task = DockerOperator(
        task_id='DockerOperator_task',
        image='stock_image:v1.0.0',
        api_version=None,
        command='echo "command running in the docker container"',
        container_name='DockerOperator_task',
        cpus=1.0,
        docker_url='unix://var/run/docker.sock',
        environment=None,
        private_environment=None,
        force_pull=False,
        mem_limit='512m',
        host_tmp_dir=None,
        network_mode='bridge',
        tls_ca_cert=None,
        tls_client_cert=None,
        tls_client_key=None,
        tls_hostname=None,
        tls_ssl_version=None,
        mount_tmp_dir=True,
        tmp_dir='/tmp/airflow',
        user=None,
        entrypoint=None,
        working_dir=None,
        xcom_all=True,
        docker_conn_id=None,
        dns=None,
        dns_search=None,
        auto_remove=True,
        shm_size=None,
        tty=False,
        privileged=False,
        cap_add=None,
        extra_hosts=None,
        retrieve_output=True,
        retrieve_output_path='/tmp/script.out',
        timeout=60,
        device_requests=None,
        log_opts_max_size=None,
        log_opts_max_file=None,
    )    

    end = EmptyOperator(task_id='end')

    start >> DockerOperator_task >> end