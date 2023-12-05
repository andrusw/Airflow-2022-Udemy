from airflow.decorators import task, dag
from airflow.sensors.base import PokeReturnValue

from datatime import datetime
from time import sleep

@dag(start_date=datetime(2023,1,1), schedule="@once", catchup=False)
def sensor_dag():

    @task
    def dummy_task(input: int):
        return input

    @task.sensor(poke_interval=60, timeout=3600, mode='poke')
    def check_dummy(input: int):
        sleep(5)
        ret = input == 100
        return PokeReturnValue(is_done=ret, xcom_value=f"Input from sensor {ret}")

    check_dummy(dummy_task(100))

sensor_dag()
