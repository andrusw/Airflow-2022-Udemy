Airflow is an orchestrator, in which it allows you to setup how & when the data is downloaded, processed, and then stored.
It makes it easier to manage and monitor multiple pipelines.

Access a docker image/server
docker-compose ps
docker exec -it ..._airflow-scheduler_1 /bin/bash

Testing a task:
airflow tasks test {dag's id} {task's id} {date yyyy-mm-dd}

View Celery with Flower:
docker-compose down 
docker-compose --profile flower up -d
goto: localhost:5555/dashboard



parallelism / AIRFLOW__CORE__PARALELISM

This defines the maximum number of task instances that can run in Airflow per scheduler. By default, you can execute up to 32 tasks at the same time. If you have 2 schedulers: 2 x 32 = 64 tasks.

What value to define here depends on the resources you have and the number of schedulers running.

max_active_tasks_per_dag / AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG

This defines the maximum number of task instances allowed to run concurrently in each DAG. By default, you can execute up to 16 tasks at the same time for a given DAG across all DAG Runs.

max_active_runs_per_dag / AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG

This defines the maximum number of active DAG runs per DAG. By default, you can have up to 16 DAG runs per DAG running at the same time.

Use Elasticsearch:
docker-compose -f docker-compose-es.yaml up -d

to verify elastic search is up and running
docker-compose ps
docker exec -it materials-airflow-scheduler-1 /bin/bash
curl -X GET 'http://elastic:9200'

In Airflow add elastic connection:
Connection Id: elastic_default
Connection Type: HTTP
Host: elastic
Port: 9200

Verify plugin is registered:
docker-compose ps
docker exec -it materials-airflow-scheduler-1 /bin/bash
airflow plugins

name    | hooks                    | source
========+==========================+==============================================
elastic | elastic_hook.ElasticHook | $PLUGINS_FOLDER/hooks/elastic/elastic_hook.py


More info:

* DockerOperator with Templating and Apache Spark: https://marclamberti.com/blog/how-to-use-dockeroperator-apache-airflow/
* Apache Airflow with Kubernetes Executor: https://marclamberti.com/blog/airflow-on-kubernetes-get-started-in-10-mins/
* How to use templates and macros in Apache Airflow: https://marclamberti.com/blog/templates-macros-apache-airflow/
* Variables with apache airflow: https://marclamberti.com/blog/variables-with-apache-airflow/


Apache 2.4:
curl -LFO 'https://airflow.apache.org/docs/apache-airflow/2.4.0/docker-compose.yaml'
docker-compose up -d

Apache 2.5:
curl -LFO 'https://airflow.apache.org/docs/apache-airflow/2.5.0/docker-compose.yaml'
docker-compose up -d


