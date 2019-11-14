from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=15)
}

dag = DAG("cowsay-test", default_args=default_args, schedule_interval=None)

dummy_operator = DummyOperator(task_id='dummy_task', dag=dag)

op = KubernetesPodOperator(namespace="default",
                           image="162808325377.dkr.ecr.us-east-1.amazonaws.com/airflow-prototype:latest",
                           cmds=["cowsay"],
                           arguments=["hello kubernetes!"],
                           name="cowsay-test",
                           task_id="cowsay-task",
                           get_logs=True,
                           is_delete_operator_pod=True,
                           dag=dag
)

dummy_operator >> op
