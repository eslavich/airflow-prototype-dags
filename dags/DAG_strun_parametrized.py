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
    "retry_delay": timedelta(seconds=30)
}

dag = DAG("strun-parametrized", default_args=default_args, schedule_interval=None)

affinity = {
    "nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{
                "matchExpressions": [{
                    "key": "node-type",
                    "operator": "In",
                    "values": ["worker"]
                }]
            }]
        }
    }
}

tolerations = [
    {
        "key": "node-type",
        "operator": "Equal",
        "value": "worker"
     }
]

op = KubernetesPodOperator(
    namespace="default",
    image="162808325377.dkr.ecr.us-east-1.amazonaws.com/airflow-prototype:latest",
    arguments=[
        "strun",
        "jwst.pipeline.Detector1Pipeline",
        "{{ dag_run.conf['input_uri'] }}",
        "--output_dir", "{{ dag_run.conf['output_uri_prefix'] }}",
    ],
    name="strun",
    task_id="strun-parametrized",
    env_vars={
        "AWS_DEFAULT_REGION": "us-east-1"
    },
    resources={
        "request_memory": 12884901888, # 12 GB
        "request_cpu": 2,
        "limit_memory": 15032385536, # 14 GB
        "limit_cpu": 3
    },
    startup_timeout_seconds=600,
    get_logs=True,
    is_delete_operator_pod=True,
    affinity=affinity,
    tolerations=tolerations,
    dag=dag
)
