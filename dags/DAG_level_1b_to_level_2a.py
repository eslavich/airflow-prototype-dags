from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

AFFINITY = {
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

TOLERATIONS = [
    {
        "key": "node-type",
        "operator": "Equal",
        "value": "worker"
    }
]

NAMESPACE = "default"
IMAGE = "162808325377.dkr.ecr.us-east-1.amazonaws.com/airflow-prototype:latest"
ENV_VARS = {
    "AWS_DEFAULT_REGION": "us-east-1"
}
STARTUP_TIMEOUT_SECONDS = 1800

def create_pod_operator(dag, arguments, name, task_id, request_memory, request_cpu, limit_memory, limit_cpu):
    return KubernetesPodOperator(
        namespace=NAMESPACE,
        image=IMAGE,
        arguments=arguments,
        name=name,
        task_id=task_id,
        env_vars=ENV_VARS,
        resources={
            "request_memory": request_memory,
            "request_cpu": request_cpu,
            "limit_memory": limit_memory,
            "limit_cpu": limit_cpu
        },
        startup_timeout_seconds=STARTUP_TIMEOUT_SECONDS,
        get_logs=True,
        is_delete_operator_pod=True,
        affinity=AFFINITY,
        tolerations=TOLERATIONS,
        dag=dag
    )

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

dag = DAG("level-1b-to-level-2a", default_args=default_args, schedule_interval=None)

preview_1b_task = create_pod_operator(
    dag,
    [
        "create_previews",
        "{{ dag_run.conf['input_uri'] }}",
        "{{ dag_run.conf['preview_uri_prefix'] }}"
    ],
    "preview-1b",
    "preview-1b",
    1073741824,
    1.0,
    1073741824,
    1.0
)

level_2a_task = create_pod_operator(
    dag,
    [
        "strun",
        "jwst.pipeline.Detector1Pipeline",
        "{{ dag_run.conf['input_uri'] }}",
        "--output_dir", "{{ dag_run.conf['output_uri_prefix'] }}",
    ],
    "level-2a",
    "level-2a",
    12884901888, # 12 GB
    1.5,
    15032385536, # 14 GB,
    3
)

preview_2a_task = create_pod_operator(
    dag,
    [
        "create_previews",
        "{{ dag_run.conf['output_uri_prefix'] }}",
        "{{ dag_run.conf['preview_uri_prefix'] }}"
    ],
    "preview-2a",
    "preview-2a",
    1073741824,
    1.0,
    1073741824,
    1.0
)

level_2a_task >> preview_2a_task
