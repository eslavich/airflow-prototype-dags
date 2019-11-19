from datetime import datetime, timedelta

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}

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

def create_slack_operator(dag, task_id, message, attachments=None):
    return SlackWebhookOperator(
        task_id=task_id,
        http_conn_id="slack-alerts",
        message=message,
        attachments=attachments,
        dag=dag
    )