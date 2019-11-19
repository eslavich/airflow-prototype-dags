from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
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

dag = DAG("slack-test", default_args=default_args, schedule_interval=None)

task = SlackWebhookOperator(
    task_id="test-slack-alert",
    http_conn_id="slack-alerts",
    message="testing...",
    username="airflow",
    icon_url="https://raw.githubusercontent.com/apache/airflow/master/airflow/www/static/pin_100.png",
    dag=dag
)