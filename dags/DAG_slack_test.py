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
    message="Test message",
    dag=dag,
    attachments=[{
        "image_url": "https://dmd-test-airflow-prototype-data.s3.amazonaws.com/previews/jw00624012001_02101_00001_nrcalong/jw00624012001_02101_00001_nrcalong_rate.jpg"
    }]
)
