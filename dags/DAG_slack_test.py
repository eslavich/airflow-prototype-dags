from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from datetime import datetime, timedelta

from common import create_slack_operator, DEFAULT_ARGS

dag = DAG("slack-test", default_args=DEFAULT_ARGS, schedule_interval=None)

task = create_slack_operator(
    dag,
    "test-slack-alert",
    "Generated preview image of jw00624012001_02101_00001_nrcalong_rate.fits",
    [{
        "fallback": "Preview image",
        "title": "Preview image",
        "title_link": "https://dmd-test-airflow-prototype-data.s3.amazonaws.com/previews/jw00624012001_02101_00001_nrcalong/jw00624012001_02101_00001_nrcalong_rate.jpg",
        "image_url": "https://dmd-test-airflow-prototype-data.s3.amazonaws.com/previews/jw00624012001_02101_00001_nrcalong/jw00624012001_02101_00001_nrcalong_rate_thumb.jpg"
    }]
)
