from airflow import DAG
from datetime import datetime, timedelta

from common import create_pod_operator

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
