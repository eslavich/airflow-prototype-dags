from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common import create_pod_operator, create_slack_operator, DEFAULT_ARGS

dag = DAG("level-1b-to-level-2a", default_args=DEFAULT_ARGS, schedule_interval=None)

def log_dataset(*args, **kwargs):
    print(f"Received { kwargs['dag_run'].conf['dataset_id'] } for processing")

log_dataset_task = PythonOperator(
    task_id="log-dataset",
    provide_context=True,
    python_callable=log_dataset,
    dag=dag
)

ingress_alert_task = create_slack_operator(
    dag,
    "ingress-slack-alert",
    "Received {{ dag_run.conf['dataset_id'] }} for processing"
)

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

preview_1b_alert_task = create_slack_operator(
    dag,
    "preview-1b-slack-alert",
    "Generated preview image of {{ dag_run.conf['input_uri'].split('/')[-1] }}",
    [{
        "fallback": "Preview image",
        "title": "Preview image",
        "title_link": "https://dmd-test-airflow-prototype-data.s3.amazonaws.com/previews/{{ dag_run.conf['dataset_id'] }}/{{ dag_run.conf['dataset_id'] + '_uncal.jpg' }}",
        "image_url": "https://dmd-test-airflow-prototype-data.s3.amazonaws.com/previews/{{ dag_run.conf['dataset_id'] }}/{{ dag_run.conf['dataset_id'] + '_uncal_thumb.jpg' }}"
    }]
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
    21474836480,
#    12884901888, # 12 GB
    1.5,
    21474836480,
#    15032385536, # 14 GB,
    3
)

level_2a_alert_task = create_slack_operator(
    dag,
    "level-2a-slack-alert",
    "Completed level 2a processing of {{ dag_run.conf['dataset_id'] }}",
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

preview_2a_alert_task = create_slack_operator(
    dag,
    "preview-2a-slack-alert",
    "Generated preview image of {{ dag_run.conf['dataset_id'] + '_rate.fits' }}",
    [{
        "fallback": "Preview image",
        "title": "Preview image",
        "title_link": "https://dmd-test-airflow-prototype-data.s3.amazonaws.com/previews/{{ dag_run.conf['dataset_id'] }}/{{ dag_run.conf['dataset_id'] + '_rate.jpg' }}",
        "image_url": "https://dmd-test-airflow-prototype-data.s3.amazonaws.com/previews/{{ dag_run.conf['dataset_id'] }}/{{ dag_run.conf['dataset_id'] + '_rate_thumb.jpg' }}"
    }]
)

ingress_alert_task >> [preview_1b_task, level_2a_task]
preview_1b_task >> [preview_1b_alert_task]
level_2a_task >> [level_2a_alert_task, preview_2a_task]
preview_2a_task >> preview_2a_alert_task


