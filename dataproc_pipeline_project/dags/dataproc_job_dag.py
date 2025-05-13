from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago
from datetime import timedelta

PROJECT_ID = "airy-actor-457907-a8"
REGION = "us-central1"
CLUSTER_NAME = "demo-cluster"
BUCKET_NAME = "sample-data-for-pract1"
BQ_TEMP_BUCKET = "sample-data-for-pract1/temp"

# ✅ Updated to single-node cluster using n1-standard-2
# CLUSTER_CONFIG = {
#     "master_config": {
#         "num_instances": 1,
#         "machine_type_uri": "n1-standard-2"
#     },
#     "worker_config": {
#         "num_instances": 0
#     }
# }

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-medium",  # you can try e2-small or e2-micro
        "disk_config": {
            "boot_disk_type": "pd-balanced",
            "boot_disk_size_gb": 50,
        },
    },
    "software_config": {
        "image_version": "2.1-debian11",
        "optional_components": ["ANACONDA", "JUPYTER"],
    },
    "single_node": True
}




PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET_NAME}/spark_jobs/main_job.py",
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
    },
}

# ✅ Disabled email alert to avoid `smtp_default` missing error
default_args = {
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False
}

with DAG(
    "dataproc_cluster_job_lifecycle",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done"
    )

    create_cluster >> submit_job >> delete_cluster
