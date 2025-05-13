from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = "airy-actor-457907-a8"
REGION = "us-central1"
CLUSTER_NAME = "demo-cluster"
BUCKET_NAME = "sample-data-for-pract1"
PYSPARK_URI = f"gs://{BUCKET_NAME}/main_job.py"
BIGQUERY_CONNECTOR_JAR = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar"

with models.DAG(
    "dataproc_pyspark_bq_dag",
    schedule_interval=None,
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=["example", "dataproc"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config={
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
            "software_config": {
                "image_version": "2.1-debian11",
                "optional_components": ["ANACONDA", "JUPYTER"],
            },
            "gce_cluster_config": {
                "service_account": "963159824406-compute@developer.gserviceaccount.com",
                "zone_uri": "us-central1-a",
            },
        },
    )

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_URI,
                "jar_file_uris": [BIGQUERY_CONNECTOR_JAR],
            },
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> submit_pyspark_job >> delete_cluster
