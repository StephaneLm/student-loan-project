from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1
}

with DAG(
    dag_id="student_loan_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    stream_job = DockerOperator(
        task_id="spark_streaming",
        image="bitnami/spark:3.5.1",
        command="spark-submit /opt/bitnami/python/bin/spark_streaming_job.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            "/mnt/data/student-loan-project/streaming:/opt/bitnami/python/bin"
        ]
    )

    batch_job = DockerOperator(
        task_id="spark_batch_metrics",
        image="bitnami/spark:3.5.1",
        command="spark-submit /opt/bitnami/python/bin/spark_batch_metrics.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            "/mnt/data/student-loan-project/batch:/opt/bitnami/python/bin"
        ]
    )

    stream_job >> batch_job
