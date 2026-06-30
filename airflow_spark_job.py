from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils import timezone
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': timezone.datetime(2026, 6, 28),
}

dag = DAG(
    'adhoc_spark_job_on_dataproc',
    default_args=default_args,
    description='A DAG to setup dataproc and run Spark job on that',
    schedule=None,  # Trigger manually or on-demand
    catchup=False,
    tags=['dev'],
)

# Pull values from Airflow Variables
PROJECT_ID = Variable.get("project_id")
REGION = Variable.get("region")
BUCKET = Variable.get("bucket")
CLUSTER_NAME = Variable.get("cluster_name")
SPARK_JOB_PATH = Variable.get("spark_job_path")

# Define cluster config (can also be moved to variables.yaml if desired)
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'worker_config': {
        'num_instances': 2,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'software_config': {
        'image_version': '2.2.26-debian12'
    }
}

create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
)

# Spark job resource parameters from Variables
spark_job_resources_parm = {
    'spark.executor.instances': Variable.get("spark.executor.instances"),
    'spark.executor.memory': Variable.get("spark.executor.memory"),
    'spark.executor.cores': Variable.get("spark.executor.cores"),
    'spark.driver.memory': Variable.get("spark.driver.memory"),
    'spark.driver.cores': Variable.get("spark.driver.cores"),
}

submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job_on_dataproc',
    job={
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": SPARK_JOB_PATH,
            "properties": spark_job_resources_parm,
        },
    },
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',
    dag=dag,
)

create_cluster >> submit_pyspark_job >> delete_cluster
