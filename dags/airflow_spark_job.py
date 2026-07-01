from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils import timezone
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor

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
    'adhoc_spark_job_serverless',
    default_args=default_args,
    description='Run Spark job on Dataproc Serverless',
    schedule=None,  # Trigger manually or on-demand
    catchup=False,
    tags=['dev'],
)

# Pull values from Airflow Variables
PROJECT_ID = Variable.get("project_id")
REGION = Variable.get("region")
BUCKET = Variable.get("bucket")
SPARK_JOB_PATH = Variable.get("spark_job_path")

# Sensor to check files exist
file_sensor_task = GCSObjectsWithPrefixExistenceSensor(
    task_id='file_sensor_task',
    bucket=BUCKET,
    prefix='data/',
    poke_interval=300,
    timeout=43200,
    mode='poke',
    dag=dag,
)

# Spark job resource parameters
spark_job_resources_parm = {
    'spark.executor.instances': Variable.get("spark.executor.instances"),
    'spark.executor.memory': Variable.get("spark.executor.memory"),
    'spark.executor.cores': Variable.get("spark.executor.cores"),
    'spark.driver.memory': Variable.get("spark.driver.memory"),
    'spark.driver.cores': Variable.get("spark.driver.cores"),
    "spark.bucket.name": Variable.get("bucket"),
    "spark.salary.threshold": Variable.get("salary_threshold"),
}

# Submit job directly to Dataproc Serverless
submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job_serverless',
    job={
        "reference": {"project_id": PROJECT_ID},
        "placement": {"region": REGION},  # serverless placement
        "pyspark_job": {
            "main_python_file_uri": SPARK_JOB_PATH,
            "properties": spark_job_resources_parm,
        },
    },
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

file_sensor_task >> submit_pyspark_job
