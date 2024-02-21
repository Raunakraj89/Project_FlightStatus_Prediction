from datetime import timedelta, datetime
from airflow import DAG
from airflow import models
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

CLUSTER_NAME = 'dataproc-airflow-cluster'
REGION = 'us-central1'
PROJECT_ID = 'eng-origin-408719'
jar_path = 'gs://bqdata12/spark-3.1-bigquery-0.36.1.jar'
project_gcs_path = 'gs://pipeline_scripts'
Ingest_table = 'emp'

Ingest = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": project_gcs_path+"/mainingestwithbq.py",
         "jar_file_uris": [jar_path],
         "args": [Ingest_table, project_gcs_path+"landingdata"],
         "python_file_uris": [project_gcs_path+"/ingestionbq.py"],
     }
}
DataCleaning = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": project_gcs_path+"maincleaningwithbq.py",
         "jar_file_uris": [jar_path],
         "args": ["gs://bqdata12/landingdata/*.csv", "stg_emp"],
         "python_file_uris": ["gs://bqdata12/datacleaning.py"],
     }
}
DataEnrich = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": "gs://bqdata12/mainenrichmentwithbq.py",
         "jar_file_uris": [jar_path],
         "args": ["stg_emp", "enrich_emp"],
         "python_file_uris": ["gs://bqdata12/dataenrich.py"],
     }
}
DataQualitycheck = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": "gs://bqdata12/enrichdqcheck.py",
         "jar_file_uris": [jar_path],
         "args": ["stg_emp", "enrich_emp"],
         "python_file_uris": ["gs://bqdata12/enrichdqcheck.py"],
     }
}
ConsumeData = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": "gs://bqdata12/maincalculatesummary.py",
         "jar_file_uris": [jar_path],
         "args": ["enrich_emp", "consume_emp"],
         "python_file_uris": ["gs://bqdata12/calculatesummary.py"],
     }
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    }
}

# Define default_args dictionary
default_dag_args = {
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'tags': ["dataproc_airflow"],
}

with models.DAG(
    "dataproc_airflow_gcp",
    schedule_interval=None,
    default_args=default_dag_args,
    tags=["dataproc_airflow"],
) as dag:


    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        dag=dag,
    )
    ingest = DataprocSubmitJobOperator(
        task_id='ingest',
        project_id=PROJECT_ID,
        region=REGION,
        job=Ingest,
        dag=dag,

    )
    dataCleaning = DataprocSubmitJobOperator(
        task_id='dataCleaning',
        project_id=PROJECT_ID,
        region=REGION,
        job=DataCleaning,
        dag=dag,

    )
    dataEnrich = DataprocSubmitJobOperator(
        task_id='dataEnrich',
        project_id=PROJECT_ID,
        region=REGION,
        job=DataEnrich,
        dag=dag,

    )
    dataQualitycheck = DataprocSubmitJobOperator(
        task_id='dataQualitycheck',
        project_id=PROJECT_ID,
        region=REGION,
        job=DataQualitycheck,
        dag=dag,

    )
    consumeData = DataprocSubmitJobOperator(
        task_id='consumeData',
        project_id=PROJECT_ID,
        region=REGION,
        job=ConsumeData,
        dag=dag,

    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        dag=dag,

    )
  
create_cluster >> ingest >> dataCleaning >> dataEnrich >>dataQualitycheck >>consumeData >>delete_cluster
