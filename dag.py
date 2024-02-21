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

CLUSTER_NAME = 'emp-dataproc-fullpipeline'
REGION = 'us-central1'
PROJECT_ID = 'strategic-hull-412800'
jar_path = 'gs://bqdata12/spark-3.1-bigquery-0.36.1.jar'
project_gcs_path = 'gs://pipeline_scripts/'
Ingest_arg1 = 'emp'
Ingest_arg2 = project_gcs_path+'landingdata'
Ingestion_main = 'gs://pipeline_scripts/mainingestwithbq.py'
Ingestion_files = 'gs://pipeline_scripts/ingestionbq.py'
DataCleaning_main = 'gs://pipeline_scripts/maincleaningwithbq.py'
DataCleaning_arg1 = project_gcs_path+"landingdata/*.csv"
DataCleaning_arg2 = 'stg_emp'
DataCleaning_files = 'gs://pipeline_scripts/datacleaning.py'
DataEnrich_main = 'gs://pipeline_scripts/mainenrichmentwithbq.py'
DataEnrich_arg1 = 'stg_emp'
DataEnrich_arg2 = 'enrich_emp'
DataEnrich_files = 'gs://pipeline_scripts/dataenrich.py'
DataQualitycheck_main = 'gs://pipeline_scripts/enrichdqcheck.py'
DataQualitycheck_arg1 = 'stg_emp'
DataQualitycheck_arg2 = 'enrich_emp'
DataQualitycheck_files = 'gs://pipeline_scripts/enrichdqcheck.py'
ConsumeData_main = 'gs://pipeline_scripts/maincalculatesummary.py'
ConsumeData_arg1 = 'enrich_emp'
ConsumeData_arg2 = 'consume_emp'
ConsumeData_files = 'gs://pipeline_scripts/calculatesummary.py'


Ingest = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": Ingestion_main,
         "jar_file_uris": [jar_path],
         "args": [Ingest_arg1, Ingest_arg2],
         "python_file_uris": [Ingestion_files],
     }
}
DataCleaning = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": DataCleaning_main,
         "jar_file_uris": [jar_path],
         "args": [DataCleaning_arg1, DataCleaning_arg2],
         "python_file_uris": [DataCleaning_files],
     }
}
DataEnrich = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": DataEnrich_main,
         "jar_file_uris": [jar_path],
         "args": [DataEnrich_arg1, DataEnrich_arg2],
         "python_file_uris": [DataEnrich_files],
     }
}
DataQualitycheck = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": DataQualitycheck_main,
         "jar_file_uris": [jar_path],
         "args": [DataQualitycheck_arg1, DataQualitycheck_arg2],
         "python_file_uris": [DataQualitycheck_files],
     }
}
ConsumeData = {
     "reference": {"project_id": PROJECT_ID},
     "placement": {"cluster_name": CLUSTER_NAME},
     "pyspark_job": {
         "main_python_file_uri": ConsumeData_main,
         "jar_file_uris": [jar_path],
         "args": [ConsumeData_arg1, ConsumeData_arg2],
         "python_file_uris": [ConsumeData_files],
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
