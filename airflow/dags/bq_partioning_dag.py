import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'br_properties_data_all')

property_types = ['sell', 'rent']

# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# DATASET = "tripdata"
# COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime'}
# COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}
# INPUT_PART = "raw"
# INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="bq_partioning_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for property_type in property_types:

        CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{property_type}_data_partitioned \
            PARTITION BY created_on \
            CLUSTER BY state_name \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{property_type}_data_external_table;"
        )

        # CREATE_BQ_TBL_QUERY = (
        #     f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.sell_data_partitioned \
        #     PARTITION BY created_on \
        #     CLUSTER BY state_name \
        #     AS \
        #     SELECT * FROM {BIGQUERY_DATASET}.sell_data;"
        # )

        # Create a partitioned table from external table
        bq_create_partitioned_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_{property_type}_data_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        bq_create_partitioned_table_job
