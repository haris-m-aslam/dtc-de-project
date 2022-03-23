import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

# from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'br_properties_data_all')

# dataset_file = "properties_sell_201510.csv"
# bq_table_id = "properati-data-public.properties_br.properties_sell_201510"
# output_file = "gs://dtc_de_project_data_lake_dtc-de-project-344607/properties_sell_201510.csv"
dataset_file = "properties_sell_{{ execution_date.strftime(\'%Y%m\') }}.csv"
bq_table_id = "properati-data-public.properties_br.properties_sell_{{ execution_date.strftime(\'%Y%m\') }}"
output_file = f"gs://{BUCKET}/{dataset_file}"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2015, 1, 1),
    "end_date": datetime(2016, 12, 31),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="sell_data_ingestion_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:

    export_property_data_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_property_data_to_gcs',
        source_project_dataset_table=bq_table_id,
        destination_cloud_storage_uris=[output_file],
        export_format='CSV'
    )

    load_csv = GCSToBigQueryOperator(
        task_id='load_csv_from_gcs_to_bigquery',
        bucket=BUCKET,
        source_objects=dataset_file,
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.sell_data_external_table",
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1,
        schema_fields=[
            {
                "name": "id", "type": "STRING", "mode": "NULLABLE"
            },
            {
                "name": "created_on", "type": "DATE", "mode": "NULLABLE"
            },
            {
                "name": "operation", "type": "STRING", "mode": "NULLABLE"
            },
            {
                "name": "property_type", "type": "STRING", "mode": "NULLABLE"
            },
            {
                "name": "place_name", "type": "STRING", "mode": "NULLABLE"
            },
            {
                "name": "place_with_parent_names", "type": "STRING", "mode": "NULLABLE"
            },
            {
                "name": "country_name", "type": "STRING", "mode": "NULLABLE"
            },
            {
                "name": "state_name", "type": "STRING", "mode": "NULLABLE"
            },
            {
                "name": "geonames_id", "type": "INTEGER", "mode": "NULLABLE"
            },
            {
                "name": "lat_lon", "type": "STRING", "mode": "NULLABLE"
            },
            {
                "name": "lat", "type": "FLOAT", "mode": "NULLABLE"
            },
            {
                "name": "lon", "type": "FLOAT", "mode": "NULLABLE"
            },
            {
                "name": "price", "type": "FLOAT", "mode": "NULLABLE"
            },
            {
                "name": "currency", "type": "STRING", "mode": "NULLABLE"
            },
            {
                "name": "price_aprox_local_currency", "type": "FLOAT", "mode": "NULLABLE"
            },
            {
                "name": "price_aprox_usd", "type": "FLOAT", "mode": "NULLABLE"
            },
            {
                "name": "surface_total_in_m2", "type": "INTEGER", "mode": "NULLABLE"
            },
            {
                "name": "surface_covered_in_m2", "type": "INTEGER", "mode": "NULLABLE"
            },
            {
                "name": "price_usd_per_m2", "type": "FLOAT", "mode": "NULLABLE"
            },
            {
                "name": "price_per_m2", "type": "FLOAT", "mode": "NULLABLE"
            },
            {
                "name": "floor", "type": "INTEGER", "mode": "NULLABLE"
            },
            {
                "name": "rooms", "type": "INTEGER", "mode": "NULLABLE"
            },
            {
                "name": "expenses", "type": "INTEGER", "mode": "NULLABLE"
            },
            {
                "name": "properati_url", "type": "STRING", "mode": "NULLABLE"
            },
            {
                "name": "description", "type": "STRING", "mode": "NULLABLE"
            },
            {
                "name": "title", "type": "STRING", "mode": "NULLABLE"
            },
            {
                "name": "image_thumbnail", "type": "STRING", "mode": "NULLABLE"
            }
        ]
    )

    export_property_data_to_gcs >> load_csv