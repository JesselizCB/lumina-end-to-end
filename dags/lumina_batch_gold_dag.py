"""DAG de orquestación End-to-End de Lumina (Batch + Iceberg + dbt Gold)."""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)

PROJECT_ID = Variable.get("gcp_project")
REGION = Variable.get("gcp_region", default_var=os.getenv("GCP_REGION", "us-central1"))
RAW_BUCKET = Variable.get("raw_bucket", default_var=f"lumina-raw-{PROJECT_ID}")
PERIODO = Variable.get("periodo", default_var="2024-01")

DAGS_FOLDER = "/home/airflow/gcs/dags"
DBT_DIR = f"{DAGS_FOLDER}/dbt"

default_args = {
    "owner": "datalandia",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lumina_end_to_end_pipeline",
    description="Orquestación Bronze -> Silver (Iceberg) -> Gold (dbt) -> Reconciliación",
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["lumina", "iceberg", "dbt", "gcp"],
) as dag:

    # 1. Carga idempotente desde Cloud Storage hacia BigQuery Bronze
    load_bronze_tlc = BigQueryInsertJobOperator(
        task_id="load_bronze_tlc",
        location=REGION,
        configuration={
            "query": {
                "query": f"""
                    LOAD DATA OVERWRITE `{PROJECT_ID}.bronze.viajes_tlc`
                    FROM FILES (
                        format = 'PARQUET',
                        uris = ['gs://{RAW_BUCKET}/tlc/{PERIODO}/yellow_tripdata_{PERIODO}.parquet']
                    );
                """,
                "useLegacySql": False,
            }
        },
    )

    # 2. Procesamiento Spark en Dataproc Serverless escribiendo tablas Apache Iceberg
    spark_silver_iceberg = DataprocCreateBatchOperator(
        task_id="spark_silver_iceberg",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="lumina-silver-iceberg-{{ ts_nodash | lower }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{RAW_BUCKET}/dependencies/silver_trips.py",
                "args": [
                    "--bronze", f"bq:{PROJECT_ID}.bronze.viajes_tlc",
                    "--zone-map", f"bq:{PROJECT_ID}.silver.map_zona_fuente",
                    "--silver", "iceberg:iceberg.silver.viajes",
                    "--rejected", "iceberg:iceberg.silver.rechazos_viajes",
                    "--periodo", PERIODO,
                ],
            },
            "runtime_config": {
                "version": "2.2",
                "properties": {
                    "spark.sql.optimizer.dynamicPartitionPruning.enabled": "false",
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
                    "spark.sql.catalog.iceberg.catalog-impl": "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog",
                    "spark.sql.catalog.iceberg.gcp_project": PROJECT_ID,
                    "spark.sql.catalog.iceberg.gcp_location": REGION,
                    "spark.sql.catalog.iceberg.warehouse": f"gs://{RAW_BUCKET}/iceberg",
                },
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default",
                }
            },
        },
    )

    # 3. Construcción del modelo estrella en Gold y ejecución de pruebas de calidad dbt
    dbt_build_gold = BashOperator(
        task_id="dbt_build_gold",
        env={
            **os.environ,
            "GCP_PROJECT": PROJECT_ID,
            "GCP_REGION": REGION,
        },
        bash_command=f"dbt build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    # 4. Control de calidad: verificación de integridad entre Bronze, Silver y Gold
    reconciliation_check = BigQueryCheckOperator(
        task_id="reconciliation_check",
        use_legacy_sql=False,
        location=REGION,
        sql=f"""
            SELECT
                (SELECT COUNT(*) FROM `{PROJECT_ID}.silver.viajes`) > 0
                AND (SELECT COUNT(*) FROM `{PROJECT_ID}.silver.viajes`)
                  = (SELECT COUNT(*) FROM `{PROJECT_ID}.gold.fact_viaje`)
                AND (
                    SELECT COUNT(*)
                    FROM `{PROJECT_ID}.gold.fact_oferta_demanda`
                ) = 480
                -- 5 zones x 96 slots; replay covers 64 slots across 5 zones.
                AND (
                    SELECT COUNTIF(intervalos_gps > 0)
                    FROM `{PROJECT_ID}.gold.fact_oferta_demanda`
                ) >= 320
        """,
    )

    load_bronze_tlc >> spark_silver_iceberg >> dbt_build_gold >> reconciliation_check
