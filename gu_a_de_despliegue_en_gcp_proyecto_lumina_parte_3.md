# Guía de Despliegue en GCP — Proyecto Lumina (Parte 3)

Esta guía cubre todo el despliegue **100 % en la nube** desde una cuenta nueva de Google Cloud Platform (GCP), partiendo de cero hasta dejar operativos:
1. **Data Lake & Lakehouse** (Cloud Storage + **Apache Iceberg** + BigQuery).
2. **Streaming en tiempo real** (Pub/Sub + Apache Beam en Dataflow).
3. **Orquestación end-to-end** (Cloud Composer / Airflow + Dataproc Serverless + dbt).
4. **Capa de exposición** (FastAPI en Cloud Run).

> **Reglas de costo y cuota (Free Trial):** Todo se despliega en `us-central1` para aprovechar la capa gratuita de BigQuery/Storage y respetar el límite de **8 vCPUs** de prueba.

---

## Paso 0: Crear el proyecto y abrir Cloud Shell (Consola Web)

1. Ingresa a [console.cloud.google.com](https://console.cloud.google.com/) con tu cuenta nueva.
2. Haz clic en el selector de proyectos (arriba a la izquierda, junto al logo de Google Cloud) $\rightarrow$ **Proyecto nuevo**.
3. Nómbralo, por ejemplo, `lumina-datalandia-p3`, asegúrate de que esté vinculada tu **Cuenta de facturación de prueba** y haz clic en **Crear**.
4. Selecciónalo como proyecto activo en la barra superior.
5. *(Recomendado)* Ve a **Facturación $\rightarrow$ Presupuestos y alertas** y crea una alerta de presupuesto por `10 USD` (o `40 PEN`) para recibir notificaciones de consumo.
6. Abre **Cloud Shell** haciendo clic en el ícono `>_` en la esquina superior derecha de la consola.

---

## Paso 1: Configurar variables y habilitar APIs

En la terminal de **Cloud Shell**, ejecuta este bloque para guardar el ID de tu proyecto y activar los servicios necesarios:

```bash
export P=$(gcloud config get-value project)
export R=us-central1
export N=$(gcloud projects describe ${P} --format="value(projectNumber)")

echo "Proyecto: ${P} | Número: ${N} | Región: ${R}"

# Habilitar todas las APIs del proyecto
gcloud services enable \
  compute.googleapis.com storage.googleapis.com bigquery.googleapis.com \
  bigquerystorage.googleapis.com pubsub.googleapis.com dataproc.googleapis.com \
  dataflow.googleapis.com composer.googleapis.com run.googleapis.com \
  cloudbuild.googleapis.com artifactregistry.googleapis.com

# Permitir acceso privado a Google en la subred default (requerido por Dataproc, Dataflow y Composer)
gcloud compute networks subnets update default \
  --region=${R} \
  --enable-private-ip-google-access
```

---

## Paso 2: Crear Bucket, Datasets, Tópicos y Tablas Streaming

Ejecuta este bloque para crear la estructura base de datos y mensajería:

```bash
# 1. Bucket principal (Raw + Iceberg Warehouse + temporales)
gcloud storage buckets create gs://lumina-raw-${P} \
  --location=${R} \
  --uniform-bucket-level-access \
  --public-access-prevention

# 2. Datasets en BigQuery
for d in bronze silver gold; do
  bq --location=${R} mk --dataset ${P}:${d}
done

# 3. Tópicos Pub/Sub
for t in gps-vehiculo errores-dlq; do
  gcloud pubsub topics create ${t} --message-storage-policy-allowed-regions=${R}
done

# 4. Tabla Silver de telemetría GPS
bq mk --table --time_partitioning_field=dia --time_partitioning_type=DAY --clustering_fields=zona_id \
  ${P}:silver.gps_vehiculo \
  evento_id:STRING,vehiculo_id:STRING,lat:FLOAT64,lon:FLOAT64,velocidad_kmh:FLOAT64,ts:TIMESTAMP,ts_15m:TIMESTAMP,dia:DATE,zona_id:INT64,procesado_en:TIMESTAMP

# 5. Tablas Bronze de streaming y permisos para Pub/Sub
for t in gps_vehiculo errores; do
  bq mk --table --time_partitioning_field=publish_time --time_partitioning_type=DAY ${P}:bronze.${t} \
    subscription_name:STRING,message_id:STRING,publish_time:TIMESTAMP,data:STRING,attributes:STRING
  bq add-iam-policy-binding --role=roles/bigquery.dataEditor \
    --member=serviceAccount:service-${N}@gcp-sa-pubsub.iam.gserviceaccount.com ${P}:bronze.${t}
done

# 6. Suscripciones Pub/Sub
gcloud pubsub subscriptions create gps-vehiculo-bronze --topic=gps-vehiculo \
  --bigquery-table=${P}:bronze.gps_vehiculo --write-metadata
gcloud pubsub subscriptions create errores-dlq-bronze --topic=errores-dlq \
  --bigquery-table=${P}:bronze.errores --write-metadata
gcloud pubsub subscriptions create gps-vehiculo-beam --topic=gps-vehiculo --ack-deadline=60
```

---

## Paso 3: Iniciar Cloud Composer (Airflow) en segundo plano

La creación del entorno de Airflow tarda entre **15 y 20 minutos**. Lánzalo ahora de forma asíncrona (`--async`) para avanzar con los demás pasos mientras se aprovisiona:

```bash
gcloud composer environments create lumina-airflow \
  --location=${R} \
  --environment-size=small \
  --async
```

---

## Paso 4: Clonar el repositorio (`feat/p3-full-flow`) y subir datos iniciales

Clona tu rama de GitHub en Cloud Shell, descarga el dataset histórico TLC y carga los catálogos de zonas en Silver:

```bash
git clone -b feat/p3-full-flow https://github.com/JesselizCB/lumina-end-to-end.git
cd lumina-end-to-end

# Descargar viajes TLC enero 2024 y subir al Data Lake
mkdir -p data
curl -fsSL -o data/yellow_tripdata_2024-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
gcloud storage cp data/yellow_tripdata_2024-01.parquet gs://lumina-raw-${P}/tlc/2024-01/

# Cargar catálogos de zonas en Silver
bq --location=${R} load --source_format=CSV --skip_leading_rows=1 \
  ${P}:silver.map_zona_fuente seeds/map_zona_fuente.csv fuente:STRING,zona_origen:STRING,zona_id:INT64

bq --location=${R} load --replace --source_format=CSV --skip_leading_rows=1 \
  ${P}:silver.zonas seeds/zonas.csv zona_id:INT64,nombre:STRING,min_lon:FLOAT64,min_lat:FLOAT64,max_lon:FLOAT64,max_lat:FLOAT64
```

---

## Paso 5: Ejecutar el pipeline Streaming (Dataflow + Simulador)

### 5.1 Lanzar el job de Apache Beam en Dataflow
Usamos `--worker_machine_type e2-medium` y `--max_num_workers 1` para mantener bajo el consumo de vCPUs y créditos:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

.venv/bin/python src/lumina/streaming/gps_pipeline.py \
  --input_subscription projects/${P}/subscriptions/gps-vehiculo-beam \
  --output_table ${P}:silver.gps_vehiculo \
  --error_topic projects/${P}/topics/errores-dlq \
  --zones seeds/zonas.csv \
  --runner DataflowRunner --project ${P} --region ${R} \
  --temp_location gs://lumina-raw-${P}/dataflow/temp \
  --staging_location gs://lumina-raw-${P}/dataflow/staging \
  --job_name lumina-gps-stream --worker_machine_type e2-medium \
  --num_workers 1 --max_num_workers 1 --enable_streaming_engine \
  --subnetwork regions/${R}/subnetworks/default --no_use_public_ips
```

*(Cuando veas que el job se envió a Dataflow, presiona `Ctrl + C` en la terminal; el job sigue ejecutándose en la nube).*

### 5.2 Esperar estado `Running` y lanzar el simulador
Verifica en la consola de **Dataflow** (o con el siguiente comando) que el job esté en estado `Running` y haya levantado su worker (~3 minutos):

```bash
gcloud dataflow jobs list --region=${R} --filter="name=lumina-gps-stream" --limit=1
```

Cuando esté `Running`, ejecuta las dos simulaciones (tiempo real y franjas de 06:00 a 22:00):

```bash
# 1. Tráfico en tiempo real (3 minutos, mide latencia p50/p95)
PYTHONPATH=src .venv/bin/python -m lumina.streaming.gps_simulator --project ${P} --duration 180

# 2. Reproducción horaria (06:00 a 22:00) para cruzar oferta y demanda en Gold
PYTHONPATH=src .venv/bin/python -m lumina.streaming.gps_simulator --project ${P} --interval 1 \
  --replay-start 2026-11-10T06:00:00-05:00 --replay-end 2026-11-10T22:00:00-05:00 --replay-step 300
```

### 5.3 Drenar Dataflow al terminar las pruebas
**Importante:** Drena el job apenas termines la simulación para liberar vCPUs y evitar consumo continuo de créditos:

```bash
JOB=$(gcloud dataflow jobs list --region=${R} --filter="name=lumina-gps-stream AND state=Running" --format="value(id)")
gcloud dataflow jobs drain ${JOB} --region=${R}
```

---

## Paso 6: Orquestar Batch + Apache Iceberg + Gold en Cloud Composer

### 6.1 Verificar que Composer esté `RUNNING`
```bash
gcloud composer environments list --locations=${R}
```

### 6.2 Subir código, configurar Airflow y disparar el DAG
Una vez que el estado sea `RUNNING`, ejecuta:

```bash
# 1. Instalar dbt-bigquery en el entorno de Airflow
gcloud composer environments update lumina-airflow \
  --location=${R} \
  --update-pypi-package=dbt-bigquery

# 2. Subir script Spark (con soporte Apache Iceberg) a Cloud Storage
gcloud storage cp src/lumina/batch/silver_trips.py gs://lumina-raw-${P}/dependencies/

# 3. Subir DAG y proyecto dbt al bucket interno de Composer
DAGS_BUCKET=$(gcloud composer environments describe lumina-airflow --location=${R} --format="value(config.dagGcsPrefix)")
gcloud storage cp dags/lumina_batch_gold_dag.py ${DAGS_BUCKET}/
gcloud storage cp -r dbt ${DAGS_BUCKET}/

# 4. Configurar variables en Airflow y disparar el pipeline end-to-end
gcloud composer environments run lumina-airflow --location=${R} variables -- set gcp_project ${P}
gcloud composer environments run lumina-airflow --location=${R} variables -- set gcp_region ${R}
gcloud composer environments run lumina-airflow --location=${R} dags trigger -- lumina_end_to_end_pipeline
```

Puedes abrir la **Interfaz Web de Airflow** desde la consola de GCP (**Composer $\rightarrow$ lumina-airflow $\rightarrow$ Airflow UI**) para ver en vivo el avance de las 4 tareas: `load_bronze_tlc` $\rightarrow$ `spark_silver_iceberg` $\rightarrow$ `dbt_build_gold` $\rightarrow$ `reconciliation_check`.

---

## Paso 7: Desplegar la API REST en Cloud Run

Con Gold ya poblado por el DAG, despliega el servicio FastAPI en **Cloud Run**:

```bash
gcloud run deploy lumina-api \
  --source api \
  --region=${R} \
  --platform=managed \
  --allow-unauthenticated \
  --set-env-vars="GCP_PROJECT=${P}"
```

Al terminar, la terminal mostrará la **Service URL** (`https://lumina-api-xxxx.a.run.app`). Abre `<SERVICE_URL>/docs` en tu navegador para probar los endpoints interactivos.

---

## Paso 8: Consultas SQL de Verificación (Para la Demo del Sábado)

Ejecuta estas consultas en **BigQuery Studio** para validar y tomar capturas de cada capa:

```sql
-- 1. Reconciliación Batch (Bronze vs Silver Iceberg vs Gold)
SELECT
  (SELECT COUNT(*) FROM `bronze.viajes_tlc`) AS bronze_viajes,
  (SELECT COUNT(*) FROM `silver.viajes`) AS silver_iceberg,
  (SELECT COUNT(*) FROM `silver.rechazos_viajes`) AS silver_rechazados,
  (SELECT COUNT(*) FROM `gold.fact_viaje`) AS gold_fact_viaje;

-- 2. Auditoría de Snapshots Apache Iceberg (Time Travel)
SELECT snapshot_id, committed_at, operation, summary
FROM `silver.viajes$snapshots`
ORDER BY committed_at DESC
LIMIT 5;

-- 3. Latencia Streaming (p50 y p95 en segundos)
SELECT
  COUNT(*) AS eventos,
  ROUND(APPROX_QUANTILES(TIMESTAMP_DIFF(procesado_en, ts, MILLISECOND), 100)[OFFSET(50)] / 1000, 2) AS p50_seg,
  ROUND(APPROX_QUANTILES(TIMESTAMP_DIFF(procesado_en, ts, MILLISECOND), 100)[OFFSET(95)] / 1000, 2) AS p95_seg
FROM `silver.gps_vehiculo`
WHERE COALESCE(modo, 'tiempo_real') = 'tiempo_real';

-- 4. Top Zonas Críticas (Modelo Estrella Gold)
SELECT zona_id, hora_inicio, periodo,
       ROUND(viajes_promedio_dia, 1) AS viajes_dia,
       ROUND(vehiculos_promedio, 1) AS vehiculos,
       ROUND(viajes_por_vehiculo, 1) AS ratio_presion
FROM `gold.fact_oferta_demanda`
WHERE intervalos_gps > 0
ORDER BY viajes_por_vehiculo DESC
LIMIT 5;
```

---

## Paso 9: Limpieza Post-Sustentación (Evitar cargos residuales)

Terminada tu evaluación del sábado, elimina los recursos activos:

```bash
# 1. Eliminar entorno de Cloud Composer
gcloud composer environments delete lumina-airflow --location=${R} --quiet

# 2. Eliminar servicio de Cloud Run
gcloud run services delete lumina-api --region=${R} --quiet

# 3. Verificar que no queden jobs de Dataflow corriendo
gcloud dataflow jobs list --region=${R} --filter="state=Running"
```