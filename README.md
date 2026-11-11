# Lumina: pipelines de datos (Parte 2)

Implementación de la arquitectura de la Parte 1 sobre GCP: un pipeline **batch** con Spark y uno de **tiempo real** con Beam, que limpian, enriquecen y cruzan los datos por zona y franja de 15 minutos. El modelo estrella en Gold con dbt se agrega después.

Para el contexto del equipo (decisiones, plan y pendientes) ver [`CONTEXTO.md`](CONTEXTO.md).

## Qué cubre de la evaluación

| Criterio | Implementación |
|---|---|
| Pipelines de cómputo / transformación | Spark: `src/lumina/batch/silver_trips.py`. Beam: `src/lumina/streaming/gps_pipeline.py` |
| Procesamiento híbrido | Batch: viajes TLC en Dataproc Serverless. Tiempo real: GPS por Pub/Sub y Dataflow |
| Modelado para analítica | Pendiente: modelo estrella en `gold` con dbt |

## Flujo implementado

```
BATCH      TLC (Parquet) → Cloud Storage → bronze.viajes_tlc → Spark (Dataproc) → silver.viajes
                                                                                → silver.rechazos_viajes

STREAMING  simulador → Pub/Sub gps-vehiculo ─┬→ suscripción BigQuery → bronze.gps_vehiculo (crudo)
                                             └→ Beam (Dataflow) ─┬→ silver.gps_vehiculo
                                                                 └→ Pub/Sub errores-dlq → bronze.errores
```

- **Bronze** guarda siempre el dato crudo, tal como llegó.
- **Silver** tiene datos limpios, en km y UTC, con `zona_id` y `ts_15m`. Cada tabla tiene un solo escritor.
- Las zonas son **provisionales** (`seeds/zonas.csv` y `seeds/map_zona_fuente.csv`) hasta tener las de DataLandia.

## Paso a paso

Todos los comandos se corren desde la raíz del repositorio. En zsh, escriban siempre `${P}` con llaves: `$P:silver` se interpreta mal.

```bash
P=project1-508022
R=southamerica-west1
```

### 1. Preparar el entorno local

Requiere Python 3.11, Java 17 y el [Google Cloud CLI](https://cloud.google.com/sdk/docs/install).

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
gcloud auth login
gcloud auth application-default login   # credenciales para el simulador y el lanzador de Beam
gcloud config set project ${P}
```

### 2. Correr las pruebas

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q tests
```

Cubren las reglas de limpieza de ambos pipelines, la deduplicación, el ruteo de errores y el simulador. Corren localmente, sin GCP.

### 3. Crear la infraestructura (una sola vez, ya hecho)

```bash
# Alerta de presupuesto calculada sin descontar créditos
gcloud billing budgets create --billing-account=<CUENTA> --display-name=lumina-p2 \
  --budget-amount=100PEN --filter-projects=projects/${P} \
  --credit-types-treatment=exclude-all-credits \
  --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0

gcloud services enable compute.googleapis.com storage.googleapis.com bigquery.googleapis.com \
  bigquerystorage.googleapis.com pubsub.googleapis.com dataproc.googleapis.com dataflow.googleapis.com

# Dataproc y Dataflow corren sin IP pública
gcloud compute networks subnets update default --region=${R} --enable-private-ip-google-access

gcloud storage buckets create gs://lumina-raw-${P} --location=${R} \
  --uniform-bucket-level-access --public-access-prevention
for d in bronze silver gold; do bq --location=${R} mk --dataset ${P}:${d}; done
for t in gps-vehiculo errores-dlq; do
  gcloud pubsub topics create ${t} --message-storage-policy-allowed-regions=${R}
done
```

### 4. Batch: viajes TLC con Spark

**4.1 Subir el archivo crudo y cargarlo a Bronze sin transformar**

```bash
mkdir -p data
curl -fsSL -o data/yellow_tripdata_2024-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
gcloud storage cp data/yellow_tripdata_2024-01.parquet gs://lumina-raw-${P}/tlc/2024-01/
bq --location=${R} load --source_format=PARQUET --time_partitioning_type=DAY \
  ${P}:bronze.viajes_tlc gs://lumina-raw-${P}/tlc/2024-01/yellow_tripdata_2024-01.parquet
bq --location=${R} load --source_format=CSV --skip_leading_rows=1 \
  ${P}:silver.map_zona_fuente seeds/map_zona_fuente.csv fuente:STRING,zona_origen:STRING,zona_id:INT64
```

**4.2 Lanzar el job de Spark en Dataproc Serverless**

```bash
gcloud dataproc batches submit pyspark src/lumina/batch/silver_trips.py \
  --region=${R} --version=2.2 --deps-bucket=gs://lumina-raw-${P} \
  --properties=spark.sql.optimizer.dynamicPartitionPruning.enabled=false \
  -- --bronze bq:${P}.bronze.viajes_tlc \
     --zone-map bq:${P}.silver.map_zona_fuente \
     --silver bq:${P}.silver.viajes \
     --rejected bq:${P}.silver.rechazos_viajes \
     --periodo 2024-01 --temp-bucket lumina-raw-${P}
```

- `dynamicPartitionPruning.enabled=false` evita una falla del conector de BigQuery al leer Bronze, que está particionada por fecha de ingesta.
- `--temp-bucket` hace que Silver se cargue a través del bucket. La escritura directa ignora la partición por día y la agrupación por zona.

**Reglas del job:** un viaje va a `silver.rechazos_viajes`, con su `motivo`, si tiene campos nulos, empieza fuera del mes del archivo, tiene distancia no positiva, termina antes de empezar, tiene tarifa negativa, supera 120 km/h o su zona no está en el mapa. Los válidos pasan a km y UTC, y reciben `viaje_id`, zonas de origen y destino, y `ts_15m`.

### 5. Tiempo real: GPS con Beam

**5.1 Tablas y suscripciones (una sola vez, ya hecho)**

```bash
N=$(gcloud projects describe ${P} --format="value(projectNumber)")

bq mk --table --time_partitioning_field=dia --time_partitioning_type=DAY --clustering_fields=zona_id \
  ${P}:silver.gps_vehiculo \
  evento_id:STRING,vehiculo_id:STRING,lat:FLOAT64,lon:FLOAT64,velocidad_kmh:FLOAT64,ts:TIMESTAMP,ts_15m:TIMESTAMP,dia:DATE,zona_id:INT64,procesado_en:TIMESTAMP

for t in gps_vehiculo errores; do
  bq mk --table --time_partitioning_field=publish_time --time_partitioning_type=DAY ${P}:bronze.${t} \
    subscription_name:STRING,message_id:STRING,publish_time:TIMESTAMP,data:STRING,attributes:STRING
  bq add-iam-policy-binding --role=roles/bigquery.dataEditor \
    --member=serviceAccount:service-${N}@gcp-sa-pubsub.iam.gserviceaccount.com ${P}:bronze.${t}
done

gcloud pubsub subscriptions create gps-vehiculo-bronze --topic=gps-vehiculo \
  --bigquery-table=${P}:bronze.gps_vehiculo --write-metadata
gcloud pubsub subscriptions create errores-dlq-bronze --topic=errores-dlq \
  --bigquery-table=${P}:bronze.errores --write-metadata
gcloud pubsub subscriptions create gps-vehiculo-beam --topic=gps-vehiculo --ack-deadline=60
```

**5.2 Lanzar el pipeline en Dataflow**

El comando queda esperando mientras el job corre. Pueden cerrarlo con Ctrl+C: el job sigue en Dataflow.

```bash
.venv/bin/python src/lumina/streaming/gps_pipeline.py \
  --input_subscription projects/${P}/subscriptions/gps-vehiculo-beam \
  --output_table ${P}:silver.gps_vehiculo \
  --error_topic projects/${P}/topics/errores-dlq \
  --zones seeds/zonas.csv \
  --runner DataflowRunner --project ${P} --region ${R} \
  --temp_location gs://lumina-raw-${P}/dataflow/temp \
  --staging_location gs://lumina-raw-${P}/dataflow/staging \
  --job_name lumina-gps-stream --worker_machine_type e2-standard-2 \
  --num_workers 1 --max_num_workers 1 --enable_streaming_engine \
  --subnetwork regions/${R}/subnetworks/default --no_use_public_ips
```

Esperen a que el job aparezca como `Running`:

```bash
gcloud dataflow jobs list --region=${R} --filter="name=lumina-gps-stream" --limit=1
```

**5.3 Enviar eventos con el simulador**

Por defecto, 50 vehículos envían un evento cada 5 segundos durante 3 minutos. El 2 % de los eventos se corrompe y el 2 % se envía duplicado, para demostrar la validación y la deduplicación.

```bash
PYTHONPATH=src .venv/bin/python -m lumina.streaming.gps_simulator --project ${P} --duration 180
```

**Reglas del pipeline:** un evento va a `errores-dlq`, con el mensaje original y su `motivo`, si no es JSON válido, le falta un campo, tiene tipos incorrectos, tiene coordenadas imposibles, su velocidad está fuera de 0 a 150 km/h, su fecha no tiene zona horaria o cae fuera de las zonas. Los duplicados con el mismo `evento_id` dentro de una ventana de 10 segundos se descartan.

**5.4 Apagar el job al terminar (importante por costo)**

Dataflow cobra mientras está encendido, unos S/ 27 al día.

```bash
JOB=$(gcloud dataflow jobs list --region=${R} --filter="name=lumina-gps-stream AND state=Running" --format="value(id)")
gcloud dataflow jobs drain ${JOB} --region=${R}
```

### 6. Verificar los resultados

```sql
-- Batch: entrada, salida y rechazos
SELECT
  (SELECT COUNT(*) FROM `project1-508022.bronze.viajes_tlc`) AS bronze,
  (SELECT COUNT(*) FROM `project1-508022.silver.viajes`) AS silver,
  (SELECT COUNT(*) FROM `project1-508022.silver.rechazos_viajes`) AS rechazados;

SELECT motivo, COUNT(*) AS n
FROM `project1-508022.silver.rechazos_viajes`
GROUP BY motivo ORDER BY n DESC;

-- Tiempo real: latencia desde el evento hasta Silver
SELECT
  COUNT(*) AS eventos,
  APPROX_QUANTILES(TIMESTAMP_DIFF(procesado_en, ts, MILLISECOND), 100)[OFFSET(50)] / 1000 AS p50_segundos,
  APPROX_QUANTILES(TIMESTAMP_DIFF(procesado_en, ts, MILLISECOND), 100)[OFFSET(95)] / 1000 AS p95_segundos
FROM `project1-508022.silver.gps_vehiculo`;

-- Tiempo real: crudo frente a limpio, y errores por motivo
SELECT
  (SELECT COUNT(*) FROM `project1-508022.bronze.gps_vehiculo`) AS bronze,
  (SELECT COUNT(*) FROM `project1-508022.silver.gps_vehiculo`) AS silver;

SELECT JSON_VALUE(data, '$.motivo') AS motivo, COUNT(*) AS n
FROM `project1-508022.bronze.errores`
GROUP BY motivo ORDER BY n DESC;
```

## Estructura del repositorio

```
src/lumina/batch/silver_trips.py       Spark: Bronze → Silver de viajes TLC
src/lumina/streaming/gps_pipeline.py   Beam: Pub/Sub → Silver de GPS, errores a la DLQ
src/lumina/streaming/gps_simulator.py  Simulador de la flota
seeds/                                 Zonas y mapa de zonas provisionales
tests/                                 Pruebas de ambos pipelines
odd/tasks/                             Registro de tareas y decisiones
```

## Limitaciones conocidas

- Las zonas son provisionales y no corresponden a DataLandia.
- El streaming cubre solo el GPS. Sensores, solicitudes de viaje y carga de trenes quedan como trabajo futuro.
- La cuenta de servicio que usan Dataproc y Dataflow tiene roles amplios heredados del proyecto. Para producción habría que reducirlos.
