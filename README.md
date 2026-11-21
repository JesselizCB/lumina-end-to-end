# Lumina: pipelines de datos (Parte 2)

Implementación de la arquitectura de la Parte 1 sobre GCP: un pipeline **batch** con Spark y uno de **tiempo real** con Beam, que limpian, enriquecen y cruzan los datos por zona y franja de 15 minutos. Ambos alimentan un modelo estrella en Gold construido con dbt.

Para el contexto del equipo (decisiones, plan y pendientes) ver [`CONTEXTO.md`](CONTEXTO.md).
Para el despliegue completo de esta arquitectura en GCP, sigue la [guía de despliegue de la Parte 3](gu_a_de_despliegue_en_gcp_proyecto_lumina_parte_3.md).

## Qué cubre de la evaluación

| Criterio | Implementación |
|---|---|
| Pipelines de cómputo / transformación | Spark: `src/lumina/batch/silver_trips.py`. Beam: `src/lumina/streaming/gps_pipeline.py` |
| Procesamiento híbrido | Batch: viajes TLC en Dataproc Serverless. Tiempo real: GPS por Pub/Sub y Dataflow |
| Modelado para analítica | Modelo estrella en `gold` con dbt: `dbt/models/` |

## Flujo implementado

```
BATCH      TLC (Parquet) → Cloud Storage → bronze.viajes_tlc → Spark (Dataproc) → silver.viajes
                                                                                → silver.rechazos_viajes

STREAMING  simulador → Pub/Sub gps-vehiculo ─┬→ suscripción BigQuery → bronze.gps_vehiculo (crudo)
                                             └→ Beam (Dataflow) ─┬→ silver.gps_vehiculo
                                                                 └→ Pub/Sub errores-dlq → bronze.errores

GOLD       silver.viajes + silver.gps_vehiculo + silver.zonas → dbt → dim_zona, dim_franja,
                                                                    fact_viaje, fact_gps_zona_15m,
                                                                    fact_oferta_demanda
```

- **Bronze** guarda siempre el dato crudo, tal como llegó.
- **Silver** tiene datos limpios, en km y UTC, con `zona_id` y `ts_15m`. Cada tabla tiene un solo escritor.
- Las zonas son **provisionales** (`seeds/zonas.csv` y `seeds/map_zona_fuente.csv`) hasta tener las de DataLandia.

## Paso a paso

Todos los comandos se corren desde la raíz del repositorio. En zsh, escriban siempre `${P}` con llaves: `$P:silver` se interpreta mal.

```bash
export P=luminia-parte3-2026
export R=us-central1
gcloud config set project "${P}"
gcloud config get-value project

```

`P` es el ID del proyecto de GCP y `R` la región usada por BigQuery, Dataflow y Composer. Si abres una nueva sesión de Cloud Shell, vuelve a ejecutar estas asignaciones y comprueba el proyecto activo antes de correr comandos.

### 1. Preparar el entorno local

Requiere Python 3.11, Java 17 y el [Google Cloud CLI](https://cloud.google.com/sdk/docs/install).

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
python3.11 -m venv .venv-dbt                # dbt va aparte para no chocar con Beam
.venv-dbt/bin/pip install -r requirements-dbt.txt
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

# Minimal roles for the service account that runs Dataproc and Dataflow.
# Projects inside an organization do not grant it Editor automatically.
SA=$(gcloud projects describe ${P} --format="value(projectNumber)")-compute@developer.gserviceaccount.com
for r in roles/dataproc.worker roles/dataflow.worker roles/bigquery.dataEditor roles/bigquery.jobUser \
         roles/bigquery.readSessionUser roles/storage.objectAdmin roles/pubsub.editor; do
  gcloud projects add-iam-policy-binding ${P} --member=serviceAccount:${SA} --role=${r} --condition=None --quiet
done
```

`roles/pubsub.editor` es necesario porque Dataflow crea una suscripción interna para seguir el tiempo de los eventos. Sin ese rol, el job queda en `Running` pero no escribe nada en Silver.

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
  evento_id:STRING,vehiculo_id:STRING,lat:FLOAT64,lon:FLOAT64,velocidad_kmh:FLOAT64,ts:TIMESTAMP,ts_15m:TIMESTAMP,dia:DATE,zona_id:INT64,modo:STRING,procesado_en:TIMESTAMP

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

**Reproducción con reloj simulado (para llenar otras franjas del día)**

Los eventos llevan horas simuladas en lugar de la hora actual, y se marcan con `modo: reproduccion`. Este ejemplo recorre el 10 de noviembre de 6:00 a 22:00 hora de Nueva York, avanzando 5 minutos simulados por segundo real:

```bash
PYTHONPATH=src .venv/bin/python -m lumina.streaming.gps_simulator --project ${P} --interval 1 \
  --replay-start 2026-11-10T06:00:00-05:00 --replay-end 2026-11-10T22:00:00-05:00 --replay-step 300
```

Usen fechas pasadas: con fechas futuras el reloj interno de Dataflow se adelantaría. Estos eventos no sirven para medir latencia, porque su hora es simulada: filtren con `COALESCE(modo, 'tiempo_real') = 'tiempo_real'`.

**Reglas del pipeline:** un evento va a `errores-dlq`, con el mensaje original y su `motivo`, si no es JSON válido, le falta un campo, tiene tipos incorrectos, tiene coordenadas imposibles, su velocidad está fuera de 0 a 150 km/h, su fecha no tiene zona horaria o cae fuera de las zonas. El primer evento con cada `evento_id` pasa de inmediato, y las copias que lleguen en los 10 minutos siguientes se descartan. La deduplicación no espera ventanas de tiempo, porque eso subía la latencia a casi un minuto.

**5.4 Apagar el job al terminar (importante por costo)**

Dataflow cobra mientras está encendido, unos S/ 27 al día.

```bash
JOB=$(gcloud dataflow jobs list --region=${R} --filter="name=lumina-gps-stream AND state=Running" --format="value(id)")
gcloud dataflow jobs drain ${JOB} --region=${R}
```

Drenar deja de leer mensajes nuevos, pero termina de procesar los que ya entraron. Puede tardar unos 10 minutos, porque la deduplicación recuerda cada `evento_id` durante ese tiempo. Si el lanzador del paso 5.2 seguía abierto, termina con un `AssertionError`. Es esperado y no indica un fallo del job.

### 6. Gold: modelo estrella con dbt

**6.1 Cargar las zonas a Silver (una sola vez, ya hecho)**

```bash
bq --location=${R} load --replace --source_format=CSV --skip_leading_rows=1 ${P}:silver.zonas \
  seeds/zonas.csv zona_id:INT64,nombre:STRING,min_lon:FLOAT64,min_lat:FLOAT64,max_lon:FLOAT64,max_lat:FLOAT64
```

**6.2 Construir Gold y correr las pruebas de calidad**

```bash
cd dbt
../.venv-dbt/bin/dbt build --profiles-dir .
```

`dbt build` crea las tablas y corre 30 pruebas: claves únicas y no nulas, que cada zona y franja exista en su dimensión, que Gold tenga exactamente los mismos viajes que Silver, y que el cruce tenga una fila por cada zona y franja. dbt usa las credenciales de `gcloud auth application-default login` y tiene un tope de 10 GB por consulta.

| Tabla | Tipo | Grano |
|---|---|---|
| `dim_zona` | Dimensión | Una zona |
| `dim_franja` | Dimensión | Una franja de 15 minutos del día (96 filas) |
| `fact_viaje` | Hecho (batch) | Un viaje, con fecha local, franja y zonas |
| `fact_gps_zona_15m` | Hecho (tiempo real) | Vehículos y eventos por zona e intervalo de 15 minutos |
| `fact_oferta_demanda` | Hecho consolidado | Demanda histórica frente a oferta en vivo, por zona y franja del día |

**Cómo se cruzan las fuentes:** los viajes son de enero de 2024 y el GPS es de hoy, así que se cruzan por **zona y franja horaria del día**, no por fecha exacta. Las dos fuentes usan la hora de Nueva York (`zona_horaria` en `dbt/dbt_project.yml`), porque TLC viene en esa hora y las zonas provisionales están dibujadas sobre Nueva York. `viajes_por_vehiculo` mide cuántos viajes diarios hay en promedio por cada vehículo disponible: un valor alto señala una zona y franja con falta de vehículos.

### 7. Verificar los resultados

Antes de ejecutar las consultas, reemplaza `YOUR_PROJECT_ID` por el valor configurado en `P`.

```sql
-- Batch: entrada, salida y rechazos
SELECT
  (SELECT COUNT(*) FROM `YOUR_PROJECT_ID.bronze.viajes_tlc`) AS bronze,
  (SELECT COUNT(*) FROM `YOUR_PROJECT_ID.silver.viajes`) AS silver,
  (SELECT COUNT(*) FROM `YOUR_PROJECT_ID.silver.rechazos_viajes`) AS rechazados;

SELECT motivo, COUNT(*) AS n
FROM `YOUR_PROJECT_ID.silver.rechazos_viajes`
GROUP BY motivo ORDER BY n DESC;

-- Tiempo real: latencia desde el evento hasta Silver (excluye la reproducción)
SELECT
  COUNT(*) AS eventos,
  APPROX_QUANTILES(TIMESTAMP_DIFF(procesado_en, ts, MILLISECOND), 100)[OFFSET(50)] / 1000 AS p50_segundos,
  APPROX_QUANTILES(TIMESTAMP_DIFF(procesado_en, ts, MILLISECOND), 100)[OFFSET(95)] / 1000 AS p95_segundos
FROM `YOUR_PROJECT_ID.silver.gps_vehiculo`
WHERE COALESCE(modo, 'tiempo_real') = 'tiempo_real';

-- Tiempo real: crudo frente a limpio, y errores por motivo
SELECT
  (SELECT COUNT(*) FROM `YOUR_PROJECT_ID.bronze.gps_vehiculo`) AS bronze,
  (SELECT COUNT(*) FROM `YOUR_PROJECT_ID.silver.gps_vehiculo`) AS silver;

SELECT JSON_VALUE(data, '$.motivo') AS motivo, COUNT(*) AS n
FROM `YOUR_PROJECT_ID.bronze.errores`
GROUP BY motivo ORDER BY n DESC;
```

## Resultados obtenidos (11 de noviembre de 2026)

**Batch, TLC enero 2024, en Dataproc Serverless**

| Medida | Valor |
|---|---|
| Viajes en Bronze | 2.964.624 |
| Viajes en Silver | 2.868.850 |
| Rechazados | 95.694 (60.371 por distancia no positiva, 34.064 por tarifa negativa, 1.129 por velocidad, 112 por horario, 18 fuera del mes) |
| Duplicados eliminados | 80 por `viaje_id`; se descartan de Silver y no se copian a `silver.rechazos_viajes` |
| Duración del job | Unos 2 minutos, incluido el arranque |

**Tiempo real, 4 minutos de simulador con 50 vehículos, en Dataflow**

| Medida | Valor |
|---|---|
| Mensajes publicados (Bronze) | 2.397 |
| Eventos válidos en Silver | 2.311, sin ningún faltante frente a Bronze |
| Errores enviados a la DLQ | 40 (12 velocidad, 11 campo faltante, 9 coordenada, 8 JSON inválido) |
| Duplicados descartados | 46 |
| Latencia del evento a Silver, ya estable | 1,2 s de mediana, 1,6 s en el percentil 95 |

Los primeros 2 minutos tuvieron latencias de hasta 2 minutos, porque los eventos esperaban a que arrancara el trabajador de Dataflow. Para una demo, conviene encender el simulador unos minutos después de que el job aparezca como `Running`.

**Reproducción con reloj simulado, 10 de noviembre de 6:00 a 22:00 hora de Nueva York**

| Medida | Valor |
|---|---|
| Mensajes publicados | 9.776 en unos 4 minutos reales |
| Eventos válidos en Silver | 9.430, sin ningún faltante frente a Bronze |
| Errores enviados a la DLQ | 176 |
| Duplicados descartados | 170 |
| Intervalos de 15 minutos cubiertos | 64 |

**Gold con dbt**

| Medida | Valor |
|---|---|
| Pruebas de dbt | 32 de 32 pasan |
| `fact_viaje` | 2.868.850 viajes, igual que Silver, en 31 días |
| `fact_oferta_demanda` | 480 filas (5 zonas × 96 franjas); la reproducción horaria llena 320 combinaciones zona-franja (64 franjas de 6:00 a 22:00) |
| Demanda diaria por periodo | Tarde 34.585, noche 30.293, mañana 20.690, madrugada 6.976 |

Viajes por vehículo disponible, promedio por periodo (solo franjas con GPS):

| Periodo | Viajes por día | Vehículos promedio | Viajes por vehículo |
|---|---|---|---|
| Tarde | 34.585 | 10,4 | 31,3 |
| Noche (hasta 22:00) | 22.462 | 10,3 | 29,6 |
| Mañana | 20.690 | 10,2 | 17,2 |

La zona y franja con más presión es la **zona 4 a las 18:00**: 390 viajes diarios frente a 5 vehículos, 78 viajes por vehículo. El simulador reparte los vehículos al azar, así que la oferta es pareja entre franjas y la diferencia la marca la demanda real de TLC.

```sql
-- Zonas y franjas con más viajes por vehículo disponible
SELECT zona_id, hora_inicio, viajes_promedio_dia, vehiculos_promedio, viajes_por_vehiculo
FROM `YOUR_PROJECT_ID.gold.fact_oferta_demanda`
WHERE intervalos_gps > 0
ORDER BY viajes_por_vehiculo DESC;
```

## Estructura del repositorio

```
src/lumina/batch/silver_trips.py       Spark: Bronze → Silver de viajes TLC
src/lumina/streaming/gps_pipeline.py   Beam: Pub/Sub → Silver de GPS, errores a la DLQ
src/lumina/streaming/gps_simulator.py  Simulador de la flota
dbt/                                   Modelo estrella en Gold y sus pruebas de calidad
seeds/                                 Zonas y mapa de zonas provisionales
tests/                                 Pruebas de ambos pipelines
odd/tasks/                             Registro de tareas y decisiones
```

## Limitaciones conocidas

- Las zonas son provisionales y no corresponden a DataLandia. Además, cada fuente se asigna a zonas con una regla distinta (`map_zona_fuente` para TLC, coordenadas para el GPS), así que el cruce funciona técnicamente, pero sus números solo tendrán sentido con las zonas reales.
- El GPS de 6:00 a 22:00 viene de una reproducción con reloj simulado (`modo = 'reproduccion'`), no de tiempo real. La madrugada (22:00 a 6:00) no tiene GPS.
- El streaming cubre solo el GPS. Sensores, solicitudes de viaje y carga de trenes quedan como trabajo futuro.
- La cuenta de servicio que usan Dataproc y Dataflow tiene roles amplios heredados del proyecto. Para producción habría que reducirlos.

## Arquitectura y orden de despliegue end-to-end (Parte 3)

La arquitectura completa es **Cloud Storage → BigQuery Bronze → Spark en Dataproc → Silver Iceberg → dbt/BigQuery Gold → API FastAPI en Cloud Run** para el batch TLC; en paralelo, **Pub/Sub → Beam en Dataflow → `silver.gps_vehiculo`**, con los errores enviados a Pub/Sub DLQ y conservados en Bronze. Cloud Composer orquesta la carga batch, la transformación Iceberg, `dbt build` y la reconciliación.

Orden recomendado: (1) crear proyecto, región y recursos base; (2) cargar TLC y mapas de zonas; (3) crear la tabla GPS con `modo:STRING` y desplegar Dataflow; (4) enviar eventos en tiempo real y ejecutar la reproducción de 06:00 a 22:00; (5) subir DAG y dbt a Composer y disparar `lumina_end_to_end_pipeline`; (6) desplegar la API y validar consultas; (7) drenar/eliminar los servicios para evitar costos. La [guía de despliegue](gu_a_de_despliegue_en_gcp_proyecto_lumina_parte_3.md) contiene los comandos paso a paso.

La reconciliación comprueba que `fact_viaje` conserve todos los viajes Silver, que `fact_oferta_demanda` tenga 480 filas (5 zonas × 96 franjas) y que al menos 320 combinaciones zona-franja tengan GPS. Esa cobertura requiere haber ejecutado la reproducción horaria del paso 5. En la tabla TLC de referencia, Bronze = Silver (2.868.850) + rechazos (95.694) + 80 filas duplicadas por `viaje_id`; los duplicados se eliminan con `dropDuplicates` y hoy no se guardan en la tabla de rechazos.

### Problemas comunes

- **Cloud Shell vuelve a otro proyecto:** establece de nuevo `P` y `R`, ejecuta `gcloud config set project "${P}"` y verifica con `gcloud config get-value project` antes de continuar. Las variables de shell no necesariamente sobreviven a una nueva sesión.
- **El proyecto de la guía no coincide:** `lumina-datalandia-p3` es solo un ejemplo; en este despliegue usa el ID real `luminia-parte3-2026`. Actualiza `P` y sustituye `YOUR_PROJECT_ID` por ese ID en consultas SQL.
- **Falla `reconciliation_check` en las 320 combinaciones GPS:** confirma que terminara la reproducción del paso 5 antes de disparar el DAG. Las 480 filas de Gold son la cuadrícula completa, pero no prueban por sí solas que haya GPS en las franjas esperadas.
- **Falla una consulta por `modo` desconocido:** asegúrate de crear `silver.gps_vehiculo` con `modo:STRING`; la Beam pipeline escribe esa columna para distinguir `tiempo_real` de `reproduccion`.
  Si la tabla ya existía sin esa columna, ejecuta en BigQuery Studio:
  ```sql
  ALTER TABLE `YOUR_PROJECT_ID.silver.gps_vehiculo`
    ADD COLUMN IF NOT EXISTS modo STRING;
  ```
