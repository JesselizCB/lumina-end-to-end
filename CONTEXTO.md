# Parte 2: construir los pipelines de Lumina

**Debemos entregar código funcional, no otro documento, hasta el 12 de noviembre de 2026** (prórroga; la fecha original era el 7). La Parte 1, el PDF de arquitectura, es nuestra base y no se modifica. La Parte 2 vale 25 % de la nota.

**Estado al 11 de noviembre:** la base en GCP está creada y el job de Spark ya corre en Dataproc Serverless con datos reales de enero 2024: 2.964.624 viajes en Bronze, 2.868.850 en Silver, 95.694 rechazados y 80 duplicados eliminados. El streaming de GPS también corre en Dataflow, con 1,2 segundos de latencia mediana y sin eventos perdidos. Falta el modelo estrella con dbt. El paso a paso está en el [README](README.md).

## Qué nos evalúan

Texto del enunciado:

| Criterio | Qué pide | Dónde lo cubrimos |
|---|---|---|
| Pipelines de cómputo / transformación | "Código funcional o scripts en motores de procesamiento (Spark, Flink, SQL, etc.) para la limpieza, enriquecimiento y cruce de los datos heterogéneos." | Pasos 2, 3, 6 y 7 |
| Procesamiento híbrido | "Implementación funcional de transformaciones para flujos en tiempo real (baja latencia) y procesamientos masivos por lotes (batch)." | Batch: paso 2. Tiempo real: pasos 4 y 5 |
| Modelado para analítica | "Creación de las estructuras finales de datos (tablas consolidadas, esquemas en estrella/copo de nieve o colecciones optimizadas) listos para su consumo." | Paso 7 |

## El flujo

Hay dos caminos de entrada que se juntan en Silver.

```
BATCH      archivos → Cloud Storage → Bronze (crudo) → Spark → Silver ─┐
                                                                       ├→ dbt → Gold → consumo
STREAMING  simulador → Pub/Sub ─┬→ Beam → Silver ──────────────────────┘
                                └→ Bronze (crudo, sin código)
```

| Paso | Qué se hace | Herramienta | Resultado |
|---|---|---|---|
| 1 | Definir las zonas de DataLandia y `map_zona_fuente` | CSV / GeoJSON | Tabla que traduce las zonas de cada dataset a las nuestras |
| 2 | Limpiar y enriquecer los viajes TLC | Spark | Viajes en Silver y tabla de rechazos |
| 3 | Cargar y normalizar Uber Movement, censo y eventos | Carga a BigQuery + dbt | Referencias en Bronze y Silver |
| 4 | Simular GPS, luego sensores, solicitudes y carga de trenes | Python + Pub/Sub | Eventos JSON en tiempo real |
| 5 | Validar, deduplicar y asignar `zona_id` y `ts_15m` a cada evento | Beam | Eventos en Silver en segundos; errores en `errores-dlq` |
| 6 | Aplicar pruebas de calidad | dbt | Tablas Silver verificadas |
| 7 | Cruzar fuentes por zona y franja de 15 minutos y construir el modelo estrella | dbt | Dimensiones y hechos en Gold |
| 8 | Documentar y correr todo de punta a punta | README | Comandos, conteos y capturas |

## Decisiones tomadas

| Tema | Decisión | Por qué |
|---|---|---|
| Bronze | Siempre crudo, en batch y en streaming | Permite reprocesar si una regla de limpieza estaba mal |
| Spark | Va entre Bronze y Silver | La página 6 del PDF decía cargar a Bronze ya limpio, pero las páginas 3 y 7 definen Bronze como dato "tal como llegó". Seguimos esa definición |
| Quién escribe Silver | Beam: eventos. Spark: viajes TLC. dbt: referencias | Una tabla con dos escritores puede duplicar filas |
| Gold | Solo lo escribe dbt | Un único lugar para el modelo estrella |
| Beam | Pipeline propio en Python, no la plantilla | Nos piden código y así la lógica queda visible |

## Qué falta en el modelo Gold

Además de las tablas del PDF, necesitamos estas, porque la API y el asistente las consultan:

- `fact_carga_oart`
- Disponibilidad de vehículos por zona
- `dim_estacion`
- `dim_evento`

`fact_demanda_prevista` queda para cuando exista el modelo predictivo.

## Reglas al implementar

- **Baja latencia:** la vista de vehículos disponibles lee de Silver, porque Gold se refresca cada 5 minutos y la meta es de hasta 1 minuto.
- **Errores en streaming:** Beam envía a `errores-dlq` lo que falle su validación. Pub/Sub rechaza al publicar los mensajes que no cumplen el esquema, así que esos no llegan al tópico de errores.
- **Costos por km:** TLC solo trae tarifas, así que el costo operativo y el costo del auto salen de una tabla de supuestos.
- **Gasto en GCP:** Dataflow en streaming cobra mientras corre. Se enciende solo para pruebas y demo.

### Reglas del job de viajes TLC

Un viaje va a rechazos, con su motivo, si cumple alguna de estas condiciones:

| Motivo | Condición |
|---|---|
| `campo_nulo` | Falta fecha, zona, distancia o tarifa |
| `fecha_fuera_de_periodo` | El viaje empieza fuera del mes del archivo (`--periodo`, por ejemplo `2024-01`) |
| `distancia_no_positiva` | Distancia menor o igual a 0 |
| `fin_no_posterior_a_inicio` | El viaje termina antes o a la misma hora en que empieza |
| `tarifa_negativa` | Tarifa menor que 0 |
| `velocidad_fuera_de_rango` | Más de 120 km/h (supuesto nuestro, se puede ajustar) |
| `zona_desconocida` | El origen o el destino no está en `map_zona_fuente` |

Los viajes válidos pasan de millas a km y de hora de Nueva York a UTC, y reciben `viaje_id`, `zona_origen_id`, `zona_destino_id` y `ts_15m`.

## Fuera de alcance en esta entrega

El enunciado no evalúa esto, así que no lo construimos ahora: Gemini y el asistente, entrenamiento en Vertex AI, VPC, KMS, Cloud Armor, plan de recuperación (DRP) y Looker Studio.

## Plan

| Día | Objetivo | Responsable |
|---|---|---|
| 11 nov | Base en GCP y Spark en Dataproc (hecho); simulador y Beam | Por asignar |
| 11-12 nov | dbt y modelo estrella mínimo en Gold | Por asignar |
| 12 nov | README, conteos, capturas y entrega | Por asignar |

Por el plazo, el streaming cubre solo el GPS. Sensores, solicitudes, carga de trenes, Uber Movement, censo y eventos quedan como trabajo futuro.

## Dónde estamos

- [x] Enunciado revisado y flujo definido
- [x] Proyecto creado en `lumina-pipelines/`
- [x] Job de Spark y sus pruebas escritos (`src/lumina/batch/silver_trips.py`)
- [x] Las 12 pruebas del job de Spark pasan
- [x] Base en GCP: alerta de presupuesto, bucket, datasets y tópicos
- [x] Job de Spark corriendo en Dataproc con TLC enero 2024
- [x] Simulador de GPS y pipeline de Beam en Dataflow
- [x] README con el paso a paso y los resultados
- [ ] dbt y modelo estrella en Gold
- [ ] Capturas para la entrega

Las zonas siguen siendo provisionales (`seeds/map_zona_fuente.csv`), así que los resultados por zona todavía no significan nada.

## Lo que necesitamos definir como equipo

1. **Zonas de DataLandia:** ¿el caso del curso las define? El PDF no las lista. Mientras tanto usamos una tabla provisional fácil de reemplazar.
2. **Responsables:** asignar quién prepara la evidencia y la entrega.

## Recursos en GCP

Proyecto `project1-508022`, región `southamerica-west1` (Santiago).

| Recurso | Nombre |
|---|---|
| Alerta de presupuesto | `lumina-p2`, S/ 100, calculada sin descontar créditos |
| Bucket privado | `gs://lumina-raw-project1-508022` |
| Datasets | `bronze`, `silver`, `gold` |
| Tópicos | `gps-vehiculo`, `errores-dlq` |
| Tablas | `bronze.viajes_tlc`, `bronze.gps_vehiculo`, `bronze.errores`, `silver.map_zona_fuente`, `silver.viajes`, `silver.rechazos_viajes`, `silver.gps_vehiculo` |
| Suscripciones | `gps-vehiculo-bronze` y `errores-dlq-bronze` (a BigQuery), `gps-vehiculo-beam` (para Dataflow) |
| Job de Dataflow | `lumina-gps-stream`, drenado al terminar cada prueba |

**Costos:** Dataflow cobra mientras está encendido, unos S/ 27 al día. Apáguenlo al terminar cada sesión.

## Cómo correr el job de Spark en GCP

```bash
P=project1-508022
gcloud dataproc batches submit pyspark src/lumina/batch/silver_trips.py \
  --region=southamerica-west1 --version=2.2 \
  --deps-bucket="gs://lumina-raw-${P}" \
  --properties=spark.sql.optimizer.dynamicPartitionPruning.enabled=false \
  -- --bronze "bq:${P}.bronze.viajes_tlc" \
     --zone-map "bq:${P}.silver.map_zona_fuente" \
     --silver "bq:${P}.silver.viajes" \
     --rejected "bq:${P}.silver.rechazos_viajes" \
     --periodo 2024-01 --temp-bucket "lumina-raw-${P}"
```

Dos detalles que evitan errores:
- **`dynamicPartitionPruning.enabled=false`:** sin esto, el conector de BigQuery falla al leer Bronze, que está particionada por fecha de ingesta.
- **`--temp-bucket`:** Silver se escribe a través del bucket porque la escritura directa ignora la partición por día y la agrupación por zona.

## Cómo correr las pruebas

Requiere Python 3.11 y Java 17.

```bash
cd lumina-pipelines
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
PYTHONPATH=src .venv/bin/python -m pytest -q tests
```
