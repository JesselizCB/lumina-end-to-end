# lumina-p2-pipelines

Objective: build the P2 pipelines from the approved P1 design (Lumina architecture PDF). Due 2026-11-12.
Branch: feat/p2-pipelines. Delivery: local work-unit commits; push and hand-in are the user's decision.
Test runner: pytest (Python 3.11, Java 17, PySpark local, Beam DirectRunner). Forecast: over 400 lines, no PR planned.

## Specs

- S1 "Pipelines de Cómputo / Transformación: Código funcional o scripts en motores de procesamiento (Spark, Flink, SQL, etc.) para la limpieza, enriquecimiento y cruce de los datos heterogéneos."
- S2 "Procesamiento Híbrido: Implementación funcional de transformaciones para flujos en tiempo real (baja latencia) y procesamientos masivos por lotes (batch)."
- S3 "Modelado para Analítica: Creación de las estructuras finales de datos (tablas consolidadas, esquemas en estrella/copo de nieve o colecciones optimizadas) listos para su consumo."
- S4 User: "el pdf es la primera entrega y esta correcto, en base a eso tenemos que hacer la segunda parte". The PDF stack, table names and layer rules are the baseline.
- Assumption A1: DataLandia zones are not defined in the PDF, so `map_zona_fuente` starts as a replaceable seed.
- Assumption A2: no GCP access is available yet. Spark and Beam are verified locally; dbt on BigQuery needs a project to run.
- Assumption A3 (design resolution): Beam owns streaming Silver; Spark owns TLC trips Silver; dbt owns reference Silver and Gold, so each Silver table has one writer.
- S5 User: "si, vamos con spark entre bronze y silver". Bronze stays raw on both paths; Spark reads Bronze and writes Silver, including `zona_id` and `ts_15m`.

## Tasks

- [x] T1 S1,S2,S5 inline: PySpark TLC trips from Bronze to Silver with rejected rows. GREEN: 12 passed locally on sample rows. Pending: real TLC run and BigQuery I/O.
- [ ] T2 S1 inline: reference loads (Uber Movement, census, events, zones).
- [ ] T3 S2 inline: GPS simulator and Beam streaming pipeline to Silver, errors to DLQ.
- [ ] T4 S1 inline: dbt Bronze to Silver, keys and quality tests.
- [ ] T5 S3 inline: dbt Gold star schema.
- [ ] T6 S1-S3 inline: README, row counts, end-to-end evidence.

## Log

- L1 2026-11-09 user: "aqui esta el documento de la arquitectura end to end que debemos entregar, ayudanos a revisar como podemos avanzar con este entregable".
- L2 2026-11-09 user: "hay prorroga hasta el 12".
- L3 2026-11-09 user: "el pdf es la primera entrega y esta correcto, en base a eso tenemos que hacer la segunda parte".
- L4 2026-11-09 user: "a brpnze llega la data ya transformada?" PDF p.6 conflicts with p.3 and p.7 on Bronze.
- L5 2026-11-09 user: "si, vamos con spark entre bronze y silver". T1 rescoped.
- L6 2026-11-09 user: "antes, dame un .md de todo el contexto, para psarles a mis compañeros y que estemos alineados". CONTEXTO.md written. T1 paused: 12 of 12 tests failing, cause not diagnosed. Next step: diagnose the Spark failure.
- L7 2026-11-09 user: "si, sigue con el error de spark". Cause: PYTHON_VERSION_MISMATCH, workers used system Python 3.14 and driver 3.11. Fixed in tests/conftest.py; 12 passed.
