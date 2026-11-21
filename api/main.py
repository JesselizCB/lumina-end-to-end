"""API REST de exposición de datos analíticos y telemetría de Lumina (DataLandia)."""

import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT")
if not PROJECT_ID:
    # Detecta automáticamente el proyecto si corre dentro de Cloud Run
    _, PROJECT_ID = bigquery.Client().project, bigquery.Client().project

bq_client = bigquery.Client(project=PROJECT_ID)

app = FastAPI(
    title="Lumina Mobility Data API",
    description="Capa de exposición de indicadores de oferta/demanda (Gold) y telemetría en vivo (Silver).",
    version="3.0.0",
)


@app.get("/", tags=["Estado"])
def root():
    return {
        "servicio": "Lumina DataLandia API",
        "version": "3.0.0",
        "project_id": PROJECT_ID,
        "docs": "/docs",
    }


@app.get("/health", tags=["Estado"])
def health():
    return {"status": "ok"}


@app.get("/api/v1/oferta-demanda", tags=["Analítica Gold"])
def get_oferta_demanda(
    zona_id: Optional[int] = Query(None, description="Filtrar por ID de zona (1-5)"),
    periodo: Optional[str] = Query(
        None, description="Filtrar por periodo: manana, tarde, noche, madrugada"
    ),
    solo_con_gps: bool = Query(
        True, description="Mostrar solo franjas con vehículos activos"
    ),
    limit: int = Query(20, ge=1, le=500),
):
    """Consulta el modelo estrella gold.fact_oferta_demanda."""
    conditions = ["1=1"]
    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]

    if solo_con_gps:
        conditions.append("intervalos_gps > 0")
    if zona_id is not None:
        conditions.append("zona_id = @zona_id")
        params.append(bigquery.ScalarQueryParameter("zona_id", "INT64", zona_id))
    if periodo:
        conditions.append("LOWER(periodo) = LOWER(@periodo)")
        params.append(bigquery.ScalarQueryParameter("periodo", "STRING", periodo))

    query = f"""
        SELECT
            zona_id,
            franja_id,
            hora_inicio,
            periodo,
            ROUND(viajes_promedio_dia, 2) AS viajes_promedio_dia,
            ROUND(vehiculos_promedio, 2) AS vehiculos_promedio,
            ROUND(viajes_por_vehiculo, 2) AS viajes_por_vehiculo,
            ROUND(tarifa_media, 2) AS tarifa_media,
            ROUND(distancia_media_km, 2) AS distancia_media_km,
            intervalos_gps
        FROM `{PROJECT_ID}.gold.fact_oferta_demanda`
        WHERE {" AND ".join(conditions)}
        ORDER BY viajes_por_vehiculo DESC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    try:
        rows = [dict(row) for row in bq_client.query(query, job_config=job_config)]
        return {"total": len(rows), "datos": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/v1/zonas-criticas", tags=["Analítica Gold"])
def get_zonas_criticas(top: int = Query(5, ge=1, le=50)):
    """Devuelve las zonas y franjas horarias con mayor déficit de vehículos."""
    query = f"""
        SELECT
            o.zona_id,
            z.nombre AS zona_nombre,
            o.hora_inicio,
            o.periodo,
            ROUND(o.viajes_promedio_dia, 1) AS viajes_dia,
            ROUND(o.vehiculos_promedio, 1) AS vehiculos_disponibles,
            ROUND(o.viajes_por_vehiculo, 1) AS viajes_por_vehiculo
        FROM `{PROJECT_ID}.gold.fact_oferta_demanda` o
        LEFT JOIN `{PROJECT_ID}.gold.dim_zona` z ON o.zona_id = z.zona_id
        WHERE o.intervalos_gps > 0
        ORDER BY o.viajes_por_vehiculo DESC
        LIMIT @top
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("top", "INT64", top)]
    )
    try:
        rows = [dict(row) for row in bq_client.query(query, job_config=job_config)]
        return {"top_criticas": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/v1/telemetria/reciente", tags=["Tiempo Real Silver"])
def get_telemetria_reciente(
    zona_id: Optional[int] = Query(None, description="Filtrar por zona"),
    limit: int = Query(20, ge=1, le=200),
):
    """Consulta las últimas posiciones GPS procesadas por Beam en silver.gps_vehiculo."""
    where_clause = "WHERE zona_id = @zona_id" if zona_id is not None else ""
    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    if zona_id is not None:
        params.append(bigquery.ScalarQueryParameter("zona_id", "INT64", zona_id))

    query = f"""
        SELECT
            evento_id,
            vehiculo_id,
            zona_id,
            lat,
            lon,
            ROUND(velocidad_kmh, 1) AS velocidad_kmh,
            CAST(ts AS STRING) AS ts_evento,
            CAST(procesado_en AS STRING) AS procesado_en
        FROM `{PROJECT_ID}.silver.gps_vehiculo`
        {where_clause}
        ORDER BY procesado_en DESC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    try:
        rows = [dict(row) for row in bq_client.query(query, job_config=job_config)]
        return {"total": len(rows), "eventos": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/v1/kpis/resumen", tags=["Analítica Gold"])
def get_kpis_resumen():
    """Resumen ejecutivo consolidado por periodo del día."""
    query = f"""
        SELECT
            periodo,
            ROUND(SUM(viajes_promedio_dia), 0) AS viajes_promedio_dia,
            ROUND(AVG(vehiculos_promedio), 1) AS vehiculos_promedio,
            ROUND(AVG(viajes_por_vehiculo), 1) AS viajes_por_vehiculo
        FROM `{PROJECT_ID}.gold.fact_oferta_demanda`
        WHERE intervalos_gps > 0
        GROUP BY periodo
        ORDER BY viajes_por_vehiculo DESC
    """
    try:
        rows = [dict(row) for row in bq_client.query(query)]
        return {"resumen_por_periodo": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
