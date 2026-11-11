-- Historical trip demand (TLC, batch) against live vehicle supply (GPS, streaming)
-- for every zone and 15-minute slot of the day. The sources cover different dates,
-- so they are crossed by slot of the day rather than by exact timestamp.

WITH dias_con_viajes AS (
  SELECT COUNT(DISTINCT fecha_local) AS dias
  FROM {{ ref('fact_viaje') }}
),

demanda AS (
  SELECT
    zona_origen_id AS zona_id,
    franja_id,
    COUNT(*) AS viajes,
    AVG(tarifa) AS tarifa_media,
    AVG(distancia_km) AS distancia_media_km
  FROM {{ ref('fact_viaje') }}
  GROUP BY zona_id, franja_id
),

oferta AS (
  SELECT
    zona_id,
    franja_id,
    AVG(vehiculos) AS vehiculos_promedio,
    COUNT(*) AS intervalos_gps
  FROM {{ ref('fact_gps_zona_15m') }}
  GROUP BY zona_id, franja_id
)

SELECT
  z.zona_id,
  f.franja_id,
  f.hora_inicio,
  f.periodo,
  SAFE_DIVIDE(COALESCE(d.viajes, 0), dias_con_viajes.dias) AS viajes_promedio_dia,
  d.tarifa_media,
  d.distancia_media_km,
  o.vehiculos_promedio,
  COALESCE(o.intervalos_gps, 0) AS intervalos_gps,
  SAFE_DIVIDE(
    SAFE_DIVIDE(COALESCE(d.viajes, 0), dias_con_viajes.dias),
    o.vehiculos_promedio
  ) AS viajes_por_vehiculo
FROM {{ ref('dim_zona') }} AS z
CROSS JOIN {{ ref('dim_franja') }} AS f
CROSS JOIN dias_con_viajes
LEFT JOIN demanda AS d
  ON d.zona_id = z.zona_id AND d.franja_id = f.franja_id
LEFT JOIN oferta AS o
  ON o.zona_id = z.zona_id AND o.franja_id = f.franja_id
