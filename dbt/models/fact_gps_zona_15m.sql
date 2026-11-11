{{ config(
    partition_by={'field': 'fecha_local', 'data_type': 'date'},
    cluster_by=['zona_id']
) }}

-- Vehicle supply observed per zone and 15-minute interval.
-- Rows written before the modo column existed are real-time events.
SELECT
  zona_id,
  ts_15m,
  COALESCE(modo, 'tiempo_real') AS modo,
  {{ fecha_local('ts_15m') }} AS fecha_local,
  {{ franja_id('ts_15m') }} AS franja_id,
  COUNT(*) AS eventos,
  COUNT(DISTINCT vehiculo_id) AS vehiculos,
  AVG(velocidad_kmh) AS velocidad_media_kmh
FROM {{ source('silver', 'gps_vehiculo') }}
GROUP BY zona_id, ts_15m, COALESCE(modo, 'tiempo_real')
