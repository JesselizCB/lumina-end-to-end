SELECT
  zona_id,
  nombre,
  min_lon,
  min_lat,
  max_lon,
  max_lat
FROM {{ source('silver', 'zonas') }}
