{{ config(
    partition_by={'field': 'fecha_local', 'data_type': 'date'},
    cluster_by=['zona_origen_id']
) }}

SELECT
  viaje_id,
  {{ fecha_local('inicio_utc') }} AS fecha_local,
  {{ franja_id('inicio_utc') }} AS franja_id,
  zona_origen_id,
  zona_destino_id,
  inicio_utc,
  distancia_km,
  duracion_min,
  tarifa,
  SAFE_DIVIDE(tarifa, distancia_km) AS tarifa_por_km
FROM {{ source('silver', 'viajes') }}
