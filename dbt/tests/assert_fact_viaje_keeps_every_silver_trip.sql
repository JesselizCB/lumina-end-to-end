-- Fails when Gold loses or multiplies trips relative to Silver.
SELECT *
FROM (
  SELECT
    (SELECT COUNT(*) FROM {{ source('silver', 'viajes') }}) AS silver,
    (SELECT COUNT(*) FROM {{ ref('fact_viaje') }}) AS gold
)
WHERE silver != gold
