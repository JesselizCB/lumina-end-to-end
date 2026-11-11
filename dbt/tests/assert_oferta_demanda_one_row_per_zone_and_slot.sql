-- Fails when the cross is missing a zone/slot pair or duplicates one.
WITH esperado AS (
  SELECT
    (SELECT COUNT(*) FROM {{ ref('dim_zona') }})
    * (SELECT COUNT(*) FROM {{ ref('dim_franja') }}) AS filas
),

real AS (
  SELECT
    COUNT(*) AS filas,
    COUNT(DISTINCT FORMAT('%d-%d', zona_id, franja_id)) AS pares
  FROM {{ ref('fact_oferta_demanda') }}
)

SELECT *
FROM esperado CROSS JOIN real
WHERE real.filas != esperado.filas OR real.pares != esperado.filas
