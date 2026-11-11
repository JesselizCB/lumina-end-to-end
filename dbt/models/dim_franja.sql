-- One row per 15-minute slot of the day: franja_id 0 is 00:00, 95 is 23:45.
SELECT
  franja_id,
  FORMAT('%02d:%02d', DIV(franja_id, 4), MOD(franja_id, 4) * 15) AS hora_inicio,
  DIV(franja_id, 4) AS hora,
  MOD(franja_id, 4) * 15 AS minuto,
  CASE
    WHEN DIV(franja_id, 4) < 6 THEN 'madrugada'
    WHEN DIV(franja_id, 4) < 12 THEN 'manana'
    WHEN DIV(franja_id, 4) < 18 THEN 'tarde'
    ELSE 'noche'
  END AS periodo
FROM UNNEST(GENERATE_ARRAY(0, 95)) AS franja_id
