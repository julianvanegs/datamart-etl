-- Perfil de calidad de la ultima ejecucion: que entro, que se cargo y que se rechazo (y por que)
SELECT run_date,
       (metrics->>'rows_in_total')::int  AS filas_leidas,
       (metrics->>'sales_rows')::int     AS ventas_cargadas,
       (metrics->>'returns_rows')::int   AS devoluciones_cargadas,
       (metrics->>'rejected_rows')::int  AS filas_rechazadas,
       ROUND(100.0 * (metrics->>'rejected_rows')::int / (metrics->>'rows_in_total')::int, 2) AS pct_rechazo_total,
       ROUND(100.0 * (SELECT COALESCE(SUM((e->>'n')::int), 0)
                        FROM jsonb_array_elements(metrics->'rejected_by_reason') e
                       WHERE e->>'reject_reason' <> 'duplicate_across_sources_history_overlap')
             / (metrics->>'rows_in_total')::int, 2) AS pct_rechazo_calidad,
       (metrics->>'anonymous_customer_lines')::int AS lineas_sin_customer_id
FROM dw.etl_run_summary ORDER BY run_date DESC LIMIT 1;

SELECT source_system AS fuente, reject_reason AS motivo, COUNT(*) AS filas
FROM dw.rejected_records
WHERE run_date = (SELECT MAX(run_date) FROM dw.rejected_records)
GROUP BY 1, 2 ORDER BY 1, 3 DESC;

-- Rango de fechas cubierto (UTC) y de donde viene cada venta
SELECT source_system AS fuente, MIN(invoice_ts_utc) AS desde, MAX(invoice_ts_utc) AS hasta, COUNT(*) AS lineas
FROM dw.fact_sales GROUP BY 1;
