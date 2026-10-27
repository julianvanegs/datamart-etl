-- P1. Evolucion mensual de las ventas netas (descontando devoluciones)
-- Neto = revenue bruto - devoluciones del mismo producto en el mismo dia (tabla agg_daily_product_net).
SELECT d.year_month                        AS mes,
       ROUND(SUM(a.gross_revenue), 2)      AS revenue_bruto,
       ROUND(SUM(a.returns_amount), 2)     AS devoluciones,
       ROUND(SUM(a.net_revenue), 2)        AS ventas_netas,
       ROUND(100.0 * SUM(a.returns_amount) / NULLIF(SUM(a.gross_revenue), 0), 2) AS pct_devuelto
FROM dw.agg_daily_product_net a
JOIN dw.dim_date d USING (date_key)
GROUP BY d.year_month
ORDER BY d.year_month;
