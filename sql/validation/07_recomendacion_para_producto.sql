-- P7. Base numerica para la recomendacion al equipo de producto
-- Hipotesis: hay referencias que venden mucho en bruto pero "devuelven" gran parte de ese ingreso,
-- por lo que su aporte real es mucho menor. Ranking por revenue bruto perdido en devoluciones.
SELECT p.stock_code, p.canonical_description AS producto, p.category AS categoria,
       SUM(a.units_sold)                          AS unidades_vendidas,
       ROUND(SUM(a.gross_revenue), 2)             AS revenue_bruto,
       ROUND(SUM(a.returns_amount), 2)            AS monto_devuelto,
       ROUND(SUM(a.net_revenue), 2)               AS revenue_neto,
       ROUND(100.0 * SUM(a.returns_amount) / NULLIF(SUM(a.gross_revenue), 0), 1) AS pct_devuelto,
       p.is_active
FROM dw.agg_daily_product_net a
JOIN dw.dim_product p USING (product_key)
GROUP BY p.stock_code, p.canonical_description, p.category, p.is_active
HAVING SUM(a.gross_revenue) > 0
ORDER BY SUM(a.returns_amount) DESC
LIMIT 15;

-- Concentracion: que porcentaje del revenue neto aportan el 10% y el 20% de las referencias
WITH por_producto AS (
    SELECT product_key, SUM(net_revenue) AS neto FROM dw.agg_daily_product_net GROUP BY product_key
), ordenado AS (
    SELECT neto, NTILE(10) OVER (ORDER BY neto DESC) AS decil FROM por_producto
)
SELECT decil, ROUND(100.0 * SUM(neto) / SUM(SUM(neto)) OVER (), 2) AS pct_del_revenue_neto
FROM ordenado GROUP BY decil ORDER BY decil;
