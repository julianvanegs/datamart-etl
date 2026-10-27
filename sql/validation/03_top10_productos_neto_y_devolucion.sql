-- P3a. Top 10 productos por revenue neto
SELECT p.stock_code, p.canonical_description AS producto, p.category AS categoria,
       ROUND(SUM(a.net_revenue), 2) AS revenue_neto
FROM dw.agg_daily_product_net a
JOIN dw.dim_product p USING (product_key)
GROUP BY p.stock_code, p.canonical_description, p.category
ORDER BY revenue_neto DESC
LIMIT 10;

-- P3b. Top 10 productos por tasa de devolucion (unidades devueltas / unidades vendidas)
-- Se exige un minimo de 50 unidades vendidas para que la tasa no la dominen productos de 1 o 2 ventas.
SELECT p.stock_code, p.canonical_description AS producto, p.category AS categoria,
       SUM(a.units_sold) AS unidades_vendidas, SUM(a.units_returned) AS unidades_devueltas,
       ROUND(100.0 * SUM(a.units_returned) / SUM(a.units_sold), 2) AS tasa_devolucion_pct
FROM dw.agg_daily_product_net a
JOIN dw.dim_product p USING (product_key)
GROUP BY p.stock_code, p.canonical_description, p.category
HAVING SUM(a.units_sold) >= 50
ORDER BY tasa_devolucion_pct DESC
LIMIT 10;
