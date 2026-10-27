-- P2. Categorias con mas revenue bruto y con mayor proporcion de devoluciones
WITH ventas AS (
    SELECT p.category, SUM(s.gross_revenue) AS bruto
    FROM dw.fact_sales s JOIN dw.dim_product p USING (product_key)
    GROUP BY p.category
), devol AS (
    SELECT p.category, SUM(r.return_amount) AS devuelto
    FROM dw.fact_returns r JOIN dw.dim_product p USING (product_key)
    GROUP BY p.category
)
SELECT v.category                                   AS categoria,
       ROUND(v.bruto, 2)                            AS revenue_bruto,
       ROUND(COALESCE(d.devuelto, 0), 2)            AS monto_devuelto,
       ROUND(100.0 * COALESCE(d.devuelto, 0) / v.bruto, 2) AS pct_devolucion,
       RANK() OVER (ORDER BY v.bruto DESC)          AS rank_revenue,
       RANK() OVER (ORDER BY COALESCE(d.devuelto, 0) / v.bruto DESC) AS rank_devolucion
FROM ventas v LEFT JOIN devol d USING (category)
ORDER BY v.bruto DESC;
