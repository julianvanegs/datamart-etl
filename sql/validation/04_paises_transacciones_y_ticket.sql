-- P4. Paises que concentran las transacciones y ticket promedio
-- Transaccion = factura (invoice_no). Ticket promedio = revenue bruto / facturas distintas.
WITH por_pais AS (
    SELECT c.country_name AS pais,
           COUNT(DISTINCT s.invoice_no) AS facturas,
           COUNT(*)                     AS lineas,
           SUM(s.gross_revenue)         AS revenue_bruto
    FROM dw.fact_sales s JOIN dw.dim_country c USING (country_key)
    GROUP BY c.country_name
)
SELECT pais, facturas, lineas,
       ROUND(100.0 * facturas / SUM(facturas) OVER (), 2)                         AS pct_de_facturas,
       ROUND(100.0 * SUM(facturas) OVER (ORDER BY facturas DESC) / SUM(facturas) OVER (), 2) AS pct_acumulado,
       ROUND(revenue_bruto, 2)                                                     AS revenue_bruto,
       ROUND(revenue_bruto / facturas, 2)                                          AS ticket_promedio
FROM por_pais
ORDER BY facturas DESC;
