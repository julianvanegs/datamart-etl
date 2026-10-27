-- P5. Comportamiento de compra: clientes identificados vs transacciones sin customer ID
-- (aplica porque se decidio INCLUIR las compras sin cliente con customer_key = 0)
SELECT CASE WHEN c.is_identified THEN 'Identificado' ELSE 'Sin customer ID' END AS tipo_cliente,
       COUNT(DISTINCT s.invoice_no)                         AS facturas,
       COUNT(*)                                             AS lineas,
       ROUND(SUM(s.gross_revenue), 2)                       AS revenue_bruto,
       ROUND(SUM(s.gross_revenue) / COUNT(DISTINCT s.invoice_no), 2) AS ticket_promedio,
       ROUND(COUNT(*)::numeric / COUNT(DISTINCT s.invoice_no), 2)     AS lineas_por_factura,
       ROUND(AVG(s.quantity), 2)                            AS unidades_por_linea,
       ROUND(AVG(s.unit_price), 2)                          AS precio_unitario_promedio,
       ROUND(100.0 * SUM(s.gross_revenue) / SUM(SUM(s.gross_revenue)) OVER (), 2) AS pct_del_revenue
FROM dw.fact_sales s
JOIN dw.dim_customer c USING (customer_key)
GROUP BY c.is_identified
ORDER BY c.is_identified DESC;

-- Impacto de la decision: lo que se perderia si se excluyeran
SELECT COUNT(*) AS lineas_sin_cliente,
       ROUND(SUM(gross_revenue), 2) AS revenue_bruto_sin_cliente
FROM dw.fact_sales WHERE customer_key = 0;
