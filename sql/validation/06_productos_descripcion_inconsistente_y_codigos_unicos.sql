-- P6a. Productos con descripcion inconsistente (mas de una descripcion DESPUES de normalizar mayusculas y espacios)
SELECT p.stock_code, p.canonical_description AS descripcion_canonica,
       p.normalized_variant_count AS variantes_reales, p.raw_variant_count AS variantes_de_escritura,
       (SELECT string_agg(v.raw_description || ' (' || v.occurrences || ')', ' | ' ORDER BY v.occurrences DESC)
          FROM dw.product_description_variant v WHERE v.product_key = p.product_key) AS descripciones_observadas
FROM dw.dim_product p
WHERE p.normalized_variant_count > 1
ORDER BY p.normalized_variant_count DESC, p.stock_code;

-- P6b. Resumen de consistencia
SELECT COUNT(*)                                            AS codigos_unicos_de_producto,
       COUNT(*) FILTER (WHERE normalized_variant_count > 1) AS con_descripcion_inconsistente,
       COUNT(*) FILTER (WHERE raw_variant_count > normalized_variant_count) AS solo_difieren_en_mayusculas_o_espacios,
       COUNT(*) FILTER (WHERE canonical_description = 'SIN DESCRIPCION')    AS sin_ninguna_descripcion
FROM dw.dim_product;

-- P6c. Codigos unicos en total, contando tambien los rechazados (ej. POST, BANK CHARGES, gift_*)
SELECT (SELECT COUNT(*) FROM dw.dim_product) AS codigos_validos,
       (SELECT COUNT(DISTINCT UPPER(REGEXP_REPLACE(stock_code_raw, '\s+', '', 'g')))
          FROM dw.rejected_records
         WHERE reject_reason = 'non_catalog_stock_code'
           AND run_date = (SELECT MAX(run_date) FROM dw.rejected_records)) AS codigos_no_catalogo_rechazados;
