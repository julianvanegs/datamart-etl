# Recomendación al equipo de producto (pregunta 7)

> **Completar después de correr el pipeline con los CSV reales de Kaggle.** Con los datos de prueba incluidos (`seeds/`) las cifras no son representativas, por eso aquí no se inventan números.

## Cómo obtener los números
1. Ejecuta `sql/validation/07_recomendacion_para_producto.sql` (ranking de productos que más ingreso pierden por devoluciones y concentración del revenue neto por deciles).
2. Complementa con `02_categorias_revenue_y_devoluciones.sql` y `03_top10_productos_neto_y_devolucion.sql`.

## Plantilla de la recomendación (rellena con tus cifras)
- **Hallazgo:** "Las `__` referencias con mayor monto devuelto representan `__ %` de las devoluciones, pero solo `__ %` del revenue bruto."
- **Evidencia:** productos `__`, `__`, `__` con tasa de devolución de `__ %` frente a un promedio de `__ %`.
- **Recomendación concreta:** revisar calidad/descripción/empaque de esas referencias (o pausar su promoción) y proteger las `__` referencias del decil superior, que aportan `__ %` del revenue neto.
- **Impacto estimado:** reducir a la mitad la tasa de devolución de esas referencias recuperaría ≈ `__` de revenue neto en el periodo.
