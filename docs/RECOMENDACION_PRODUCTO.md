# Recomendación al equipo de producto (pregunta 7)

Cifras obtenidas ejecutando el pipeline con los CSV reales de Kaggle (`data.csv` y `online_retail.csv`), periodo 1-dic-2010 a 9-dic-2011. Se reproducen con `sql/validation/07_recomendacion_para_producto.sql` y `03_top10_productos_neto_y_devolucion.sql`.

## Recomendación

**Revisar de inmediato la familia "CHANDELIER T-LIGHT HOLDER" (códigos 23055, 23056 y 23057) y el PANTRY CHOPPING BOARD (23113); no tocar el resto del catálogo por devoluciones.**

| Producto | Unidades vendidas | Unidades devueltas | Tasa de devolución | Devoluciones registradas |
|---|---:|---:|---:|---:|
| 23055 IVORY CHANDELIER T-LIGHT HOLDER | 657 | 432 | 65,8 % | 5 clientes |
| 23056 FLOWERS CHANDELIER T-LIGHT HOLDER | 963 | 579 | 60,1 % | 7 clientes |
| 23057 BEADED CHANDELIER T-LIGHT HOLDER | 773 | 433 | 56,0 % | 6 clientes |
| 23113 PANTRY CHOPPING BOARD | 1.154 | 946 | 82,0 % | 5 clientes |

**Por qué estos y no otros.** El ranking de tasa de devolución engaña si se mira solo el porcentaje:

- Dos pedidos explican **el 51,7 % de todo el dinero devuelto** (£245.949 de £475.901): PAPER CRAFT, LITTLE BIRDIE (80.995 unidades compradas y canceladas por un solo cliente) y MEDIUM CERAMIC TOP STORAGE JAR (74.494 de 78.033 unidades devueltas, casi todo en una sola línea de 74.215). Son pedidos mayoristas cancelados, no un problema del producto. Sin ellos, la tasa de devolución global baja de **4,6 % a 2,3 %**. Lo mismo ocurre con 84347 (99 % devuelto, pero 9.360 de 9.376 unidades en una sola línea) y 21108 (3.114 de 3.150).
- Los chandelier y la tabla de picar, en cambio, se devuelven **repartidos entre 5 y 7 clientes distintos por producto**, con líneas de entre 144 y 756 unidades: es un patrón de insatisfacción repetido, no un pedido aislado.
- La familia chandelier perdió **£3.186 de £6.062 vendidos (52,6 %)**; el neto real de la familia es £2.876 sobre £6.062 de bruto.

**Acción concreta:** (1) inspeccionar calidad, empaque y descripción de las 3 referencias chandelier y de la tabla de picar, y pausar su promoción hasta saber el motivo; (2) en los reportes, marcar los pedidos cancelados masivos para que no distorsionen las tasas de devolución (por ejemplo, el mes de **diciembre de 2011 muestra 28,3 % de devolución** y enero de 2011 13,7 %, casi por completo por esos dos pedidos).

## Segundo hallazgo: el negocio depende de pocas referencias

- El **10 % de las referencias (381 de 3.810) genera el 60,6 % del revenue neto**, y el **20 % superior, el 78,0 %**. Las 5 deciles inferiores suman solo el **4,2 %**.
- Los diez primeros por revenue neto son REGENCY CAKESTAND 3 TIER (£164.459), WHITE HANGING HEART T-LIGHT HOLDER (£99.791), PARTY BUNTING (£98.244), JUMBO BAG RED RETROSPOT (£92.176) y RABBIT NIGHT LIGHT (£66.662), entre otros.

**Acción concreta:** proteger inventario y calidad de ese 10 % superior (evitar quiebres de stock en las referencias top) y revisar si mantener la mitad inferior del catálogo (que aporta ≈ 4 % del neto) justifica su costo de inventario.

## Contexto de las cifras
- Revenue bruto £10.246.372 · devoluciones £475.901 (4,6 %) · neto £9.770.471.
- Las categorías se calcularon con las reglas por palabras clave (la API simulada usa las mismas), por lo que son una aproximación; la conclusión de arriba se apoya en productos individuales, no en categorías.
- El dataset termina el 9-dic-2011: diciembre de 2011 es un mes parcial.
