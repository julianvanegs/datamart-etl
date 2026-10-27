# Preguntas de negocio: consulta y respuesta

Autor: Julián Vanegas · Cifras obtenidas ejecutando el pipeline con los CSV reales de Kaggle (`data.csv` y `online_retail.csv`, 1-dic-2010 a 9-dic-2011) y las consultas de [`sql/validation/`](../sql/validation/). Cada consulta se puede ejecutar tal cual contra el esquema `dw` después de correr el DAG.

Definiciones usadas: **revenue bruto** = cantidad × precio unitario de las ventas; **devoluciones** = cantidad ≤ 0 valorada a su precio; **revenue neto** = bruto − devoluciones del mismo producto en el mismo día (UTC). Moneda del dataset: libras (£). Total del periodo: bruto £10.246.372, devoluciones £475.901 (4,6 %), neto £9.770.471.

---

## 1. ¿Cuál fue la evolución mensual de las ventas netas (descontando devoluciones)?
Consulta: [`01_evolucion_mensual_ventas_netas.sql`](../sql/validation/01_evolucion_mensual_ventas_netas.sql)

| Mes | Revenue bruto | Devoluciones | Ventas netas | % devuelto |
|---|---:|---:|---:|---:|
| 2010-12 | 775.638 | 17.548 | 758.091 | 2,26 |
| 2011-01 | 670.380 | 91.525 | 578.855 | 13,65 |
| 2011-02 | 507.783 | 8.336 | 499.448 | 1,64 |
| 2011-03 | 689.708 | 10.504 | 679.204 | 1,52 |
| 2011-04 | 515.371 | 33.316 | 482.055 | 6,46 |
| 2011-05 | 739.902 | 8.948 | 730.954 | 1,21 |
| 2011-06 | 737.559 | 13.714 | 723.845 | 1,86 |
| 2011-07 | 688.145 | 11.407 | 676.738 | 1,66 |
| 2011-08 | 724.197 | 22.927 | 701.270 | 3,17 |
| 2011-09 | 1.028.314 | 16.981 | 1.011.333 | 1,65 |
| 2011-10 | 1.103.311 | 41.613 | 1.061.698 | 3,77 |
| 2011-11 | 1.451.576 | 24.991 | 1.426.584 | 1,72 |
| 2011-12 | 614.488 | 174.091 | 440.396 | 28,33 |

**Lectura:** las ventas netas pasan de ≈ £500.000–£750.000 al mes en el primer semestre a más de £1 millón entre septiembre y noviembre de 2011 (temporada de fin de año), con noviembre como el mejor mes (£1.426.584). Dos meses se ven anormales por **pedidos masivos cancelados**, no por un problema general: enero de 2011 (13,65 %) y diciembre de 2011 (28,33 %). Además, **diciembre de 2011 es un mes parcial** (el dataset termina el 9-dic), por lo que su neto bajo no indica caída de ventas.

---

## 2. ¿Qué categorías generaron más revenue bruto y cuáles tuvieron mayor proporción de devoluciones?
Consulta: [`02_categorias_revenue_y_devoluciones.sql`](../sql/validation/02_categorias_revenue_y_devoluciones.sql)

| Categoría | Revenue bruto | Monto devuelto | % devolución | Rank revenue | Rank devolución |
|---|---:|---:|---:|---:|---:|
| Hogar | 8.586.589 | 281.730 | 3,28 | 1 | 2 |
| Papelería | 1.104.606 | 182.526 | 16,52 | 2 | 1 |
| Electrónica | 380.144 | 9.089 | 2,39 | 3 | 3 |
| Ropa | 116.974 | 2.368 | 2,02 | 4 | 4 |
| Deportes | 58.060 | 187 | 0,32 | 5 | 5 |

**Lectura:** **Hogar** genera el 84 % del revenue bruto. **Papelería** tiene la mayor proporción de devoluciones (16,5 %), pero casi todo viene de un solo pedido: el producto 23843 *PAPER CRAFT, LITTLE BIRDIE* (£168.470 comprados y cancelados) explica el 92 % de lo devuelto en la categoría; sin él la tasa de Papelería baja a ≈ 1,5 %.
**Advertencia:** el dataset no trae categoría; se asignó con la API de productos simulada, que clasifica por palabras clave de la descripción. Son aproximaciones, por lo que las conclusiones por categoría deben leerse con cautela.

---

## 3. ¿Cuáles son los 10 productos con mayor revenue neto y cuáles los 10 con mayor tasa de devolución?
Consulta: [`03_top10_productos_neto_y_devolucion.sql`](../sql/validation/03_top10_productos_neto_y_devolucion.sql)

**Top 10 por revenue neto**

| # | Código | Producto | Categoría | Revenue neto |
|---|---|---|---|---:|
| 1 | 22423 | REGENCY CAKESTAND 3 TIER | Hogar | 164.459 |
| 2 | 85123A | WHITE HANGING HEART T-LIGHT HOLDER | Hogar | 99.791 |
| 3 | 47566 | PARTY BUNTING | Hogar | 98.244 |
| 4 | 85099B | JUMBO BAG RED RETROSPOT | Hogar | 92.176 |
| 5 | 23084 | RABBIT NIGHT LIGHT | Hogar | 66.662 |
| 6 | 22086 | PAPER CHAIN KIT 50'S CHRISTMAS | Papelería | 63.715 |
| 7 | 84879 | ASSORTED COLOUR BIRD ORNAMENT | Hogar | 58.792 |
| 8 | 79321 | CHILLI LIGHTS | Electrónica | 53.747 |
| 9 | 22502 | PICNIC BASKET WICKER SMALL | Hogar | 51.024 |
| 10 | 22197 | POPCORN HOLDER | Hogar | 50.968 |

**Top 10 por tasa de devolución** (unidades devueltas / vendidas, mínimo 50 unidades vendidas)

| Código | Producto | Vendidas | Devueltas | Tasa |
|---|---|---:|---:|---:|
| 23843 | PAPER CRAFT, LITTLE BIRDIE | 80.995 | 80.995 | 100,00 % |
| 84347 | ROTATING SILVER ANGELS T-LIGHT HLDR | 9.461 | 9.376 | 99,10 % |
| 23166 | MEDIUM CERAMIC TOP STORAGE JAR | 78.033 | 74.494 | 95,46 % |
| 85232B | SET OF 3 BABUSHKA STACKING TINS | 274 | 255 | 93,07 % |
| 23113 | PANTRY CHOPPING BOARD | 1.154 | 946 | 81,98 % |
| 85023B | EAU DE NILE JEWELLED PHOTOFRAME | 105 | 73 | 69,52 % |
| 23055 | IVORY CHANDELIER T-LIGHT HOLDER | 657 | 432 | 65,75 % |
| 23056 | FLOWERS CHANDELIER T-LIGHT HOLDER | 963 | 579 | 60,12 % |
| 23057 | BEADED CHANDELIER T-LIGHT HOLDER | 773 | 433 | 56,02 % |
| 47351B | BLUE VOILE LAMPSHADE | 86 | 48 | 55,81 % |

**Lectura:** los 4 primeros de la lista de devoluciones son pedidos masivos cancelados (una sola línea concentra casi todas las unidades devueltas), no productos defectuosos. Los chandelier y la tabla de picar, en cambio, se devuelven repartidos entre 5 y 7 clientes distintos: ahí sí hay un patrón de producto (ver pregunta 7).

---

## 4. ¿Qué países concentran la mayor parte de las transacciones y cómo varía el ticket promedio?
Consulta: [`04_paises_transacciones_y_ticket.sql`](../sql/validation/04_paises_transacciones_y_ticket.sql) · Transacción = factura; ticket = revenue bruto / facturas.

| País | Facturas | % de facturas | Revenue bruto | Ticket promedio |
|---|---:|---:|---:|---:|
| United Kingdom | 17.901 | 90,53 | 8.723.597 | 487 |
| Germany | 443 | 2,24 | 205.381 | 464 |
| France | 382 | 1,93 | 184.493 | 483 |
| Ireland | 282 | 1,43 | 270.851 | 960 |
| Belgium | 98 | 0,50 | 36.927 | 377 |
| Netherlands | 93 | 0,47 | 283.889 | 3.053 |
| Spain | 88 | 0,45 | 55.707 | 633 |
| Australia | 56 | 0,28 | 138.104 | 2.466 |

(38 países en total; el resto de países suma menos del 3 % de las facturas.)

**Lectura:** **Reino Unido concentra el 90,5 % de las facturas**, y con Alemania, Francia e Irlanda se llega al 96,1 %. El ticket promedio es de £380–£640 en la mayoría de países europeos, pero **Países Bajos (£3.053), Australia (£2.466) y Suecia (£1.083)** tienen tickets muy altos con pocas facturas (compras mayoristas); con 56–93 facturas conviene no sobreinterpretar su promedio.

---

## 5. ¿Hay diferencia entre clientes identificados y transacciones sin customer ID?
Consulta: [`05_clientes_identificados_vs_anonimos.sql`](../sql/validation/05_clientes_identificados_vs_anonimos.sql) (aplica porque se decidió **incluir** las compras sin cliente).

| Indicador | Identificado | Sin customer ID |
|---|---:|---:|
| Facturas | 18.402 | 1.371 |
| Líneas | 391.148 | 131.363 |
| Revenue bruto | £8.736.676 | £1.509.697 |
| % del revenue | 85,27 % | 14,73 % |
| Ticket promedio | £474,77 | £1.101,16 |
| Líneas por factura | 21,3 | 95,8 |
| Unidades por línea | 13,1 | 3,2 |
| Precio unitario promedio | £2,87 | £4,46 |

**Lectura:** sí, hay diferencia clara. Las compras sin ID son pocas (7 % de las facturas) pero **facturas mucho más grandes**: ticket 2,3 veces mayor, 4,5 veces más líneas por factura, menos unidades por línea y precio unitario más alto. Excluirlas habría ocultado el **14,7 % del ingreso (£1,5 millones)**, que es la razón por la que se incluyeron con `customer_key = 0`.

---

## 6. ¿Qué productos aparecen sin descripción consistente? ¿Cuántos códigos únicos hay?
Consulta: [`06_productos_descripcion_inconsistente_y_codigos_unicos.sql`](../sql/validation/06_productos_descripcion_inconsistente_y_codigos_unicos.sql)

- **Códigos únicos de producto en el repositorio: 3.810** (3.940 si se cuentan los que solo aparecen en filas rechazadas por precio ≤ 0), más 18 códigos que no son productos (`POST`, `D`, `M`, `BANK CHARGES`, `gift_*`...) rechazados como `non_catalog_stock_code`.
- **Productos con descripción inconsistente: 207** (5,4 %), es decir, con más de un texto distinto después de normalizar mayúsculas y espacios. Ninguno difiere solo por mayúsculas: en los datos reales la inconsistencia es de texto real.

Ejemplos (descripción canónica y variantes observadas):

| Código | Canónica | Variantes |
|---|---|---|
| 23196 | VINTAGE LEAF MAGNETIC NOTEPAD | 4 (ej. RETRO LEAVES MAGNETIC NOTEPAD) |
| 23236 | STORAGE TIN VINTAGE DOILY | 4 (ej. DOILEY STORAGE TIN) |
| 22776 | SWEETHEART CAKESTAND 3 TIER | 3 (ej. SWEETHEART 3 TIER CAKE STAND) |
| 23131 | MISTLETOE HEART WREATH CREAM | 3 (incluye el error de escritura MISELTOE) |

**Lectura:** las variantes son sobre todo reordenamientos, errores de digitación (`DOILEY`/`DOILY`, `MISELTOE`) y cambios de color o estilo para el mismo código. Se resolvió con la descripción **más frecuente**; las demás quedan en `product_description_variant` para auditoría.

---

## 7. Recomendación concreta para el equipo de producto
Consulta: [`07_recomendacion_para_producto.sql`](../sql/validation/07_recomendacion_para_producto.sql) · Análisis completo en [`RECOMENDACION_PRODUCTO.md`](RECOMENDACION_PRODUCTO.md).

**Recomendación:** revisar de inmediato la familia **CHANDELIER T-LIGHT HOLDER** (23055, 23056, 23057), que perdió **£3.186 de £6.062 vendidos (52,6 %)**, y la **PANTRY CHOPPING BOARD** (23113, 82 % de unidades devueltas); sus devoluciones están repartidas entre 5 y 7 clientes por producto, lo que indica un problema de calidad o de descripción y no un pedido aislado.

**Respaldo en números:**
- Dos pedidos masivos cancelados (23843 y 23166) explican el **51,7 % de todo el dinero devuelto** (£245.949 de £475.901). Sin ellos, la tasa de devolución global baja de 4,6 % a 2,3 %, así que no deben guiar decisiones de calidad.
- El **10 % de las referencias (381 de 3.810) genera el 60,6 % del revenue neto**, y el 20 % superior el 78,0 %; las cinco deciles inferiores suman solo el 4,2 %. Hay que proteger el inventario de ese grupo superior y revisar si la mitad inferior del catálogo justifica su costo.
