# Documento de decisiones técnicas

Autor: Julián Vanegas · Caso: DataMart S.A.S. · Pipeline ETL con Apache Airflow y PostgreSQL

Este documento explica **qué decidí, por qué, y cómo se comprueba** en el código. Cada decisión apunta al archivo donde vive la lógica.

## 1. Arquitectura general

```
 data.csv (diario) ─┐
 online_retail_II.csv (histórico) ─┼─► Airflow (Docker) ─► Repositorio analítico (PostgreSQL, esquema dw)
 API de productos ─┘      │
                          └─ Metadatos de Airflow en OTRA base PostgreSQL (separada)
```

| Servicio | Para qué sirve |
|---|---|
| `airflow-postgres` | Metadatos internos de Airflow (no contiene datos de negocio) |
| `dw-postgres` | Repositorio analítico destino. Las tablas se crean solas con `sql/ddl/` |
| `airflow-init` | Se ejecuta una vez: migra Airflow, crea el administrador, las **Connections** y las **Variables** |
| `airflow-webserver` / `airflow-scheduler` | Interfaz en `localhost:8080` y ejecución automática del DAG |
| `products-api` | Catálogo de productos (sistema legado simulado) + indicadores para cualquier dispositivo |

Los dos datasets de Kaggle pesan cientos de MB, por eso no se versionan. El pipeline busca cada archivo en este orden:
1. `data/raw/` (los CSV reales que descargues),
2. descarga automática desde Kaggle si defines `KAGGLE_USERNAME` y `KAGGLE_KEY` en `.env`,
3. `seeds/` (datos de prueba incluidos, con los mismos problemas de calidad), para que el proyecto corra desde cero sin nada más.

## 2. Exploración de las fuentes

El pipeline **lee todo como texto** y solo convierte tipos dentro de la limpieza, para no perder información (por ejemplo `CustomerID` llega como `17850.0`). Lo que se observó y cómo se trata:

| Hallazgo | Fuente | Tratamiento |
|---|---|---|
| Columnas con nombres distintos (`InvoiceNo`/`Invoice`, `UnitPrice`/`Price`, `CustomerID`/`Customer ID`) | ambas | Se unifican en `sources.py` (`COLUMN_ALIASES`) |
| Fecha `12/1/2010 8:26` vs `2009-12-01 07:45:00`; hora local sin zona | ambas | Formato esperado por fuente + respaldo flexible; luego a UTC |
| Archivo `data.csv` en ISO-8859-1 | diario | Lectura con UTF-8 y, si falla, Latin-1 |
| Cantidades negativas (devoluciones) | ambas | Tabla de hechos separada `fact_returns` |
| Precio 0 o negativo | ambas | Rechazo con motivo |
| Códigos que no son productos (`POST`, `D`, `M`, `BANK CHARGES`, `gift_...`) | ambas | Rechazo `non_catalog_stock_code` |
| Códigos con minúsculas o espacios | ambas | Normalización a MAYÚSCULAS sin espacios |
| Descripciones con distinto uso de mayúsculas | ambas | Descripción canónica (sección 4.2) |
| `CustomerID` vacío | ambas | Se incluyen como cliente `ANONYMOUS` (sección 4.1) |
| Nombres de país distintos (`EIRE`, `RSA`) | ambas | Tabla `COUNTRY_ALIASES` en `transform.py` |
| Filas exactamente repetidas | ambas | Rechazo `duplicate_within_source` |
| Fechas que se solapan entre fuentes | ambas | Prioridad de fuente (sección 4.3) |

Cómo ver el perfil real después de correr el pipeline: `sql/validation/00_perfil_de_calidad.sql` (filas leídas, cargadas, rechazadas y motivos) y la tabla `dw.etl_run_summary` (métricas en JSON por ejecución).

> Los volúmenes exactos dependen de los CSV que cargues. El perfil sale de tu propia ejecución, no de cifras copiadas.

## 3. Modelo del repositorio analítico

Se eligió un **esquema en estrella** con dos tablas de hechos separadas. Diagrama: `docs/modelo_datos.png` (y `docs/modelo_datos.dbml` para pegar en dbdiagram.io).

| Tabla | Contenido | Por qué |
|---|---|---|
| `fact_sales` | Líneas con cantidad > 0 y precio > 0 | Regla del negocio: ventas y devoluciones no se mezclan |
| `fact_returns` | Líneas con cantidad ≤ 0 (cantidad devuelta en positivo + monto) | Permite calcular el neto por producto y periodo |
| `agg_daily_product_net` | Bruto, devoluciones y **neto** por producto y día | Es exactamente la definición del enunciado (devoluciones del mismo producto en el mismo día) y hace baratas las consultas |
| `dim_product` | Una fila por código, descripción canónica, categoría, país proveedor, activo | Cruce con el catálogo |
| `product_description_variant` | Todas las descripciones observadas por producto | Responde "qué productos tienen descripción inconsistente" |
| `dim_customer`, `dim_country`, `dim_date` | Dimensiones | Análisis por cliente, país y mes |
| `rejected_records` | Cada fila rechazada con motivo, fuente, fila original y JSON completo | Trazabilidad exigida |
| `etl_run_summary` | Métricas por ejecución | Auditoría |

Decisiones de diseño:
- **Revenue bruto** = `cantidad × precio unitario`, guardado en `fact_sales.gross_revenue`. **Revenue neto** = bruto − monto devuelto del mismo código en el mismo día (UTC), guardado en `agg_daily_product_net.net_revenue`. Si un producto solo tiene devoluciones un día, su neto de ese día es negativo (así se conserva el dinero).
- Las llaves de las dimensiones son sustitutas (`*_key`); los hechos tienen restricciones `CHECK` (`quantity > 0`, `unit_price > 0`) como segunda línea de defensa.
- `line_id` es un UUID derivado de un hash de la línea (factura, producto, fecha UTC, cantidad, precio, cliente, país). Es la base de la idempotencia.

## 4. Casos ambiguos y cómo los resolví

### 4.1 Transacciones sin customer ID → se **incluyen** con `customer_key = 0` (`ANONYMOUS`)
- **Por qué:** excluirlas elimina ingresos reales. Para las preguntas de ventas, países y productos no necesitan cliente. Solo el análisis por cliente las ignora.
- **Impacto:** se mide con `sql/validation/05_clientes_identificados_vs_anonimos.sql` (líneas y revenue que se perderían). Ese mismo archivo compara su comportamiento con el de los clientes identificados (pregunta 5).
- **Riesgo asumido:** las ventas anónimas no se pueden asociar a un comprador; cualquier métrica "por cliente" debe filtrar `is_identified`.

### 4.2 Descripciones distintas para el mismo código → **la más frecuente**, sin importar mayúsculas
- Se normalizan espacios y se compara en MAYÚSCULAS; la descripción canónica es la que más veces aparece. Si hay empate, la **más larga** (suele ser la más informativa) y luego orden alfabético. Es determinista: siempre elige lo mismo.
- Se guardan `raw_variant_count` (variantes tal como llegaron) y `normalized_variant_count` (variantes reales tras normalizar). Un producto es **inconsistente** cuando `normalized_variant_count > 1`, es decir, cuando el texto realmente cambia y no solo las mayúsculas.

### 4.3 Solapamiento entre los dos datasets → **prioridad al volcado diario**, con clave compuesta
- Una línea del histórico se considera duplicado real si coincide con una del diario en **(factura, producto, fecha-hora UTC, cantidad, precio)**. Se rechaza la del histórico con motivo `duplicate_across_sources_history_overlap`.
- No uso solo el número de factura porque una factura tiene varias líneas; no incluyo cliente ni país porque son los campos que más cambian de formato entre datasets.
- **Por qué prioridad al diario:** representa el sistema operacional vigente; el histórico es carga de respaldo.
- Dentro de una misma fuente, las líneas **idénticas en todo** se tratan como un error de captura y se conserva la primera.

### 4.4 Otras reglas del negocio
| Regla | Implementación |
|---|---|
| Cantidad ≤ 0 = devolución/ajuste | `split_sales_and_returns` |
| Precio ≤ 0 no es venta válida | Rechazo `non_positive_unit_price_sale`. En devoluciones/ajustes sin valor monetario: `non_positive_unit_price_return_or_adjustment` |
| Fechas en UTC | `to_utc`: se asume que el origen está en hora de Londres (variable `datamart_source_timezone`). Hora ambigua del cambio de horario = hora estándar; hora inexistente = se adelanta |
| Códigos en mayúsculas y sin espacios | `normalize_stock_code`, antes de cruzar con el catálogo |
| Códigos fuera del catálogo | El patrón es la variable `datamart_stock_code_pattern`: 5 dígitos + hasta 3 letras, o códigos `DCGS####`. Lo demás (`POST`, `D`, `M`, `BANK CHARGES`, `gift_*`) son servicios, comisiones o vales, no productos |

### 4.5 Categoría de producto
- **Con API** (`datamart_use_products_api = true`): se consume `/products` con paginación y se usa su categoría (`category_source = 'products_api'`).
- **Sin API** (apagada o caída): el pipeline **no falla**; asigna categoría por palabras clave de la descripción (`shared/category_rules.py`, `category_source = 'keyword_rules'`). Es una aproximación, claramente marcada en la tabla. La API usa el mismo módulo, así que ambos caminos coinciden.
- El catálogo de la API se construye con los mismos CSV; `is_active = false` si el producto no se vende en los últimos 90 días del periodo, y el país proveedor es simulado de forma determinista (no existe en los datasets).

## 5. Idempotencia del DAG

Ejecutarlo dos veces con los mismos datos deja **exactamente** el mismo contenido:

| Tabla | Mecanismo |
|---|---|
| Dimensiones | `INSERT ... ON CONFLICT` sobre la clave natural; los productos solo se actualizan si cambió algún atributo |
| `fact_sales`, `fact_returns` | `line_id` determinista + `ON CONFLICT (line_id) DO UPDATE` |
| `agg_daily_product_net` | `TRUNCATE` + recálculo completo desde los hechos |
| `rejected_records` | `DELETE` de la fecha lógica + `INSERT` |
| `etl_run_summary` | `UPSERT` por fecha lógica |

Además: cada tarea de carga corre en **una transacción** (todo o nada), el DAG tiene `max_active_runs = 1`, y los archivos intermedios se guardan por fecha lógica. **Comprobado**: se ejecutó el DAG dos veces con la misma fecha y se compararon conteos, sumas y hashes de todas las tablas: idénticos (única diferencia posible: los ids autoincrementales de `rejected_records`, que no son datos de negocio).

## 6. Orquestación, reintentos y configuración

- DAG `datamart_sales_etl`, `@daily`, `retries = 3` con espera exponencial.
- Tareas: `validate_configuration` → (`extract_sales_daily`, `extract_sales_history`, `extract_products_catalog`) → `transform_and_unify` → `run_quality_checks` → `load_dimensions` → (`load_sales_and_returns`, `load_rejected_records`) → `build_net_revenue_aggregate` → `verify_warehouse_load`.
- **Connections:** `datamart_dw` (base destino) y `products_api`. **Variables:** 9 parámetros operativos (zona horaria, umbral de rechazo, uso de la API, tamaño de página, patrón de códigos, nombres de archivo y datasets de Kaggle). Todas se crean en `scripts/bootstrap_airflow.sh` a partir de `.env`; ningún usuario, clave ni cadena de conexión está en el código.
- **Chequeos de calidad** (`quality.py`), si alguno falla no se carga nada: precios y cantidades válidos, fechas en UTC, códigos normalizados, `line_id` único, todo producto existe en la dimensión, categorías válidas, **cuadre** (filas leídas = ventas + devoluciones + rechazos) y tasa de rechazo bajo el umbral (`datamart_max_rejection_ratio`). La tasa cuenta solo problemas de calidad: el descarte por solapamiento entre fuentes es esperado y no se cuenta, porque el histórico repite cerca de la mitad del diario.

## 7. Integración con cualquier dispositivo

Para que el repositorio sea fácil de consumir desde un celular, una tablet u otro computador, el servicio `products-api`:
- expone **REST con JSON estándar** y documentación interactiva en `/docs`,
- tiene **CORS habilitado** y escucha en `0.0.0.0`,
- ofrece un **panel web adaptable (responsive)** en `/` con ventas netas por mes, top de productos y países,
- se protege con la cabecera `X-API-Key`.

Desde otro dispositivo de la misma red se usa la IP del equipo: `http://<IP-del-equipo>:8000`. Además, el repositorio analítico acepta conexiones SQL en el puerto configurado en `DW_HOST_PORT` (herramientas como DBeaver, Power BI o Metabase).

## 8. Pendientes y mejoras (qué haría con más tiempo)

- **Carga incremental real:** hoy cada ejecución reprocesa los archivos completos (es lo que garantiza idempotencia simple). Con un archivo diario nuevo, se filtraría por fecha lógica.
- **Pruebas de DAG en CI** y más casos de prueba con los CSV reales.
- **Tabla de tipo de cambio:** el enunciado menciona monedas distintas por país; los datasets están en una sola moneda (GBP), por lo que no se convirtió. Si hubiera monedas, se agregaría `dim_currency` y el revenue en una moneda común.
- **Catálogo real:** reemplazar el catálogo simulado por el sistema legado y reemplazar la categoría por palabras clave por una tabla maestra.
- **Recomendación a producto (pregunta 7):** el SQL que la respalda está en `sql/validation/07_recomendacion_para_producto.sql`. Las cifras y la conclusión deben redactarse con **los CSV reales** (ver `docs/RECOMENDACION_PRODUCTO.md`); con los datos de prueba no tendría validez.
