# DataMart ETL · Pipeline con Apache Airflow y PostgreSQL

Pipeline de datos de extremo a extremo para **DataMart S.A.S.**: toma las ventas de dos datasets de Kaggle y el catálogo de productos, aplica las reglas de negocio y deja todo listo para consultar en un repositorio analítico PostgreSQL. Todo corre en Docker, orquestado por Airflow.

Autor: **Julián Vanegas** · Prueba de desempeño, Ingeniería de Datos · Cohorte 7

- 📐 Decisiones técnicas y casos ambiguos: [`docs/DECISIONES_TECNICAS.md`](docs/DECISIONES_TECNICAS.md)
- 🗺️ Diagrama del modelo de datos: [`docs/modelo_datos.png`](docs/modelo_datos.png) · [`docs/modelo_datos.dbml`](docs/modelo_datos.dbml) (pegar en dbdiagram.io)
- 🔎 Consultas SQL de validación (una o más por cada pregunta de negocio): [`sql/validation/`](sql/validation/)

---

## Levantar el entorno en menos de 10 minutos

**Requisito único:** Docker Desktop (o Docker + Docker Compose) instalado y abierto. Se recomienda asignarle 4 GB de memoria si vas a usar los CSV completos de Kaggle.

### 1. Clonar el repositorio
```bash
git clone https://github.com/julianvanegs/datamart-etl.git
cd datamart-etl
```
(También sirve descargar el zip desde GitHub y descomprimirlo.)

### 2. (Opcional) Usar los datos reales de Kaggle
Sin hacer nada, el pipeline intenta **descargar los datasets reales solo** (con `kagglehub`; son públicos) y, si no hay internet o Kaggle no responde, usa los **datos de prueba** de `seeds/` (≈1.700 filas con los mismos problemas de calidad). Si prefieres ponerlos tú, cópialos en `data/raw/` con estos nombres:

- `data.csv` ← <https://www.kaggle.com/datasets/carrie1/ecommerce-data>
- `online_retail.csv` ← <https://www.kaggle.com/datasets/thedevastator/online-retail-transaction-data>

### 3. Levantar todo (elige la forma que te quede más fácil)

| Sistema | Qué hacer |
|---|---|
| **Windows** | Doble clic en **`iniciar.bat`** |
| **Mac / Linux** | `./iniciar.sh` (si no abre: `chmod +x iniciar.sh detener.sh` una vez) |
| **Cualquiera, a mano** | `cp .env.example .env` (Windows: `copy .env.example .env`) y luego `docker compose up --build` |

Los scripts verifican que Docker esté abierto, crean el archivo `.env` con valores por defecto si no existe (en Linux además lo ajustan a tu usuario) y levantan los servicios. La primera vez tarda unos minutos mientras descarga las imágenes; para apagar todo usa `detener.bat` / `./detener.sh`. Sin ningún otro paso manual quedan corriendo:

| Servicio | Dónde |
|---|---|
| Airflow (usuario y clave del `.env`, por defecto `admin` / `cambia_esta_clave_admin`) | <http://localhost:8080> |
| Repositorio analítico PostgreSQL | `localhost:5433` (base `datamart_dw`) |
| API de productos + panel de indicadores | <http://localhost:8000> |

El DAG `datamart_sales_etl` se activa solo y ejecuta la primera corrida automáticamente (con los datos reales tarda ≈ 2 minutos). Para repetirla: en Airflow pulsa ▶ (*Trigger DAG*).

**Único requisito:** Docker Desktop (o Docker + Docker Compose) instalado y abierto. Con los CSV completos conviene asignarle 4 GB de memoria.

### 4. Verificar que Connections y Variables quedaron configuradas
**Con clics:** en Airflow, menú **Admin → Connections** (deben aparecer `datamart_dw` y `products_api`) y **Admin → Variables** (nueve variables `datamart_*`).

**Por línea de comandos:**
```bash
docker compose exec airflow-scheduler airflow connections list
docker compose exec airflow-scheduler airflow variables list
docker compose exec airflow-scheduler airflow connections get datamart_dw
```
La primera tarea del DAG (`validate_configuration`) también comprueba las Variables y la conexión, y falla con un mensaje claro si algo falta.

### 5. Verificar que los datos llegaron al repositorio analítico
Cuando todas las tareas del DAG estén en verde (⏱ segundos con los datos de prueba; unos minutos con los CSV completos):

```bash
docker compose exec dw-postgres psql -U datamart -d datamart_dw -c "
  SELECT 'fact_sales' AS tabla, count(*) FROM dw.fact_sales
  UNION ALL SELECT 'fact_returns', count(*) FROM dw.fact_returns
  UNION ALL SELECT 'dim_product', count(*) FROM dw.dim_product
  UNION ALL SELECT 'rejected_records', count(*) FROM dw.rejected_records;"
```
(Si cambiaste `DW_DB_USER` o `DW_DB_NAME` en el `.env`, usa esos valores.) También puedes conectar DBeaver, pgAdmin o Power BI a `localhost:5433` con las credenciales del `.env`.

### 6. Responder las preguntas de negocio
```bash
docker compose exec -T dw-postgres psql -U datamart -d datamart_dw < sql/validation/01_evolucion_mensual_ventas_netas.sql
```
Cambia el nombre del archivo para cada pregunta:

| Pregunta | Archivo |
|---|---|
| Perfil de calidad de la corrida | `00_perfil_de_calidad.sql` |
| 1. Evolución mensual de ventas netas | `01_evolucion_mensual_ventas_netas.sql` |
| 2. Categorías: revenue bruto y devoluciones | `02_categorias_revenue_y_devoluciones.sql` |
| 3. Top 10 por neto y por tasa de devolución | `03_top10_productos_neto_y_devolucion.sql` |
| 4. Países: transacciones y ticket promedio | `04_paises_transacciones_y_ticket.sql` |
| 5. Clientes identificados vs sin ID | `05_clientes_identificados_vs_anonimos.sql` |
| 6. Descripciones inconsistentes y códigos únicos | `06_productos_descripcion_inconsistente_y_codigos_unicos.sql` |
| 7. Base numérica de la recomendación | `07_recomendacion_para_producto.sql` + [`docs/RECOMENDACION_PRODUCTO.md`](docs/RECOMENDACION_PRODUCTO.md) |

---

## Adaptarlo a tu equipo

Todo lo que cambia de una persona a otra está en el archivo `.env` (no hay que tocar código):

| Quiero... | Cambio en `.env` |
|---|---|
| Que un puerto ocupado no moleste | `AIRFLOW_WEB_PORT`, `PRODUCTS_API_PORT`, `DW_HOST_PORT` |
| Cambiar usuarios y claves | `AIRFLOW_ADMIN_*`, `AIRFLOW_DB_*`, `DW_DB_*`, `PRODUCTS_API_TOKEN` |
| Usar otros archivos o datasets | `DATAMART_SALES_DAILY_FILE`, `DATAMART_SALES_HISTORY_FILE`, `KAGGLE_DATASET_*` |
| Cambiar la zona horaria de origen de las fechas, el umbral de rechazo o apagar la API de productos | `DATAMART_SOURCE_TIMEZONE`, `DATAMART_MAX_REJECTION_RATIO`, `DATAMART_USE_PRODUCTS_API` |

Después de editar `.env`, vuelve a ejecutar `iniciar.bat` / `./iniciar.sh`. Las Variables también se pueden cambiar en vivo desde Airflow (**Admin → Variables**) sin reiniciar nada.

Funciona igual en Windows, Mac y Linux porque todo corre dentro de contenedores: no hay que instalar Python, Airflow ni PostgreSQL en tu computador.

### Consultar los resultados desde otros equipos o el celular
- **Panel web adaptable:** `http://<IP-del-equipo>:8000/` — pega la clave `PRODUCTS_API_TOKEN` del `.env` y verás ventas netas por mes, top de productos y países.
- **API REST en JSON** con documentación interactiva en `/docs` (cabecera `X-API-Key`): `GET /products`, `GET /products/{codigo}`, `GET /analytics/monthly-net-sales`, `/analytics/top-products`, `/analytics/countries`.
- **Base SQL** en el puerto `DW_HOST_PORT` para DBeaver, Power BI o Metabase.

`<IP-del-equipo>` es la IP local del computador donde corre Docker (en ese mismo equipo basta `localhost`). El otro dispositivo debe estar en la misma red y el firewall debe permitir los puertos 8000 y 8080.

---

## Qué hace el DAG `datamart_sales_etl` (diario)

```
validate_configuration
        ├─► extract_sales_daily ───┐
        └─► extract_sales_history ─┴─► extract_products_catalog ─► transform_and_unify ─► run_quality_checks ─► load_dimensions
                                                                                                                  │
                                                              ┌───────────────────────────────────────────────────┤
                                                 load_sales_and_returns                       load_rejected_records │
                                                              └───────────────────────┬───────────────────────────┘
                                                                      build_net_revenue_aggregate ─► verify_warehouse_load
```
Reintenta 3 veces ante fallos, es idempotente (correrlo dos veces deja el mismo resultado) y no carga nada si falla algún chequeo de calidad. Detalle en [`docs/DECISIONES_TECNICAS.md`](docs/DECISIONES_TECNICAS.md).

## Estructura del repositorio

```
├── iniciar.bat / iniciar.sh    Levantan todo con un clic (Windows / Mac-Linux)
├── detener.bat / detener.sh    Apagan los servicios
├── docker-compose.yml          Todos los servicios
├── .env.example                Variables de entorno (copiar a .env)
├── dags/
│   ├── datamart_etl_dag.py     Definición del DAG
│   └── datamart/               Código del pipeline: sources, transform, quality, loaders, tasks
├── shared/category_rules.py    Reglas de categoría compartidas por el pipeline y la API
├── services/products_api/      API de productos (FastAPI) + panel web
├── sql/ddl/                    Creación de tablas (se ejecuta al iniciar la base)
├── sql/validation/             Consultas por pregunta de negocio
├── scripts/                    bootstrap_airflow.sh (Connections y Variables), generador de seeds
├── seeds/                      Datos de prueba para correr desde cero
├── data/raw/                   Aquí van los CSV reales (no se versionan)
├── docs/                       Decisiones técnicas, diagrama del modelo
└── tests/                      Pruebas de las reglas de negocio
```

## Pruebas (opcional)
```bash
pip install pandas pyarrow pytest
python -m pytest tests -q
```

## Problemas frecuentes

| Síntoma | Solución |
|---|---|
| `variable is not set` al iniciar | Falta el archivo `.env`: usa `iniciar.bat` / `./iniciar.sh` o copia `.env.example` a `.env` |
| Puerto 8080, 8000 o 5433 ocupado | Cambia `AIRFLOW_WEB_PORT`, `PRODUCTS_API_PORT` o `DW_HOST_PORT` en `.env` |
| Quiero empezar desde cero | `docker compose down -v` (borra bases y vuelve a crear tablas al iniciar) |
| El DAG usa datos de prueba y yo quería los reales | Revisa que los CSV estén en `data/raw/` con el nombre exacto, y vuelve a ejecutar el DAG |
| En Linux: `Permission denied` en `data/` | Arranca con `./iniciar.sh` (ajusta el usuario); o `chmod -R a+rwX data` |
| La API tarda en responder la primera vez | Construye el catálogo al arrancar; con los CSV completos toma ~1 minuto. El pipeline reintenta y, si no responde, usa la categorización de respaldo |
