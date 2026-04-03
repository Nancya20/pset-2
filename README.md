# PSet-2: NY Taxi ELT Pipeline (2015–2025)

Pipeline ELT end-to-end para datos de NY Yellow Taxi, orquestado con **Mage**, almacenado en **PostgreSQL** e inspeccionable con **pgAdmin**, todo desplegado con **Docker Compose**.

---

## Objetivo

Construir una arquitectura ELT reproducible que ingiera datos históricos de NY Yellow Taxi (2015–2025), los almacene en una capa `raw` inmutable, y los transforme en un modelo dimensional analítico en una capa `clean`.

---

## Arquitectura

```
TLC CDN (parquet) → Mage Pipeline raw → PostgreSQL schema raw
                  → Mage Pipeline clean → PostgreSQL schema clean (star schema)
```

Ver [docs/architecture.md](docs/architecture.md) para el diagrama completo.

---

## Stack

| Servicio   | Puerto | Rol                        |
|------------|--------|----------------------------|
| PostgreSQL | 5432   | Data Warehouse             |
| Mage       | 6789   | Orquestador de pipelines   |
| pgAdmin    | 9000   | UI de inspección y validación |

---

## Estructura del proyecto

```
pset-2/
├── docker-compose.yml           # Infraestructura completa
├── .env.example                 # Plantilla de variables de entorno
├── .gitignore
├── pgadmin-servers.json         # Preconfigura conexión en pgAdmin
├── init-scripts/
│   └── 01_create_schemas.sql    # Crea schemas raw y clean al inicio
├── notebooks/                   # Notas del curso — patrones aplicados
│   ├── notas-command.ipynb      → Command (retry, idempotencia)
│   ├── notas-pubsub.ipynb       → Observer (eventos de pipeline)
│   ├── notas-state.ipynb        → State (tracking de ejecución)
│   └── notas-template.ipynb     → Template Method (bloques Mage)
├── scripts/
│   └── create_triggers.py       # Configura triggers vía API de Mage
├── docs/
│   └── architecture.md          # Diagrama de arquitectura y modelo dimensional
├── screenshots/                 # Evidencia visual
└── mage-volume/
    └── ny_taxi_project/
        ├── io_config.yaml        # Conexión PG (referencia env vars, sin credenciales)
        ├── requirements.txt      # pyarrow, psycopg2, sqlalchemy, requests
        ├── pipelines/
        │   ├── raw_ingestion/    # Pipeline 1
        │   └── clean_transformation/  # Pipeline 2
        ├── data_loaders/
        │   ├── download_ny_taxi_data.py    # P1: descarga parquet TLC
        │   └── load_from_raw_schema.py     # P2: lee desde raw
        ├── transformers/
        │   ├── transform_raw_minimal.py    # P1: tipado técnico mínimo
        │   └── transform_to_clean.py       # P2: modelo dimensional
        ├── data_exporters/
        │   ├── export_to_raw_postgres.py   # P1: escribe a raw
        │   └── export_to_clean_postgres.py # P2: escribe dimensiones y hechos
        └── utils/
            ├── db_helpers.py
            └── patterns/
                ├── template.py  # Template Method → estructura de bloques
                ├── command.py   # Command       → descarga + retry + undo
                ├── observer.py  # Observer       → eventos del pipeline
                └── state.py     # State          → tracking de ejecución
```

---

## Levantar el entorno

### 1. Clonar y configurar

```bash
# Copiar plantilla de variables de entorno
cp .env.example .env

# Editar .env con tus valores (no commitear este archivo)
# Variables mínimas a cambiar:
#   PG_PASSWORD, PGADMIN_PASSWORD
```

### 2. Arrancar todos los servicios

```bash
docker compose up -d
```

Esto levanta:
- PostgreSQL con los schemas `raw` y `clean` ya creados
- Mage instalando las dependencias del `requirements.txt`
- pgAdmin preconfigurado para conectar al PostgreSQL

### 3. Verificar servicios

```bash
docker compose ps
# Todos deben estar en estado "Up" o "healthy"
```

---

## Ejecutar los pipelines

### Opción A — Desde la UI de Mage

1. Abrir http://localhost:6789
2. Ir a **Pipelines** en el menú lateral
3. Seleccionar **raw_ingestion** → clic en **Run pipeline now**
4. Esperar que complete (monitorear en la sección **Runs**)
5. Seleccionar **clean_transformation** → clic en **Run pipeline now**

### Opción B — Desde terminal

```bash
# Pipeline raw (dentro del contenedor)
docker exec ny_taxi_mage mage run ny_taxi_project raw_ingestion

# Pipeline clean (después de que raw complete)
docker exec ny_taxi_mage mage run ny_taxi_project clean_transformation
```

### Configurar rango de datos

Edita `.env` antes de ejecutar:

```bash
# Demo (rápido, ~3 meses, ~5M registros):
START_YEAR=2024  START_MONTH=1
END_YEAR=2024    END_MONTH=3

# Producción completa (2015-2025, ~120 archivos, requiere >50GB disco):
START_YEAR=2015  START_MONTH=1
END_YEAR=2025    END_MONTH=12
```

Luego reiniciar Mage para aplicar:
```bash
docker compose restart mage
```

---

## Configurar triggers automáticos

```bash
# Instalar requests si no está disponible
pip install requests

# Crear triggers (con Mage corriendo)
python scripts/create_triggers.py
```

Triggers que se crean:
| Trigger | Pipeline | Horario | Descripción |
|---|---|---|---|
| `raw_ingestion_daily_2am` | raw_ingestion | @daily 02:00 UTC | Ingesta diaria de datos raw |
| `clean_after_raw_4am` | clean_transformation | @daily 04:00 UTC | Transformación clean (2h después de raw) |

**Dependencia lógica:** `clean_transformation` se ejecuta 2 horas después de `raw_ingestion`, garantizando que los datos raw estén disponibles.

También puedes configurarlos manualmente:
1. Mage UI → **Pipelines** → **raw_ingestion** → **Triggers** → **New trigger**
2. Tipo: **Schedule**, Interval: **@daily**, Start time: `2025-01-01 02:00:00`

---

## Acceder a pgAdmin

1. Abrir http://localhost:9000
2. Login: `admin@example.com` / (el valor de `PGADMIN_PASSWORD` en tu `.env`)
3. La conexión **"NY Taxi DW"** ya está preconfigurada
4. Navegar a: **Servers → NY Taxi DW → Databases → nyc_taxi_db**

---

## Validar resultados en PostgreSQL

### Schema raw

```sql
-- Verificar que se cargaron datos
SELECT source_year, source_month, COUNT(*) as registros
FROM raw.yellow_taxi_trips
GROUP BY source_year, source_month
ORDER BY source_year, source_month;

-- Estructura de la tabla
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'raw' AND table_name = 'yellow_taxi_trips';
```

### Schema clean — Dimensiones

```sql
SELECT * FROM clean.dim_vendor;
SELECT * FROM clean.dim_payment_type;
SELECT * FROM clean.dim_rate_code;
SELECT COUNT(*) FROM clean.dim_location;     -- 265 zonas de NYC
SELECT COUNT(*) FROM clean.dim_date;         -- 4018 fechas (2015-2026)
```

### Schema clean — Fact table

```sql
-- Total de viajes por año/mes
SELECT
    d.year, d.month,
    COUNT(*)             AS total_viajes,
    ROUND(AVG(f.trip_distance)::NUMERIC, 2) AS dist_promedio_millas,
    ROUND(AVG(f.total_amount)::NUMERIC, 2)  AS monto_promedio,
    ROUND(AVG(f.trip_duration_minutes)::NUMERIC, 1) AS duracion_promedio_min
FROM clean.fact_trips f
JOIN clean.dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Viajes por vendor
SELECT v.vendor_name, COUNT(*) AS total
FROM clean.fact_trips f
JOIN clean.dim_vendor v ON f.vendor_key = v.vendor_key
GROUP BY v.vendor_name;

-- Top 10 zonas de pickup
SELECT l.zone, l.borough, COUNT(*) AS pickups
FROM clean.fact_trips f
JOIN clean.dim_location l ON f.pickup_location_key = l.location_key
GROUP BY l.zone, l.borough
ORDER BY pickups DESC
LIMIT 10;
```

---

## Modelo Dimensional

### Granularidad
**1 fila = 1 viaje de taxi amarillo en NYC**

### Tablas

| Tabla | Tipo | Descripción |
|---|---|---|
| `clean.fact_trips` | Fact | Métricas de cada viaje (distancia, montos, duración) |
| `clean.dim_vendor` | Dim | Proveedor del servicio (CMT, VeriFone) |
| `clean.dim_payment_type` | Dim | Tipo de pago (tarjeta, cash, etc.) |
| `clean.dim_rate_code` | Dim | Tarifa aplicada (estándar, JFK, Newark, etc.) |
| `clean.dim_location` | Dim | Zona geográfica de NYC (265 zonas TLC) |
| `clean.dim_date` | Dim | Fechas 2015–2026 con atributos temporales |

### Relaciones

```
fact_trips.vendor_key          → dim_vendor.vendor_key
fact_trips.pickup_date_key     → dim_date.date_key
fact_trips.dropoff_date_key    → dim_date.date_key
fact_trips.pickup_location_key → dim_location.location_key
fact_trips.dropoff_location_key→ dim_location.location_key
fact_trips.payment_type_key    → dim_payment_type.payment_type_key
fact_trips.rate_code_key       → dim_rate_code.rate_code_key
```

### Decisiones de modelado

- **Datos pre-2017** se cargan a `raw` pero no a `clean.fact_trips`: los datos anteriores a 2017 usan lat/long en vez de `LocationID`, incompatibles con `dim_location`.
- **Claves sustitutas (surrogate keys)** en todas las dimensiones: desacopla los IDs del dominio del modelo físico.
- **dim_date precalculada** (2015–2026): evita JOINs a funciones de fecha en tiempo de consulta.
- **location_key = 0 / vendor_key = 0** sirven como "desconocido" para FK safety.

---

## Manejo de secrets y configuración sensible

Todas las credenciales se gestionan mediante variables de entorno:

1. **`.env`** — archivo local (nunca al repositorio, incluido en `.gitignore`)
2. **`docker-compose.yml`** — pasa las variables al contenedor Mage como `environment:`
3. **`io_config.yaml`** — referencia las variables con `{{ env_var('VAR') }}` (sin hardcoding)
4. **Código Python** — lee via `os.environ.get('VAR')` o lanza error si no existe

**Nunca permitido:**
- Credenciales en notebooks, scripts, o en el repositorio
- Contraseñas en texto plano en docker-compose.yml

---

## Patrones de diseño aplicados (del curso)

Los notebooks de la carpeta `notebooks/` documentan los patrones. Aquí su aplicación concreta:

### Template Method (`notas-template.ipynb`)
`utils/patterns/template.py` — Clase `BasePipelineBlock` define el esqueleto:
```
execute() = setup() → validate_config() → run() → teardown()
```
Todos los bloques Mage heredan de `BaseDataLoaderBlock`, `BaseTransformerBlock` o `BaseExporterBlock`.

### Command (`notas-command.ipynb`)
`utils/patterns/command.py` — Encapsula operaciones de descarga y borrado:
- `DownloadParquetCommand` → descarga un archivo TLC + retry/backoff
- `DeleteRawFileCommand` → compensating action para idempotencia
- `CommandRunner` → Invoker con política de reintentos exponenciales

### Observer (`notas-pubsub.ipynb`)
`utils/patterns/observer.py` — Bus de eventos desacoplado:
- `AuditLogObserver` → logging estructurado de cada evento
- `MetricsObserver` → acumula contadores de registros
- `PipelineTriggerObserver` → registra completions en BD para encadenamiento

### State (`notas-state.ipynb`)
`utils/patterns/state.py` — Máquina de estados para pipeline runs:
```
PENDIENTE → EN_EJECUCION → COMPLETADO
                        ↘ FALLIDO → (reintento) → EN_EJECUCION
```

---

## Detener el entorno

```bash
# Detener contenedores (preserva datos en volúmenes)
docker compose down

# Detener y eliminar todos los datos (reset completo)
docker compose down -v
```

---

## Solución de problemas

| Problema | Solución |
|---|---|
| Mage no levanta | Verificar que `.env` exista y tenga PG_PASSWORD |
| Error de conexión PG en Mage | Esperar ~30s para que PostgreSQL esté healthy |
| `EnvironmentError: Variables faltantes` | Asegurarse que `.env` tiene todas las variables de `.env.example` |
| Descarga falla para ciertos meses | Algunos meses de 2025 pueden no estar disponibles aún en TLC |
| fact_trips vacía después de clean | Verificar que raw tiene datos con `source_year >= 2017` |
