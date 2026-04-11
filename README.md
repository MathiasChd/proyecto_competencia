# Pipeline BCP — Proyecto Final Integrador
## Maestría en Inteligencia Artificial y Análisis de datos — Ingeniería de Datos
### Facultad Politécnica, Universidad Nacional de Asunción

---

## Descripción

Pipeline de datos end-to-end que procesa los **Boletines Estadísticos de Bancos**
publicados por el Banco Central del Paraguay (BCP), integrando datos en
Moneda Nacional (MN) y Moneda Extranjera (ME) para análisis del sistema
bancario paraguayo.

---

## Stack tecnológico

| Capa | Herramienta | Descripción |
|---|---|---|
| Extracción y Carga | Python + MotherDuck | Script custom que procesa boletines BCP y carga a DuckDB cloud |
| Transformación | dbt-duckdb 1.10.1 | Modelos staging, intermedio y OBT |
| Base de datos | DuckDB / MotherDuck | Schema `raw_bcp` (raw) → `main_bcp` (transformado) |
| Calidad de datos | dbt-expectations | 25 tests — PASS=25 WARN=0 ERROR=0 |
Orquestación | Prefect 3.0 | Pipeline automatizado con lógica de reintentos y observabilidad (`flow_bcp.py`) |
| Dashboard | Metabase | (en desarrollo) |

---

## Fuentes de datos

| Fuente | Descripción | Formato |
|---|---|---|
| Boletín Bancos MN | Créditos, cartera y EEFF en Guaraníes | .xlsm |
| Boletín Bancos ME | Créditos, cartera y EEFF en Dólares | .xlsm |

Ambas fuentes son publicadas mensualmente por el BCP en:
https://www.bcp.gov.py/boletines-estadisticos-i398

---

## Estructura del proyecto
```
proyecto_bcp/
├── flow_bcp.py                 # Orquestador Prefect
├── cargar_motherduck.py        # Script EL: carga raw data a MotherDuck
├── packages.yml                # Dependencias dbt
├── dbt_project.yml             # Configuración del proyecto dbt
├── profiles.yml                # Configuración de conexión a la base de datos (DuckDB/MotherDuck)
├── README.md
└── models/
    ├── sources.yml             # Definición de fuentes raw
    ├── staging/
    │   ├── schema.yml          # Tests de calidad (25 tests)
    │   ├── stg_bcp__sector.sql
    │   ├── stg_bcp__actividad.sql
    │   ├── stg_bcp__cartera.sql
    │   └── stg_bcp__eeff.sql
    └── marts/
        └── obt_creditos_banco.sql
```

---

## Modelo de datos

### Decisión: One Big Table (OBT)

Se optó por OBT en lugar de modelo dimensional Kimball por las siguientes razones:

- El análisis es exploratorio y multidimensional sin jerarquías complejas
- Las dimensiones (banco, sector, moneda, fecha) tienen cardinalidad baja
- DuckDB está optimizado para consultas analíticas sobre tablas anchas
- Facilita el consumo directo desde Metabase sin JOINs adicionales

### Lineage
```
BCP Boletín MN (.xlsm)  ─┐
                          ├─► cargar_motherduck.py ─► raw_bcp.raw_* ─► stg_bcp__* ─► obt_creditos_banco
BCP Boletín ME (.xlsm)  ─┘
```

### Tablas en MotherDuck

| Schema | Tabla | Descripción |
|---|---|---|
| raw_bcp | raw_eeff | Estados financieros raw |
| raw_bcp | raw_cartera | Cartera de créditos raw |
| raw_bcp | raw_sector | Créditos por sector raw |
| raw_bcp | raw_actividad | Créditos por actividad raw |
| main_bcp | stg_bcp__eeff | EEFF limpio y unpivoteado |
| main_bcp | stg_bcp__cartera | Cartera limpia y unpivoteada |
| main_bcp | stg_bcp__sector | Sector limpio y unpivoteado |
| main_bcp | stg_bcp__actividad | Actividad limpia y unpivoteada |
| main_bcp | obt_creditos_banco | One Big Table para análisis |

---

## Instalación y uso

### Prerrequisitos

- Python 3.12+
- WSL2 (Ubuntu) o Linux
- Cuenta en MotherDuck (https://motherduck.com)
- Token de MotherDuck propio

### Setup
```bash
# Clonar el repo
git clone https://github.com/MathiasChd/proyecto_bcp.git
cd proyecto_bcp

# Crear entorno virtual
python3 -m venv venv
source venv/bin/activate

# Instalar dependencias
pip install dbt-duckdb duckdb pandas openpyxl

#Instalar librerias 
pip install pandas openpyxl duckdb dbt-duckdb
pip install prefect
pip install "griffe<1.0.0"  # Fix requerido para evitar conflictos de importación en el server de Prefect

# Configurar token de MotherDuck en profiles.yml
# Crear ~/.dbt/profiles.yml con el siguiente contenido:
```
```yaml
proyecto_bcp:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "md:airbyte_curso?motherduck_token=TU_TOKEN_AQUI"
      schema: main_bcp
```

### Ejecutar el pipeline
```bash
# Paso 1 — Cargar datos a MotherDuck
# Colocar los archivos .xlsm del BCP en la carpeta del proyecto
python cargar_motherduck.py

# Paso 2 — Instalar dependencias dbt
dbt deps

# Paso 3 — Transformar
dbt run

# Paso 4 — Validar calidad
dbt test
```

Resultado esperado:
```
Done. PASS=25 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=25
```

---

### Ejecutar el pipeline (Orquestado)

El proyecto utiliza **Prefect** para garantizar que las tareas se ejecuten en el orden correcto y manejar fallos automáticamente.

#### 1. Iniciar el servidor (en una terminal):
```bash
prefect server start
```
#### 2. Ejecutar el codigo (en otra terminal):
```bash
python3 flow_bcp.py
```
#### 3.Monitorear la ejecución en: http://localhost:4200

```markdown
Resultado esperado:
Done. PASS=25 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=25
```
--- 

## Tests de calidad

| Test | Modelos | Tipo |
|---|---|---|
| not_null | sector, actividad, cartera, eeff | Genérico dbt |
| accepted_values (MN/ME) | todos | Genérico dbt |
| expect_column_values_to_be_between | sector, actividad, cartera, eeff | dbt-expectations |
| expect_column_mean_to_be_between | sector | dbt-expectations |
| expect_table_row_count_to_be_between | obt_creditos_banco | dbt-expectations |

---
## Visualizaciones en Metabase

- Descargar el repositorio.
- Ejecutar java -jar metabase.jar.
- Entrar a localhost:3000.
- Ir a Configuración de base de datos y poner tu propio MotherDuck Token en el campo Database Path.

## Análisis posibles (cruce MN vs ME)

- Dolarización del crédito por banco
- Dolarización por sector económico
- Evolución de la composición de moneda en el tiempo
- Concentración bancaria por moneda
- Comparación de morosidad MN vs ME

---

## Integrantes
- Maria del Pilar Ruiz Diaz
- Tanya Godoy
- Mathias Chaparro

---

## Pendiente para entrega final

- [ ] Dashboard Metabase (5+ visualizaciones)
- [ ] Análisis cruzado MN vs ME
- [ ] Informe técnico en formato reporte
