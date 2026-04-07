import duckdb
import pandas as pd

MOTHERDUCK_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6Im1hdGhpYXMuY2hkQGdtYWlsLmNvbSIsIm1kUmVnaW9uIjoiYXdzLXVzLWVhc3QtMSIsInNlc3Npb24iOiJtYXRoaWFzLmNoZC5nbWFpbC5jb20iLCJwYXQiOiI3ZVZGNTk5TzJGRkdQLTV6aFhaaW9LYllGZXF6SWhYaTVZNXBERTdNOGZRIiwidXNlcklkIjoiNDVhMWRlZjgtMDcwNS00MDRiLWJlNTUtNzIzMGIxZGUxMDU1IiwiaXNzIjoibWRfcGF0IiwicmVhZE9ubHkiOmZhbHNlLCJ0b2tlblR5cGUiOiJyZWFkX3dyaXRlIiwiaWF0IjoxNzczNDQzMTc4fQ.vu2dX8J7vQPRcOqS6hlCfEIgqr45K0RyzafOHJCor5s"

# ── Leer CSVs / Excel procesado ──────────────────────────────────────────────
df_eeff      = pd.read_excel("Boletin_Procesado.xlsx", sheet_name="EEFF")
df_cartera   = pd.read_excel("Boletin_Procesado.xlsx", sheet_name="Cartera")
df_sector    = pd.read_excel("Boletin_Procesado.xlsx", sheet_name="Cred_por_sector")
df_actividad = pd.read_excel("Boletin_Procesado.xlsx", sheet_name="Cred_por_actividad")

# ── Conectar a MotherDuck ─────────────────────────────────────────────────────
con = duckdb.connect(f"md:airbyte_proyecto?motherduck_token={MOTHERDUCK_TOKEN}")

# ── Crear schema raw_bcp ──────────────────────────────────────────────────────
con.execute("CREATE SCHEMA IF NOT EXISTS raw_bcp")

# ── Cargar cada DataFrame como tabla ─────────────────────────────────────────
tablas = {
    "raw_bcp.raw_eeff":      df_eeff,
    "raw_bcp.raw_cartera":   df_cartera,
    "raw_bcp.raw_sector":    df_sector,
    "raw_bcp.raw_actividad": df_actividad,
}

for nombre, df in tablas.items():
    con.execute(f"DROP TABLE IF EXISTS {nombre}")
    con.execute(f"CREATE TABLE {nombre} AS SELECT * FROM df")
    count = con.execute(f"SELECT COUNT(*) FROM {nombre}").fetchone()[0]
    print(f"✓ {nombre}: {count} filas cargadas")

con.close()
print("\nCarga completa en MotherDuck.")