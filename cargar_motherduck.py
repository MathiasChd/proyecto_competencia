import duckdb
import pandas as pd

MOTHERDUCK_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InBpbGFyLnJ1aXpkaWF6LnJpdmVyb3NAZ21haWwuY29tIiwibWRSZWdpb24iOiJhd3MtdXMtZWFzdC0xIiwic2Vzc2lvbiI6InBpbGFyLnJ1aXpkaWF6LnJpdmVyb3MuZ21haWwuY29tIiwicGF0IjoidlQ3S2Q5dUVTZkVkelhXSzNtTVk3TFVkd3p3T1c5elQ1WF9zc0RXaUptNCIsInVzZXJJZCI6ImI1OTMwZjFmLTBhMjgtNDJjNS1hYTZhLWYxMzRmODY5NjdlMiIsImlzcyI6Im1kX3BhdCIsInJlYWRPbmx5IjpmYWxzZSwidG9rZW5UeXBlIjoicmVhZF93cml0ZSIsImlhdCI6MTc3NDYzNzI1M30.ee_vDkvdyii-pEBE2h3BD40wHxNWpXByJIpYySqx8bo"

# ── Leer CSVs / Excel procesado ──────────────────────────────────────────────
df_eeff      = pd.read_excel("Boletin_Procesado.xlsx", sheet_name="EEFF")
df_cartera   = pd.read_excel("Boletin_Procesado.xlsx", sheet_name="Cartera")
df_sector    = pd.read_excel("Boletin_Procesado.xlsx", sheet_name="Cred_por_sector")
df_actividad = pd.read_excel("Boletin_Procesado.xlsx", sheet_name="Cred_por_actividad")

# ── Leer archivo de Tipo de Cambio ──
df_tc        = pd.read_excel("tc_bcp.xlsx", sheet_name="cotizacion")

# ── Conectar a MotherDuck ─────────────────────────────────────────────────────
con = duckdb.connect(f"md:airbyte_curso?motherduck_token={MOTHERDUCK_TOKEN}")

# ── Crear schema raw_bcp ──────────────────────────────────────────────────────
con.execute("CREATE SCHEMA IF NOT EXISTS raw_bcp")

# ── Cargar cada DataFrame como tabla ─────────────────────────────────────────
tablas = {
    "raw_bcp.raw_eeff":      df_eeff,
    "raw_bcp.raw_cartera":   df_cartera,
    "raw_bcp.raw_sector":    df_sector,
    "raw_bcp.raw_actividad": df_actividad,
    "raw_bcp.raw_tc":        df_tc
}

for nombre, df in tablas.items():
    con.execute(f"DROP TABLE IF EXISTS {nombre}")
    con.execute(f"CREATE TABLE {nombre} AS SELECT * FROM df")
    count = con.execute(f"SELECT COUNT(*) FROM {nombre}").fetchone()[0]
    print(f"✓ {nombre}: {count} filas cargadas")

con.close()
print("\nCarga completa en MotherDuck.")