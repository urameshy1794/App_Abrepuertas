import streamlit as st
import sqlite3
import unicodedata

DB_PATH = "data.db"
TABLE = "direcciones"

st.set_page_config(page_title="Buscador por Dirección", page_icon="🔎", layout="wide")
st.title("🔎 Buscador por Dirección")

st.caption("Ej: `av los sauces nro 123`. No importa mayúsculas, tildes, 'av.' vs 'avenida'.")

def normalize_text(s: str) -> str:
    s = s.strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    s = (s.replace("avenida", "av").replace("av.", "av")
           .replace(" jiron", " jr").replace("jiron", "jr"))
    s = " ".join(s.split())
    return s

@st.cache_resource
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def search_all(q: str, limit: int):
    qn = normalize_text(q)
    sql = f"""
    SELECT
        fecha_final_abrepuertas AS "Fecha",
        codigo_proyecto AS "Código",
        direccion_full AS "Dirección",
        distrito AS "Distrito",
        nombre_proyecto AS "Nombre del Proyecto",
        item_plan_general_ip_general AS "ITEM PLAN GENERAL - IP GENERAL",
        status AS "STATUS",
        contratista_1 AS "CONTRATISTA_1",
        source_file AS "Fuente"
    FROM {TABLE}
    WHERE direccion_norm LIKE '%' || ? || '%'
    ORDER BY
        CASE WHEN fecha_final_abrepuertas_iso IS NULL OR fecha_final_abrepuertas_iso = '' THEN 1 ELSE 0 END,
        fecha_final_abrepuertas_iso DESC,
        id ASC
    LIMIT ?
    """
    cur = get_conn().cursor()
    return cur.execute(sql, (qn, limit)).fetchall()

def search_dedup(q: str, limit: int):
    qn = normalize_text(q)
    sql = f"""
    WITH matches AS (
      SELECT
        id, direccion_norm,
        fecha_final_abrepuertas, fecha_final_abrepuertas_iso,
        codigo_proyecto, item_plan_general_ip_general,
        status, contratista_1, nombre_proyecto, distrito,
        direccion_full, source_file
      FROM {TABLE}
      WHERE direccion_norm LIKE '%' || ? || '%'
    ),
    ranked AS (
      SELECT *,
        ROW_NUMBER() OVER(
          PARTITION BY direccion_norm
          ORDER BY
            CASE WHEN fecha_final_abrepuertas_iso IS NULL OR fecha_final_abrepuertas_iso = '' THEN 1 ELSE 0 END,
            fecha_final_abrepuertas_iso DESC,
            id ASC
        ) rn
      FROM matches
    )
    SELECT
        fecha_final_abrepuertas AS "Fecha",
        codigo_proyecto AS "Código",
        direccion_full AS "Dirección",
        distrito AS "Distrito",
        nombre_proyecto AS "Nombre del Proyecto",
        item_plan_general_ip_general AS "ITEM PLAN GENERAL - IP GENERAL",
        status AS "STATUS",
        contratista_1 AS "CONTRATISTA_1",
        source_file AS "Fuente"
    FROM ranked
    WHERE rn = 1
    LIMIT ?
    """
    cur = get_conn().cursor()
    return cur.execute(sql, (qn, limit)).fetchall()

with st.form(key="search"):
    q = st.text_input("Dirección", value="")
    limit = st.number_input("Límite de filas", min_value=1, max_value=20000, value=200, step=100)
    show_all = st.toggle("Mostrar todas las coincidencias (sin deduplicar)", value=True)
    submitted = st.form_submit_button("Buscar")

if submitted and q.strip():
    rows = search_all(q, int(limit)) if show_all else search_dedup(q, int(limit))
    st.write(f"**{len(rows)}** resultado(s).")
    if rows:
        data = [{
            "Fecha": r["Fecha"],
            "Código": r["Código"],
            "Dirección": r["Dirección"],
            "Distrito": r["Distrito"],
            "Nombre del Proyecto": r["Nombre del Proyecto"],
            "ITEM PLAN GENERAL - IP GENERAL": r["ITEM PLAN GENERAL - IP GENERAL"],
            "STATUS": r["STATUS"],
            "CONTRATISTA_1": r["CONTRATISTA_1"],
            "Fuente": r["Fuente"],
        } for r in rows]
        st.dataframe(data, use_container_width=True)
    else:
        st.info("Sin coincidencias. Prueba con menos términos o valida 'nro/numero'.")
else:
    st.info("Ingresa una dirección y presiona **Buscar**.")





