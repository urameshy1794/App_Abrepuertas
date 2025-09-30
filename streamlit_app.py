import streamlit as st
import sqlite3
import unicodedata

DB_PATH = "data.db"
TABLE = "direcciones"

st.set_page_config(page_title="Celso se la come", page_icon="🔎", layout="wide")
st.title("🔎 Buscador por Dirección")

st.caption("Busca por ejemplo: `av los sauces nro 123` (no importa mayúsculas, tildes o 'av.' vs 'avenida').")

def normalize_text(s: str) -> str:
    s = s.strip().lower()
    s = "".join(
        ch for ch in unicodedata.normalize("NFD", s)
        if unicodedata.category(ch) != "Mn"
    )
    s = s.replace("avenida", "av").replace("av.", "av").replace(" jr.", " jr").replace("jiron", "jr")
    s = " ".join(s.split())
    return s

@st.cache_resource
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def search(q: str, limit: int = 200):
    q_norm = normalize_text(q)
    conn = get_conn()
    cur = conn.cursor()

    # 1) Traemos coincidencias por LIKE sobre direccion_norm (rápido con índice)
    # 2) DEDUPE: elegimos 1 fila por direccion_norm, priorizando la fecha más reciente (ISO mayor)
    sql = f"""
    WITH matches AS (
        SELECT
            id,
            direccion_norm,
            fecha_final_abrepuertas,
            fecha_final_abrepuertas_iso,
            codigo_proyecto,
            item_plan_general_ip_general,
            status,
            contratista_1,
            direccion_full,
            source_file
        FROM {TABLE}
        WHERE direccion_norm LIKE '%' || ? || '%'
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER(
                PARTITION BY direccion_norm
                ORDER BY
                    CASE WHEN fecha_final_abrepuertas_iso IS NULL OR fecha_final_abrepuertas_iso = '' THEN 1 ELSE 0 END,
                    fecha_final_abrepuertas_iso DESC,
                    id ASC
            ) AS rn
        FROM matches
    )
    SELECT
        fecha_final_abrepuertas AS "Fecha Final del Abrepuertas",
        codigo_proyecto AS "Código del Proyecto",
        item_plan_general_ip_general AS "ITEM PLAN GENERAL - IP GENERAL",
        status AS "STATUS",
        contratista_1 AS "CONTRATISTA_1",
        direccion_full AS "Dirección",
        source_file AS "Fuente"
    FROM ranked
    WHERE rn = 1
    LIMIT ?
    """
    rows = cur.execute(sql, (q_norm, limit)).fetchall()
    return rows

with st.form(key="search_form"):
    q = st.text_input("Dirección (ej: av los sauces nro 123)", value="")
    limit = st.number_input("Límite de resultados (únicas direcciones)", min_value=1, max_value=5000, value=200, step=50)
    submit = st.form_submit_button("Buscar")

if submit and q.strip():
    results = search(q, int(limit))
    st.write(f"**{len(results)}** dirección(es) encontrada(s).")
    if results:
        st.dataframe(
            [{"Fecha Final del Abrepuertas": r["Fecha Final del Abrepuertas"],
              "Código del Proyecto": r["Código del Proyecto"],
              "ITEM PLAN GENERAL - IP GENERAL": r["ITEM PLAN GENERAL - IP GENERAL"],
              "STATUS": r["STATUS"],
              "CONTRATISTA_1": r["CONTRATISTA_1"],
              "Dirección": r["Dirección"],
              "Fuente": r["Fuente"]} for r in results],
            use_container_width=True
        )
    else:
        st.info("No se encontraron coincidencias.")
else:
    st.info("Ingresa una dirección y presiona **Buscar**.")

