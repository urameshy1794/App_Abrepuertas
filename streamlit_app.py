# streamlit_app.py
import streamlit as st
import sqlite3
import unicodedata

DB_PATH = "data.db"
TABLE = "direcciones"

st.set_page_config(page_title="Buscador por Dirección", page_icon="🔎", layout="wide")
st.title("🔎 Buscador por Dirección")
st.caption("Ej: `av los sauces nro 123`. No importa mayúsculas, tildes, 'av.' vs 'avenida'.")

def normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
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

def existing_cols(conn) -> set:
    rows = conn.execute(f"PRAGMA table_info({TABLE})").fetchall()
    return {r[1] for r in rows}  # 2da columna = nombre

def col_or_blank(colname: str, alias: str, cols: set) -> str:
    """Devuelve 'colname AS "Alias"' si existe; si no, '' AS "Alias"'."""
    if colname in cols:
        return f'{colname} AS "{alias}"'
    else:
        return f"'' AS \"{alias}\""

def order_expr(cols: set) -> str:
    # Preferimos ordenar por fecha ISO si existe; si no, por id
    if "fecha_final_abrepuertas_iso" in cols:
        return ("CASE WHEN (fecha_final_abrepuertas_iso IS NULL "
                "OR fecha_final_abrepuertas_iso='') THEN 1 ELSE 0 END, "
                "fecha_final_abrepuertas_iso DESC, id ASC")
    else:
        # Fallback estable
        return "id ASC"

def select_clause(cols: set) -> str:
    # Orden exacto solicitado
    parts = [
        col_or_blank("fecha_final_abrepuertas", "Fecha", cols),
        col_or_blank("codigo_proyecto", "Código", cols),
        col_or_blank("direccion_full", "Dirección", cols),
        col_or_blank("distrito", "Distrito", cols),
        col_or_blank("nombre_proyecto", "Nombre del Proyecto", cols),
        col_or_blank("item_plan_general_ip_general", "ITEM PLAN GENERAL - IP GENERAL", cols),
        col_or_blank("status", "STATUS", cols),
        col_or_blank("contratista_1", "CONTRATISTA_1", cols),
        col_or_blank("source_file", "Fuente", cols),
    ]
    return ",\n        ".join(parts)

def search_all(q: str, limit: int):
    qn = normalize_text(q)
    conn = get_conn()
    cols = existing_cols(conn)
    sel = select_clause(cols)
    ord_by = order_expr(cols)
    # NOTA: inyectamos el LIMIT como entero validado para evitar problemas con "LIMIT ?"
    sql = f"""
    SELECT
        {sel}
    FROM {TABLE}
    WHERE direccion_norm LIKE '%' || ? || '%'
    ORDER BY {ord_by}
    LIMIT {int(limit)}
    """
    return conn.execute(sql, (qn,)).fetchall()

def search_dedup(q: str, limit: int):
    qn = normalize_text(q)
    conn = get_conn()
    cols = existing_cols(conn)
    sel = select_clause(cols)
    ord_by = order_expr(cols)

    if "fecha_final_abrepuertas_iso" in cols:
        # Usamos ventana si está disponible la columna de fecha ISO (SQLite moderno lo soporta)
        sql = f"""
        WITH matches AS (
          SELECT *
          FROM {TABLE}
          WHERE direccion_norm LIKE '%' || ? || '%'
        ),
        ranked AS (
          SELECT *,
            ROW_NUMBER() OVER(
              PARTITION BY direccion_norm
              ORDER BY {ord_by}
            ) AS rn
          FROM matches
        )
        SELECT
            {sel}
        FROM ranked
        WHERE rn = 1
        LIMIT {int(limit)}
        """
        return conn.execute(sql, (qn,)).fetchall()
    else:
        # Fallback sin funciones ventana: nos quedamos con el menor id por dirección
        sql = f"""
        SELECT
            {sel}
        FROM {TABLE} d
        WHERE direccion_norm LIKE '%' || ? || '%'
          AND NOT EXISTS (
            SELECT 1 FROM {TABLE} d2
            WHERE d2.direccion_norm = d.direccion_norm
              AND d2.id < d.id
          )
        LIMIT {int(limit)}
        """
        return conn.execute(sql, (qn,)).fetchall()

with st.form(key="search"):
    q = st.text_input("Dirección", value="")
    limit = st.number_input("Límite de filas", min_value=1, max_value=20000, value=200, step=100)
    show_all = st.toggle("Mostrar todas las coincidencias (sin deduplicar)", value=True)
    submitted = st.form_submit_button("Buscar")

if submitted and q.strip():
    try:
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
    except Exception as e:
        st.error("Ocurrió un error ejecutando la consulta. Verifica que `data.db` esté actualizado con el ETL.")
        st.exception(e)
else:
    st.info("Ingresa una dirección y presiona **Buscar**.")







