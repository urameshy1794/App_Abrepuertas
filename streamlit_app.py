# streamlit_app.py  (con Diagnóstico y cache-busting por mtime/size)
import streamlit as st
import sqlite3
import unicodedata
import os
from datetime import datetime

DB_PATH = "data.db"
TABLE = "direcciones"

st.set_page_config(page_title="Buscador de Proyectos", page_icon="🔎", layout="wide")
st.title("🔎 Buscador de Proyectos")
st.caption("Busca por dirección (Ej: `av los sauces nro 123`) o por código de proyecto.")

def normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    s = (s.replace("avenida", "av").replace("av.", "av")
          .replace(" jiron", " jr").replace("jiron", "jr"))
    s = " ".join(s.split())
    return s

def db_signature(path: str) -> str:
    """Firma simple para invalidar cache cuando cambia el archivo."""
    try:
        size = os.path.getsize(path)
        mtime = os.path.getmtime(path)
        return f"{size}-{mtime}"
    except FileNotFoundError:
        return "missing"

@st.cache_resource
def get_conn(path: str, signature: str):
    # signature fuerza a streamlit a recrear la conexión si cambia el archivo
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def existing_cols(conn) -> set:
    rows = conn.execute(f"PRAGMA table_info({TABLE})").fetchall()
    return {r[1] for r in rows}

def col_or_blank(colname: str, alias: str, cols: set) -> str:
    if colname in cols:
        return f'{colname} AS "{alias}"'
    else:
        return f"'' AS \"{alias}\""

def order_expr(cols: set) -> str:
    if "fecha_final_abrepuertas_iso" in cols:
        return ("CASE WHEN (fecha_final_abrepuertas_iso IS NULL "
                "OR fecha_final_abrepuertas_iso='') THEN 1 ELSE 0 END, "
                "fecha_final_abrepuertas_iso DESC, id ASC")
    else:
        return "id ASC"

def select_clause(cols: set) -> str:
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

# --- Funciones de búsqueda por DIRECCIÓN ---
def search_all_by_address(conn, q: str, limit: int):
    qn = normalize_text(q)
    cols = existing_cols(conn)
    sel = select_clause(cols)
    ord_by = order_expr(cols)
    sql = f"""
    SELECT
        {sel}
    FROM {TABLE}
    WHERE direccion_norm LIKE '%' || ? || '%'
    ORDER BY {ord_by}
    LIMIT {int(limit)}
    """
    return conn.execute(sql, (qn,)).fetchall()

def search_dedup_by_address(conn, q: str, limit: int):
    qn = normalize_text(q)
    cols = existing_cols(conn)
    sel = select_clause(cols)
    ord_by = order_expr(cols)

    if "fecha_final_abrepuertas_iso" in cols:
        sql = f"""
        WITH matches AS (
          SELECT * FROM {TABLE}
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
        SELECT {sel}
        FROM ranked
        WHERE rn = 1
        LIMIT {int(limit)}
        """
        return conn.execute(sql, (qn,)).fetchall()
    else:
        sql = f"""
        SELECT {sel}
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

# --- (NUEVO) Funciones de búsqueda por CÓDIGO DE PROYECTO ---
def search_all_by_code(conn, q: str, limit: int):
    # No se normaliza el texto para códigos de proyecto
    qn = (q or "").strip()
    cols = existing_cols(conn)
    sel = select_clause(cols)
    ord_by = order_expr(cols)
    sql = f"""
    SELECT
        {sel}
    FROM {TABLE}
    WHERE codigo_proyecto LIKE '%' || ? || '%'
    ORDER BY {ord_by}
    LIMIT {int(limit)}
    """
    return conn.execute(sql, (qn,)).fetchall()

def search_dedup_by_code(conn, q: str, limit: int):
    # No se normaliza el texto para códigos de proyecto
    qn = (q or "").strip()
    cols = existing_cols(conn)
    sel = select_clause(cols)
    ord_by = order_expr(cols)

    if "fecha_final_abrepuertas_iso" in cols:
        sql = f"""
        WITH matches AS (
          SELECT * FROM {TABLE}
          WHERE codigo_proyecto LIKE '%' || ? || '%'
        ),
        ranked AS (
          SELECT *,
            ROW_NUMBER() OVER(
              PARTITION BY direccion_norm  -- Mantenemos deduplicación por dirección
              ORDER BY {ord_by}
            ) AS rn
          FROM matches
        )
        SELECT {sel}
        FROM ranked
        WHERE rn = 1
        LIMIT {int(limit)}
        """
        return conn.execute(sql, (qn,)).fetchall()
    else:
        # Simplificamos si no hay fecha, ya que el caso es menos común
        # y la deduplicación por código es menos necesaria
        return search_all_by_code(conn, q, limit)

# -------- (MODIFICADO) Barra de búsqueda ----------
with st.form(key="search"):
    # (NUEVO) Selector para el tipo de búsqueda
    search_by = st.radio(
        "Buscar por:",
        ("Dirección", "Código del Proyecto"),
        horizontal=True,
    )

    # (NUEVO) Etiqueta dinámica para el campo de texto
    label = "Dirección" if search_by == "Dirección" else "Código del Proyecto"
    q = st.text_input(label, value="")

    limit = st.number_input("Límite de filas", min_value=1, max_value=20000, value=200, step=100)
    show_all = st.toggle("Mostrar todas las coincidencias (sin deduplicar)", value=True)
    submitted = st.form_submit_button("Buscar")

# -------- Conexión con cache-busting ----------
sig = db_signature(DB_PATH)
conn = get_conn(DB_PATH, sig)

# -------- Diagnóstico (sin cambios) ----------
with st.expander("🛠️ Modo diagnóstico (verifica que la app está leyendo el data.db correcto)"):
    try:
        size = os.path.getsize(DB_PATH)
        mtime = datetime.fromtimestamp(os.path.getmtime(DB_PATH))
        st.write(f"**Ruta:** `{os.path.abspath(DB_PATH)}`")
        st.write(f"**Tamaño:** {size:,} bytes")
        st.write(f"**Última modif:** {mtime}")
    except FileNotFoundError:
        st.error("`data.db` no se encuentra en el directorio de la app.")
    cols = existing_cols(conn)
    st.write("**Columnas en tabla `direcciones`:**", sorted(cols))
    # Conteos en vivo desde la BD usada por la app
    try:
        row = conn.execute(f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN TRIM(COALESCE(nombre_proyecto,''))<>'' THEN 1 ELSE 0 END) AS con_nombre_proy,
                SUM(CASE WHEN TRIM(COALESCE(distrito,''))<>'' THEN 1 ELSE 0 END) AS con_distrito
            FROM {TABLE}
        """).fetchone()
        st.write(f"**Total:** {row['total']}, **con Nombre del Proyecto:** {row['con_nombre_proy']}, **con Distrito:** {row['con_distrito']}")
        # muestra una muestra de valores (para confirmar que no están vacíos)
        sample = conn.execute(f"""
            SELECT nombre_proyecto, distrito
            FROM {TABLE}
            WHERE TRIM(COALESCE(nombre_proyecto,''))<>'' OR TRIM(COALESCE(distrito,''))<>''
            LIMIT 5
        """).fetchall()
        st.write("**Muestra (nombre_proyecto, distrito)**:", [tuple(r) for r in sample])
    except Exception as e:
        st.warning("No se pudo consultar la tabla. ¿Existe `direcciones`?")
        st.exception(e)

# -------- (MODIFICADO) Lógica de Búsqueda ----------
if submitted and q.strip():
    try:
        # (NUEVO) Lógica para decidir qué funciones llamar
        if search_by == "Dirección":
            rows = search_all_by_address(conn, q, int(limit)) if show_all else search_dedup_by_address(conn, q, int(limit))
        else: # Búsqueda por Código del Proyecto
            rows = search_all_by_code(conn, q, int(limit)) if show_all else search_dedup_by_code(conn, q, int(limit))

        st.write(f"**{len(rows)}** resultado(s).")
        if rows:
            # El código para mostrar los datos no necesita cambios
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
            st.info("Sin coincidencias. Prueba con otros términos.")
    except Exception as e:
        st.error("Ocurrió un error ejecutando la consulta. Verifica que `data.db` esté junto al app y actualizado.")
        st.exception(e)
else:
    st.info("Ingresa un término de búsqueda y presiona **Buscar**.")
