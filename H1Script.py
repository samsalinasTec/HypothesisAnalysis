import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from time import perf_counter
import re

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from BQLoadClass import BQLoad
import numpy as np  

# ----------------------------
# Parámetros generales
# ----------------------------
PROJECT_ID = "sorteostec-ml"
DATE_START = "2024-12-01"
DATE_END   = "2025-11-30"

# Tabla base GA4 (canónica por PRODUCTO, no por boleto)
TABLE_B = "sorteostec-ml.h1.intentos_producto_canonico_web_20241201_20251130"

# Solo se usa en script de VM. Aquí no se deposita ninguna tabla en ningun lado fuera de local.
# Solo son pruebas locales.
OUTPUT_TABLE = ""

# Rutas (AJUSTAR en la VM)
CREDENTIALS_PATH_ML = "/home/sam.salinas/PythonProjects/H1/Data/credentials/sorteostec-ml-5f178b142b6f.json"
LOG_DIR = Path("/home/sam.salinas/PythonProjects/H1/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "h1Logs.log"

OUTPUT_CSV_PROMOS = "/home/sam.salinas/PythonProjects/H1/Data/CSV/ga4_patrones_promociones.csv"
OUTPUT_CSV_FUNNEL = "/home/sam.salinas/PythonProjects/H1/Data/CSV/ga4_patrones_funnel_completo.csv"

# ----------------------------
# Configuración de logging
# ----------------------------
logger = logging.getLogger("h1_patrones_promociones")
logger.setLevel(logging.INFO)

if not logger.handlers:
    fh = RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)


def _df_stats(df: pd.DataFrame, name: str) -> str:
    try:
        mem_mb = df.memory_usage(deep=True).sum() / (1024 ** 2)
        return f"{name}: shape={df.shape}, memory≈{mem_mb:.2f} MB, cols={list(df.columns)}"
    except Exception as e:
        return f"{name}: shape={df.shape}, (mem n/a: {e})"


def _time_block(label: str):
    """Context manager simple para medir tiempos de etapas."""
    class _Timer:
        def __enter__(self):
            self.t0 = perf_counter()
            logger.info(f"▶️  Iniciando: {label}")
            return self

        def __exit__(self, exc_type, exc, tb):
            dt = perf_counter() - self.t0
            if exc:
                logger.exception(f"❌ Error en etapa '{label}' (duración {dt:.2f}s): {exc}")
            else:
                logger.info(f"✅ Finalizado: {label} (duración {dt:.2f}s)")
    return _Timer()


# ----------------------------
# BigQuery client
# ----------------------------
credentialsML = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH_ML)
clientML = bigquery.Client(credentials=credentialsML, project=PROJECT_ID)


# ----------------------------
# Helpers SQL / BigQuery
# ----------------------------
def load_sql(path: str, **params) -> str:
    """
    Lee un archivo .sql y, si se pasan parámetros, hace un .format(**params)
    sobre el contenido.
    """
    sql_text = Path(path).read_text(encoding="utf-8")
    if params:
        sql_text = sql_text.format(**params)
    return sql_text


def execute_ddl(query: str) -> None:
    """Ejecuta un CREATE/REPLACE/DELETE/etc. en BigQuery (sin DataFrame)."""
    job = clientML.query(query)
    job.result()


def execute_query_to_df(query: str):
    """Ejecuta un SELECT en BigQuery y devuelve un DataFrame."""
    job = clientML.query(query)
    return job.to_dataframe()


# ----------------------------
# Lógica de negocio (funciones)
# ----------------------------
def interpretar_cantidad(row):
    """Interpreta la condición de cantidad en lenguaje natural"""
    tipo = row['clave_tipo_cantidad_condicion']
    cant_inicial = row['cantidad_inicial']
    cant_final = row['cantidad_final']

    interpretaciones = {
        1: f"Exactamente {cant_inicial} unidades",
        2: f"Mínimo {cant_inicial} unidades",
        3: f"Máximo {cant_inicial} unidades",
        4: f"Entre {cant_inicial} y {cant_final} unidades",
        5: f"Acumula {cant_inicial} unidades",
        6: f"Por cada {cant_inicial} unidades",
        7: f"Múltiplo de {cant_inicial}"
    }

    return interpretaciones.get(tipo, f"Tipo {tipo}: {cant_inicial}")


def cumple_patron(cantidad, tipo_condicion, cant_inicial, cant_final=None):
    """Verifica si una cantidad cumple con un tipo de condición"""
    if pd.isna(cantidad) or cantidad == 0:
        return False

    if tipo_condicion == 1:  # Exactamente
        return cantidad == cant_inicial
    elif tipo_condicion == 2:  # Mínimo
        return cantidad >= cant_inicial
    elif tipo_condicion == 3:  # Máximo
        return cantidad <= cant_inicial
    elif tipo_condicion == 4:  # Entre
        return cant_inicial <= cantidad <= (cant_final if cant_final else cant_inicial)
    elif tipo_condicion == 5:  # Acumula
        return cantidad >= cant_inicial
    elif tipo_condicion == 6:  # Por cada
        return cantidad >= cant_inicial and cantidad % cant_inicial == 0
    elif tipo_condicion == 7:  # Múltiplo
        return cantidad % cant_inicial == 0 if cant_inicial > 0 else False
    else:
        return False


def evaluar_promociones_sesion(df_sesion, requisitos_multi, vigencia_promo):
    """
    Evalúa promociones multi-producto (combinadas) para TODA la sesión.
    Regresa IDs de promos que están COMPLETAS por etapa.
    - Requisito V1: para cada promo combinada, TODOS sus productos deben sumar
      cantidad >= cantidad_requerida dentro de la sesión, por etapa.
    - Vigencia: si la promo no está activa a la fecha del evento (sesión), se ignora.
    """
    promociones_completas = {'add_cart': [], 'checkout': [], 'purchase': []}

    def _parse_dt_mx(x):
        for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S.%f"):
            try:
                return pd.to_datetime(x, format=fmt)
            except Exception:
                continue
        return pd.NaT

    try:
        fecha_evento = _parse_dt_mx(df_sesion['DATETIME'].iloc[0])
        if pd.isna(fecha_evento):
            return promociones_completas
    except Exception:
        return promociones_completas

    # Por cada promo combinada definida en requisitos_multi
    for pid, reqs in requisitos_multi.items():
        inicio, cierre = vigencia_promo.get(pid, (pd.NaT, pd.NaT))
        if pd.isna(inicio) or pd.isna(cierre) or not (inicio <= fecha_evento <= cierre):
            continue

        ok_add = True
        ok_chk = True
        ok_pur = True

        for req in reqs:
            prod = req['clave_edicion_producto']
            need = req['cantidad_requerida']

            subset = df_sesion[df_sesion['clave_edicion_producto'] == prod]
            s_add = subset['CANTIDAD_ADD_TO_CART'].sum() if 'CANTIDAD_ADD_TO_CART' in subset.columns else 0
            s_chk = subset['CANTIDAD_BEGIN_CHECKOUT'].sum() if 'CANTIDAD_BEGIN_CHECKOUT' in subset.columns else 0
            s_pur = subset['CANTIDAD_PURCHASE'].sum() if 'CANTIDAD_PURCHASE' in subset.columns else 0

            ok_add = ok_add and (s_add >= need)
            ok_chk = ok_chk and (s_chk >= need)
            ok_pur = ok_pur and (s_pur >= need)

            if not (ok_add or ok_chk or ok_pur):
                break

        if ok_add:
            promociones_completas['add_cart'].append(int(pid))
        if ok_chk:
            promociones_completas['checkout'].append(int(pid))
        if ok_pur:
            promociones_completas['purchase'].append(int(pid))

    return promociones_completas


def es_incompleta_simple(cantidad, cant_inicial):
    """
    Heurístico de 'promo simple incompleta':
    - La promo está activa (se valida afuera).
    - NO cumple el patrón.
    - La cantidad es exactamente N - 1 (near miss).
    """
    if pd.isna(cantidad) or pd.isna(cant_inicial):
        return False
    try:
        return int(cantidad) == int(cant_inicial) - 1
    except Exception:
        return False


def detectar_patrones_producto(row, condiciones_df, promociones_completas_sesion,
                               promos_multi, requisitos_multi, vigencia_promo):
    """
    Detecta qué promociones cumple un producto individual (fila).
    - Promos simples:
        * COMPLETAS: igual que antes, vía condiciones_df + cumple_patron.
        * INCOMPLETAS: nuevo criterio de 'near miss' -> cantidad == N - 1.
    - Promos combinadas (V1): operador mínimo (≥) por producto requerido;
      si la sesión cerró el combo -> COMPLETA; si no -> INCOMPLETA.
    """
    clave_producto = row.get('clave_edicion_producto', None)

    resultado_vacio = {
        'PATRON_ADD_CART': 'NO',
        'PROMOS_ADD_CART_COMPLETAS': [],
        'PROMOS_ADD_CART_INCOMPLETAS': [],
        'PROMOS_ADD_CART_TODAS': [],
        'DESC_ADD_CART_COMPLETAS': '',
        'PATRON_BEGIN_CHECKOUT': 'NO',
        'PROMOS_CHECKOUT_COMPLETAS': [],
        'PROMOS_CHECKOUT_INCOMPLETAS': [],
        'PROMOS_CHECKOUT_TODAS': [],
        'DESC_CHECKOUT_COMPLETAS': '',
        'PATRON_PURCHASE': 'NO',
        'PROMOS_PURCHASE_COMPLETAS': [],
        'PROMOS_PURCHASE_INCOMPLETAS': [],
        'PROMOS_PURCHASE_TODAS': [],
        'DESC_PURCHASE_COMPLETAS': ''
    }
    if pd.isna(clave_producto):
        return resultado_vacio

    try:
        fecha_evento = pd.to_datetime(row['DATETIME'], format='%d/%m/%Y %H:%M:%S')
    except Exception:
        return resultado_vacio

    resultados = {
        'add_cart': {'completas': set(), 'incompletas': set(), 'todas': set(), 'desc_completas': set()},
        'checkout': {'completas': set(), 'incompletas': set(), 'todas': set(), 'desc_completas': set()},
        'purchase': {'completas': set(), 'incompletas': set(), 'todas': set(), 'desc_completas': set()}
    }

    # 6.A) PROMOS COMBINADAS
    for pid, reqs in requisitos_multi.items():
        if pid not in promos_multi:
            continue

        req_actual = next((r for r in reqs if r['clave_edicion_producto'] == clave_producto), None)
        if not req_actual:
            continue

        inicio, cierre = vigencia_promo.get(pid, (pd.NaT, pd.NaT))
        if pd.isna(inicio) or pd.isna(cierre) or not (inicio <= fecha_evento <= cierre):
            continue

        need = req_actual['cantidad_requerida']
        c_add = row.get('CANTIDAD_ADD_TO_CART', 0) or 0
        c_chk = row.get('CANTIDAD_BEGIN_CHECKOUT', 0) or 0
        c_pur = row.get('CANTIDAD_PURCHASE', 0) or 0

        if c_add >= need:
            resultados['add_cart']['todas'].add(int(pid))
            if int(pid) in promociones_completas_sesion['add_cart']:
                resultados['add_cart']['completas'].add(int(pid))
            else:
                resultados['add_cart']['incompletas'].add(int(pid))

        if c_chk >= need:
            resultados['checkout']['todas'].add(int(pid))
            if int(pid) in promociones_completas_sesion['checkout']:
                resultados['checkout']['completas'].add(int(pid))
            else:
                resultados['checkout']['incompletas'].add(int(pid))

        if c_pur >= need:
            resultados['purchase']['todas'].add(int(pid))
            if int(pid) in promociones_completas_sesion['purchase']:
                resultados['purchase']['completas'].add(int(pid))
            else:
                resultados['purchase']['incompletas'].add(int(pid))

    # 6.B) PROMOS SIMPLES
    condiciones_producto = condiciones_df[condiciones_df['clave_edicion_producto'] == clave_producto]
    for _, condicion in condiciones_producto.iterrows():
        pid = condicion.get('clave_promocion', None)
        if pd.isna(pid):
            continue
        pid = int(pid)

        if pid in promos_multi:
            continue

        tipo = condicion['clave_tipo_cantidad_condicion']
        cant_inicial = condicion['cantidad_inicial']
        cant_final = condicion['cantidad_final']
        interpretacion = condicion.get('interpretacion', '')
        fecha_inicio = condicion.get('d_inicio_promocion', pd.NaT)
        fecha_cierre = condicion.get('d_cierre_promocion', pd.NaT)

        promo_activa = False
        if pd.notna(fecha_inicio) and pd.notna(fecha_cierre) and (fecha_inicio <= fecha_evento <= fecha_cierre):
            promo_activa = True

        cant_add = row.get('CANTIDAD_ADD_TO_CART', 0) or 0
        cant_chk = row.get('CANTIDAD_BEGIN_CHECKOUT', 0) or 0
        cant_pur = row.get('CANTIDAD_PURCHASE', 0) or 0

        # ADD_TO_CART
        cumple_add = cumple_patron(cant_add, tipo, cant_inicial, cant_final)
        if cumple_add:
            resultados['add_cart']['todas'].add(pid)
            if promo_activa:
                resultados['add_cart']['completas'].add(pid)
                if isinstance(interpretacion, str) and interpretacion:
                    resultados['add_cart']['desc_completas'].add(interpretacion)
        else:
            if promo_activa and cant_add > 0 and es_incompleta_simple(cant_add, cant_inicial):
                resultados['add_cart']['todas'].add(pid)
                resultados['add_cart']['incompletas'].add(pid)

        # BEGIN_CHECKOUT
        cumple_chk = cumple_patron(cant_chk, tipo, cant_inicial, cant_final)
        if cumple_chk:
            resultados['checkout']['todas'].add(pid)
            if promo_activa:
                resultados['checkout']['completas'].add(pid)
                if isinstance(interpretacion, str) and interpretacion:
                    resultados['checkout']['desc_completas'].add(interpretacion)
        else:
            if promo_activa and cant_chk > 0 and es_incompleta_simple(cant_chk, cant_inicial):
                resultados['checkout']['todas'].add(pid)
                resultados['checkout']['incompletas'].add(pid)

        # PURCHASE
        cumple_pur = cumple_patron(cant_pur, tipo, cant_inicial, cant_final)
        if cumple_pur:
            resultados['purchase']['todas'].add(pid)
            if promo_activa:
                resultados['purchase']['completas'].add(pid)
                if isinstance(interpretacion, str) and interpretacion:
                    resultados['purchase']['desc_completas'].add(interpretacion)
        else:
            if promo_activa and cant_pur > 0 and es_incompleta_simple(cant_pur, cant_inicial):
                resultados['purchase']['todas'].add(pid)
                resultados['purchase']['incompletas'].add(pid)

    return {
        'PATRON_ADD_CART': 'SI' if resultados['add_cart']['completas'] else 'NO',
        'PROMOS_ADD_CART_COMPLETAS': sorted(list(resultados['add_cart']['completas'])),
        'PROMOS_ADD_CART_INCOMPLETAS': sorted(list(resultados['add_cart']['incompletas'])),
        'PROMOS_ADD_CART_TODAS': sorted(list(resultados['add_cart']['todas'])),
        'DESC_ADD_CART_COMPLETAS': ' | '.join(sorted(resultados['add_cart']['desc_completas'])),

        'PATRON_BEGIN_CHECKOUT': 'SI' if resultados['checkout']['completas'] else 'NO',
        'PROMOS_CHECKOUT_COMPLETAS': sorted(list(resultados['checkout']['completas'])),
        'PROMOS_CHECKOUT_INCOMPLETAS': sorted(list(resultados['checkout']['incompletas'])),
        'PROMOS_CHECKOUT_TODAS': sorted(list(resultados['checkout']['todas'])),
        'DESC_CHECKOUT_COMPLETAS': ' | '.join(sorted(resultados['checkout']['desc_completas'])),

        'PATRON_PURCHASE': 'SI' if resultados['purchase']['completas'] else 'NO',
        'PROMOS_PURCHASE_COMPLETAS': sorted(list(resultados['purchase']['completas'])),
        'PROMOS_PURCHASE_INCOMPLETAS': sorted(list(resultados['purchase']['incompletas'])),
        'PROMOS_PURCHASE_TODAS': sorted(list(resultados['purchase']['todas'])),
        'DESC_PURCHASE_COMPLETAS': ' | '.join(sorted(resultados['purchase']['desc_completas']))
    }


def _extraer_promos(df_ga4_events_final, col):
    acc = []
    for x in df_ga4_events_final[col]:
        if isinstance(x, list):
            acc.extend(x)
    return set(acc)


def mover_numero_al_final(texto):
    if isinstance(texto, str):
        match = re.match(r"^(\d+)\s*°?\s*(.*)$", texto)
        if match:
            numero = match.group(1)
            resto = match.group(2)
            return f"{resto.strip()} {numero}"
    return texto


def reemplazar_prefijo(texto: str, diccionario: dict) -> str:
    if not isinstance(texto, str):
        return texto

    for clave, valor in diccionario.items():
        if re.match(rf"^{clave}\s*\d+", texto):
            return re.sub(rf"^{clave}", valor, texto)

        if clave in texto:
            return texto.replace(clave, valor)

    return texto


def limpiar_item(nombre: str) -> str:
    if not isinstance(nombre, str):
        return nombre
    return re.sub(r'(\b\d+)\s+\1$', r'\1', nombre)

# ----------------------------
# Helpers promos simples / combinadas (ADD_TO_CART)
# ----------------------------
def _todas_incompletas_fila(row):
    """
    Devuelve todas las promos incompletas ADD_TO_CART de una fila,
    aceptando listas o ndarrays.
    """
    val = row.get("PROMOS_ADD_CART_INCOMPLETAS", [])

    if isinstance(val, (list, np.ndarray)):
        return list(val)

    return []


def has_simple_incomplete(row, promos_multi):
    incompletas = _todas_incompletas_fila(row)
    return any((p not in promos_multi) for p in incompletas)


def has_combined_incomplete(row, promos_multi):
    incompletas = _todas_incompletas_fila(row)
    return any((p in promos_multi) for p in incompletas)


def _todas_completas_fila(row):
    """
    Devuelve todas las promos COMPLETAS ADD_TO_CART de una fila,
    aceptando listas o ndarrays.
    """
    val = row.get("PROMOS_ADD_CART_COMPLETAS", [])

    if isinstance(val, (list, np.ndarray)):
        return list(val)

    return []


def has_simple_complete(row, promos_multi):
    completas = _todas_completas_fila(row)
    return any((p not in promos_multi) for p in completas)


def has_combined_complete(row, promos_multi):
    completas = _todas_completas_fila(row)
    return any((p in promos_multi) for p in completas)



# ----------------------------
# main()
# ----------------------------
def main():
    try:
        logger.info("======== EJECUCIÓN H1 PATRONES PROMOCIONES - INICIO ========")

        # Cargar queries
        with _time_block("Carga de archivos SQL"):
            query_base_patrones = load_sql("./Data/queries/base_patrones.sql")

            query_ga4_events = load_sql(
                "./Data/queries/ga4_events.sql",
                TABLE_B=TABLE_B,
                DATE_START=DATE_START,
                DATE_END=DATE_END,
            )
            query_sorteo = load_sql("./Data/queries/sorteo.sql")
            query_condiciones_promocion = load_sql("./Data/queries/condiciones_promocion.sql")
            query_tipo_cantidad_promocion = load_sql("./Data/queries/tipo_cantidad_promocion.sql")
            query_grupo_condicion_promocion = load_sql("./Data/queries/grupo_condicion_promocion.sql")
            query_catalogo_promociones_fechas = load_sql("./Data/queries/catalogo_promociones_fechas.sql")
            query_promociones_combinadas = load_sql("./Data/queries/promociones_combinadas.sql")
            query_complemento_funnel = load_sql("./Data/queries/complemento_funnel.sql")
            query_procesamiento_patrones = load_sql("./Data/queries/procesamiento_patrones.sql")
            query_patrones_funnel_completo = load_sql("./Data/queries/patrones_funnel_completo.sql")

        # execute_ddl(query_base_patrones) y execute_ddl(query_complemento_funnel)
        # se quedan como llamadas manuales según necesidad (costosas en BQ).

        # Ejecutar queries soporte
        with _time_block("Ejecución de queries BigQuery (GA4 + soporte)"):
            logger.info("Ejecutando query_ga4_events...")
            df_ga4_events = execute_query_to_df(query_ga4_events)
            logger.info(_df_stats(df_ga4_events, "df_ga4_events"))

            logger.info("Ejecutando query_sorteo...")
            df_sorteo = execute_query_to_df(query_sorteo)
            logger.info(_df_stats(df_sorteo, "df_sorteo"))

            logger.info("Ejecutando query_condiciones_promocion...")
            df_condiciones = execute_query_to_df(query_condiciones_promocion)
            logger.info(_df_stats(df_condiciones, "df_condiciones"))

            logger.info("Ejecutando query_tipo_cantidad_promocion...")
            df_tipo_cantidad = execute_query_to_df(query_tipo_cantidad_promocion)
            logger.info(_df_stats(df_tipo_cantidad, "df_tipo_cantidad"))

            logger.info("Ejecutando query_grupo_condicion_promocion...")
            df_grupo_condicion = execute_query_to_df(query_grupo_condicion_promocion)
            logger.info(_df_stats(df_grupo_condicion, "df_grupo_condicion"))

            logger.info("Ejecutando query_catalogo_promociones_fechas...")
            df_fechas_promocion = execute_query_to_df(query_catalogo_promociones_fechas)
            logger.info(_df_stats(df_fechas_promocion, "df_fechas_promocion"))

            logger.info("Ejecutando query_promociones_combinadas...")
            df_promos_combinadas = execute_query_to_df(query_promociones_combinadas)
            logger.info(_df_stats(df_promos_combinadas, "df_promos_combinadas"))

        # Preparar sorteo + GA4 base + montos
        with _time_block("Preparación df_sorteo + df_ga4_events_base + montos"):
            df_sorteo['numero_sorteo_int'] = df_sorteo['numero_sorteo'].fillna(0).astype(int).astype(str)
            df_sorteo['item_completo'] = df_sorteo['desc_sorteo'] + ' ' + df_sorteo['numero_sorteo_int']

            df_ga4_events_base = df_ga4_events.copy()
            df_condiciones_base = df_condiciones.copy()

            df_ga4_events_base = df_ga4_events_base.merge(
                df_sorteo[['item_completo', 'clave_edicion_producto', 'precio_unitario']],
                left_on='ITEM',
                right_on='item_completo',
                how='left'
            )

            df_ga4_events_base['clave_edicion_producto'] = pd.to_numeric(
                df_ga4_events_base['clave_edicion_producto'], errors='coerce'
            ).astype('Int64')

            df_ga4_events_base['MONTO_ADD_TO_CART'] = (
                df_ga4_events_base['precio_unitario'] * df_ga4_events_base['CANTIDAD_ADD_TO_CART']
            )
            df_ga4_events_base['MONTO_BEGIN_CHECKOUT'] = (
                df_ga4_events_base['precio_unitario'] * df_ga4_events_base['CANTIDAD_BEGIN_CHECKOUT']
            )
            df_ga4_events_base['MONTO_PURCHASE'] = (
                df_ga4_events_base['precio_unitario'] * df_ga4_events_base['CANTIDAD_PURCHASE']
            )

            df_ga4_events_base = df_ga4_events_base.drop('item_completo', axis=1)
            logger.info(_df_stats(df_ga4_events_base, "df_ga4_events_base"))

        # Condiciones + fechas promo
        with _time_block("Merge df_condiciones con grupo_condicion + fechas_promocion"):
            df_condiciones_base = df_condiciones_base.merge(
                df_grupo_condicion[['clave_grupo_condiciones', 'clave_promocion']],
                on='clave_grupo_condiciones',
                how='left'
            )

            df_condiciones_base = df_condiciones_base.merge(
                df_fechas_promocion[['clave_promocion', 'd_inicio_promocion', 'd_cierre_promocion']],
                on='clave_promocion',
                how='left'
            )

            total = len(df_condiciones_base)
            con_fechas = df_condiciones_base['d_inicio_promocion'].notna().sum()
            sin_fechas = df_condiciones_base['d_inicio_promocion'].isna().sum()

            logger.info("=== RESUMEN CONDICIONES ===")
            logger.info("Total de filas: %d", total)
            logger.info("Condiciones con fechas: %d", con_fechas)
            logger.info("Condiciones sin fechas: %d", sin_fechas)

        # Promos combinadas
        with _time_block("Normalización de promociones combinadas + vigencia_promo"):
            df_combinadas = df_promos_combinadas.copy()

            rename_map = {
                'CLAVE_PROMOCION': 'clave_promocion',
                'CLAVE_EDICION_PRODUCTO': 'clave_edicion_producto',
                'CANTIDAD_INICIAL': 'cantidad_inicial',
            }
            df_combinadas = df_combinadas.rename(columns={c: rename_map.get(c, c) for c in df_combinadas.columns})

            for c in ['clave_promocion', 'clave_edicion_producto', 'cantidad_inicial']:
                if c in df_combinadas.columns:
                    df_combinadas[c] = pd.to_numeric(df_combinadas[c], errors='coerce')

            df_combinadas = df_combinadas.drop_duplicates(
                subset=['clave_promocion', 'clave_edicion_producto'],
                keep='last'
            ).reset_index(drop=True)

            promos_multi = set(df_combinadas['clave_promocion'].dropna().astype(int).unique())

            requisitos_multi = {}
            for pid, grp in df_combinadas.groupby('clave_promocion'):
                if pd.isna(pid):
                    continue
                reqs = []
                for _, r in grp.iterrows():
                    if pd.isna(r['clave_edicion_producto']) or pd.isna(r['cantidad_inicial']):
                        continue
                    reqs.append({
                        'clave_edicion_producto': int(r['clave_edicion_producto']),
                        'cantidad_requerida': int(r['cantidad_inicial'])
                    })
                if reqs:
                    requisitos_multi[int(pid)] = reqs

            df_fechas_promocion['d_inicio_promocion'] = pd.to_datetime(
                df_fechas_promocion['d_inicio_promocion'], errors='coerce'
            )
            df_fechas_promocion['d_cierre_promocion'] = pd.to_datetime(
                df_fechas_promocion['d_cierre_promocion'], errors='coerce'
            )

            vigencia_promo = {}
            for _, r in df_fechas_promocion[['clave_promocion', 'd_inicio_promocion', 'd_cierre_promocion']].drop_duplicates().iterrows():
                pid = r['clave_promocion']
                if pd.isna(pid):
                    continue
                vigencia_promo[int(pid)] = (r['d_inicio_promocion'], r['d_cierre_promocion'])

            logger.info("Promociones multi-producto detectadas: %d", len(promos_multi))

        # Enriquecer condiciones con interpretación
        with _time_block("Enriquecimiento df_condiciones_enriquecido"):
            df_condiciones_enriquecido = df_condiciones_base.merge(
                df_tipo_cantidad[['clave_tipo_cantidad_condicion', 'descripcion']],
                on='clave_tipo_cantidad_condicion',
                how='left'
            )

            df_condiciones_enriquecido['interpretacion'] = df_condiciones_enriquecido.apply(
                interpretar_cantidad, axis=1
            )

            df_condiciones_enriquecido['d_inicio_promocion'] = pd.to_datetime(
                df_condiciones_enriquecido['d_inicio_promocion']
            )
            df_condiciones_enriquecido['d_cierre_promocion'] = pd.to_datetime(
                df_condiciones_enriquecido['d_cierre_promocion']
            )

            logger.info("Condiciones enriquecidas: %d", len(df_condiciones_enriquecido))

        # Detección de patrones con evaluación por sesión
        with _time_block("Detección de patrones (por sesión y producto)"):
            logger.info("Detectando patrones con validación completa (con combinadas V1)...")
            sesiones = df_ga4_events_base.groupby(['USER', 'SESION'])
            total_sesiones = len(sesiones)
            logger.info("Total sesiones: %d", total_sesiones)

            resultados_list = []
            for idx_sesion, ((user, sesion), df_sesion) in enumerate(sesiones, start=1):
                if idx_sesion % 1000 == 0:
                    logger.info("Progreso sesiones: %d / %d (%.1f%%)",
                                idx_sesion, total_sesiones, 100 * idx_sesion / total_sesiones)

                promociones_completas_sesion = evaluar_promociones_sesion(
                    df_sesion=df_sesion,
                    requisitos_multi=requisitos_multi,
                    vigencia_promo=vigencia_promo
                )

                for idx_row, row in df_sesion.iterrows():
                    resultado = detectar_patrones_producto(
                        row=row,
                        condiciones_df=df_condiciones_enriquecido,
                        promociones_completas_sesion=promociones_completas_sesion,
                        promos_multi=promos_multi,
                        requisitos_multi=requisitos_multi,
                        vigencia_promo=vigencia_promo
                    )
                    resultado['index'] = idx_row
                    resultados_list.append(resultado)

            df_resultados = pd.DataFrame(resultados_list)
            df_resultados = df_resultados.set_index('index').sort_index()

            df_ga4_events_final = pd.concat([df_ga4_events_base, df_resultados], axis=1)
            logger.info(_df_stats(df_ga4_events_final, "df_ga4_events_final"))

        # Columnas resumen
        with _time_block("Cálculo columnas resumen patrón completo / incompleto"):
            df_ga4_events_final['TIENE_PATRON_COMPLETO'] = df_ga4_events_final.apply(
                lambda row: 'SI' if (row['PATRON_ADD_CART'] == 'SI' or
                                     row['PATRON_BEGIN_CHECKOUT'] == 'SI' or
                                     row['PATRON_PURCHASE'] == 'SI') else 'NO',
                axis=1
            )

            df_ga4_events_final['TIENE_PATRON_INCOMPLETO'] = df_ga4_events_final.apply(
                lambda row: 'SI' if (len(row['PROMOS_ADD_CART_INCOMPLETAS']) > 0 or
                                     len(row['PROMOS_CHECKOUT_INCOMPLETAS']) > 0 or
                                     len(row['PROMOS_PURCHASE_INCOMPLETAS']) > 0) else 'NO',
                axis=1
            )

            logger.info("=== RESUMEN GENERAL ===")
            total_filas = len(df_ga4_events_final)
            completos = len(df_ga4_events_final[df_ga4_events_final['TIENE_PATRON_COMPLETO'] == 'SI'])
            incompletos = len(df_ga4_events_final[df_ga4_events_final['TIENE_PATRON_INCOMPLETO'] == 'SI'])
            logger.info("Total de filas analizadas: %d", total_filas)
            logger.info("Filas con patrón COMPLETO: %d", completos)
            logger.info("Filas con patrón INCOMPLETO: %d", incompletos)

            logger.info("=== PATRONES COMPLETOS POR ETAPA ===")
            add_comp = len(df_ga4_events_final[df_ga4_events_final['PATRON_ADD_CART'] == 'SI'])
            bc_comp = len(df_ga4_events_final[df_ga4_events_final['PATRON_BEGIN_CHECKOUT'] == 'SI'])
            pur_comp = len(df_ga4_events_final[df_ga4_events_final['PATRON_PURCHASE'] == 'SI'])
            logger.info("ADD_TO_CART completo: %d", add_comp)
            logger.info("BEGIN_CHECKOUT completo: %d", bc_comp)
            logger.info("PURCHASE completo: %d", pur_comp)

        # Análisis multi-producto y guardado CSV patrones_promociones
        with _time_block("Análisis multi-producto + guardado CSV patrones_promociones"):

            df_ga4_events_final.to_csv(OUTPUT_CSV_PROMOS, index=False)
            logger.info("Archivo guardado: %s", OUTPUT_CSV_PROMOS)


        # Carga a BigQuery (tabla patrones_promociones)
        with _time_block("Carga df_ga4_events_final a BigQuery (BQLoad)"):
            loader = BQLoad(credentials_path=CREDENTIALS_PATH_ML)
            PROJECT_DATASET = "sorteostec-ml.h1"
            TABLE_NAME = "ga4_patrones_promociones_20241201_20251130"
            table = f"{PROJECT_DATASET}.{TABLE_NAME}"

            logger.info("Eliminando tabla destino (si existe): %s", table)
            loader.delete_tables(table)

            logger.info("Cargando df_ga4_events_final a %s", table)
            loader.load_table(
                df=df_ga4_events_final,
                destination=table,
                schema=[],
            )

        # Procesamiento funnel completo
        with _time_block("Procesamiento patrones funnel completo (DDL + SELECT)"):
            execute_ddl(query_procesamiento_patrones)
            df_patrones_funnel_completo = execute_query_to_df(query_patrones_funnel_completo)
            logger.info(_df_stats(df_patrones_funnel_completo, "df_patrones_funnel_completo"))

        # Limpieza ITEM + precios + montos y guardado CSV funnel
        with _time_block("Limpieza ITEM, inferencia precio_unitario, montos y guardado CSV funnel"):
            dictCambiosNombre = {
                "LQ": "Sorteo Lo Quiero",
                "Sorteo Efectivo": "Efectivo",
            }

            df_filtrado_copy = df_patrones_funnel_completo.copy()

            df_filtrado_copy["ITEM"] = df_filtrado_copy["ITEM"].apply(mover_numero_al_final)
            df_filtrado_copy["ITEM"] = df_filtrado_copy["ITEM"].apply(
                lambda x: reemplazar_prefijo(x, dictCambiosNombre)
            )
            df_filtrado_copy["ITEM"] = df_filtrado_copy["ITEM"].apply(limpiar_item)

            df_sorteo['numero_sorteo_int'] = df_sorteo['numero_sorteo'].fillna(0).astype(int).astype(str)
            df_sorteo['item_completo'] = df_sorteo['desc_sorteo'] + ' ' + df_sorteo['numero_sorteo_int']

            precio_por_item = (
                df_sorteo.drop_duplicates('item_completo')
                        .set_index('item_completo')['precio_unitario']
            )

            mask_precio = df_filtrado_copy['precio_unitario_inferido'].isna()
            df_filtrado_copy.loc[mask_precio, 'precio_unitario_inferido'] = (
                df_filtrado_copy.loc[mask_precio, 'ITEM'].map(precio_por_item)
            )

            df_filtrado_copy['qty_add_to_cart'] = df_filtrado_copy['qty_add_to_cart'].astype(float)
            df_filtrado_copy['qty_begin_checkout'] = df_filtrado_copy['qty_begin_checkout'].astype(float)
            df_filtrado_copy['qty_purchase'] = df_filtrado_copy['qty_purchase'].astype(float)
            df_filtrado_copy['precio_unitario_inferido'] = df_filtrado_copy['precio_unitario_inferido'].astype(float)

            df_filtrado_copy['MONTO_ADD_TO_CART'] = (
                df_filtrado_copy['precio_unitario_inferido'] * df_filtrado_copy['qty_add_to_cart']
            )
            df_filtrado_copy['MONTO_BEGIN_CHECKOUT'] = (
                df_filtrado_copy['precio_unitario_inferido'] * df_filtrado_copy['qty_begin_checkout']
            )
            df_filtrado_copy['MONTO_PURCHASE'] = (
                df_filtrado_copy['precio_unitario_inferido'] * df_filtrado_copy['qty_purchase']
            )

            df_filtrado_copy.to_csv(OUTPUT_CSV_FUNNEL, index=False)
            logger.info("Archivo funnel completo guardado: %s", OUTPUT_CSV_FUNNEL)
            logger.info(_df_stats(df_filtrado_copy, "df_filtrado_copy"))

        # Agregar nivel sesión + KPIs
        with _time_block("Agregación nivel sesión + KPIs"):
            df_sesiones = (
                df_filtrado_copy
                .groupby(["user_pseudo_id", "session_id"], as_index=False)
                .agg({
                    "categoria_login": "first",
                    "MONTO_BEGIN_CHECKOUT": "sum",
                    "MONTO_PURCHASE": "sum",
                    "PATRON_BEGIN_CHECKOUT": lambda s: (s == "SI").any(),
                })
            )

            df_sesiones["tiene_login"] = df_sesiones["categoria_login"] != "SIN LOGIN EN SESIÓN"
            df_sesiones["tiene_purchase"] = df_sesiones["MONTO_PURCHASE"] > 0
            df_sesiones["tiene_patron_bc"] = df_sesiones["PATRON_BEGIN_CHECKOUT"]
            df_sesiones["es_sin_registro_con_purchase"] = (
                (df_sesiones["categoria_login"] == "SIN LOGIN EN SESIÓN")
                & df_sesiones["tiene_purchase"]
            )

            # 1) Total sesiones
            total_sesiones_count = len(df_sesiones)
            total_sesiones_monto_begin_checkout = df_sesiones["MONTO_BEGIN_CHECKOUT"].sum()
            total_sesiones_monto_purchase = df_sesiones["MONTO_PURCHASE"].sum()

            # 2) Sesiones con login
            mask_login = df_sesiones["tiene_login"]

            sesiones_con_login_count = mask_login.sum()
            sesiones_con_login_monto_begin_checkout = df_sesiones.loc[
                mask_login, "MONTO_BEGIN_CHECKOUT"
            ].sum()
            sesiones_con_login_monto_purchase = df_sesiones.loc[
                mask_login, "MONTO_PURCHASE"
            ].sum()

            # 3) Sesiones con login sin purchase
            mask_login_sin_purchase = df_sesiones["tiene_login"] & (~df_sesiones["tiene_purchase"])

            sesiones_con_login_sin_purchase_count = mask_login_sin_purchase.sum()
            sesiones_con_login_sin_purchase_monto_begin_checkout = df_sesiones.loc[
                mask_login_sin_purchase, "MONTO_BEGIN_CHECKOUT"
            ].sum()
            sesiones_con_login_sin_purchase_monto_purchase = df_sesiones.loc[
                mask_login_sin_purchase, "MONTO_PURCHASE"
            ].sum()

            # 4) Sesiones con login, sin purchase con patrón begin checkout
            mask_login_sin_purchase_con_patron_bc = (
                df_sesiones["tiene_login"]
                & (~df_sesiones["tiene_purchase"])
                & df_sesiones["tiene_patron_bc"]
            )

            sesiones_con_login_sin_purchase_con_patron_bc_count = (
                mask_login_sin_purchase_con_patron_bc.sum()
            )
            sesiones_con_login_sin_purchase_con_patron_bc_monto_begin_checkout = df_sesiones.loc[
                mask_login_sin_purchase_con_patron_bc, "MONTO_BEGIN_CHECKOUT"
            ].sum()
            sesiones_con_login_sin_purchase_con_patron_bc_monto_purchase = df_sesiones.loc[
                mask_login_sin_purchase_con_patron_bc, "MONTO_PURCHASE"
            ].sum()

            # 5) Sesiones sin login, sin purchase con patrón begin checkout
            mask_sin_login_sin_purchase_con_patron_bc = (
                (~df_sesiones["tiene_login"])
                & (~df_sesiones["tiene_purchase"])
                & df_sesiones["tiene_patron_bc"]
            )

            sesiones_sin_login_sin_purchase_con_patron_bc_count = (
                mask_sin_login_sin_purchase_con_patron_bc.sum()
            )
            sesiones_sin_login_sin_purchase_con_patron_bc_monto_begin_checkout = df_sesiones.loc[
                mask_sin_login_sin_purchase_con_patron_bc, "MONTO_BEGIN_CHECKOUT"
            ].sum()
            sesiones_sin_login_sin_purchase_con_patron_bc_monto_purchase = df_sesiones.loc[
                mask_sin_login_sin_purchase_con_patron_bc, "MONTO_PURCHASE"
            ].sum()

            # 6) Sesiones "login sin registrar", con purchase
            mask_sin_registro_con_purchase = df_sesiones["es_sin_registro_con_purchase"]

            sesiones_login_sin_registrar_con_purchase_count = (
                mask_sin_registro_con_purchase.sum()
            )
            sesiones_login_sin_registrar_con_purchase_monto_begin_checkout = df_sesiones.loc[
                mask_sin_registro_con_purchase, "MONTO_BEGIN_CHECKOUT"
            ].sum()
            sesiones_login_sin_registrar_con_purchase_monto_purchase = df_sesiones.loc[
                mask_sin_registro_con_purchase, "MONTO_PURCHASE"
            ].sum()

            # 7) Sesiones "login sin registrar", con purchase y patrón begin checkout
            mask_sin_registro_con_purchase_con_patron_bc = (
                df_sesiones["es_sin_registro_con_purchase"]
                & df_sesiones["tiene_patron_bc"]
            )

            sesiones_login_sin_registrar_con_purchase_con_patron_bc_count = (
                mask_sin_registro_con_purchase_con_patron_bc.sum()
            )
            sesiones_login_sin_registrar_con_purchase_con_patron_bc_monto_begin_checkout = (
                df_sesiones.loc[
                    mask_sin_registro_con_purchase_con_patron_bc, "MONTO_BEGIN_CHECKOUT"
                ].sum()
            )
            sesiones_login_sin_registrar_con_purchase_con_patron_bc_monto_purchase = (
                df_sesiones.loc[
                    mask_sin_registro_con_purchase_con_patron_bc, "MONTO_PURCHASE"
                ].sum()
            )

            KPI_sesiones_resumen = (
                (sesiones_con_login_sin_purchase_con_patron_bc_monto_begin_checkout /
                 sesiones_con_login_monto_begin_checkout)
                * sesiones_sin_login_sin_purchase_con_patron_bc_monto_begin_checkout
            )

            logger.info("=== RESUMEN SESIONES / KPIs ===")
            logger.info("Total sesiones: %d", total_sesiones_count)
            logger.info("Sesiones con login: %d", sesiones_con_login_count)
            logger.info("Sesiones con login sin purchase: %d", sesiones_con_login_sin_purchase_count)
            logger.info("Sesiones con login sin purchase con patrón BC: %d",
                        sesiones_con_login_sin_purchase_con_patron_bc_count)
            logger.info("Sesiones sin login, sin purchase con patrón BC: %d",
                        sesiones_sin_login_sin_purchase_con_patron_bc_count)
            logger.info("Sesiones 'sin registro' con purchase: %d",
                        sesiones_login_sin_registrar_con_purchase_count)
            logger.info("Sesiones 'sin registro' con purchase y patrón BC: %d",
                        sesiones_login_sin_registrar_con_purchase_con_patron_bc_count)
            logger.info("KPI_sesiones_resumen: %f", KPI_sesiones_resumen)


        # Análisis promos simples vs combinadas (incompletas/completas)
        with _time_block("Análisis promos simples/combinadas por fila y sesión"):
            df_flags = df_filtrado_copy.copy()

            # Flags fila a fila
            df_flags["HAS_SIMPLE_INCOMPLETE"] = df_flags.apply(
                has_simple_incomplete, axis=1, promos_multi=promos_multi
            )
            df_flags["HAS_COMBINED_INCOMPLETE"] = df_flags.apply(
                has_combined_incomplete, axis=1, promos_multi=promos_multi
            )
            df_flags["HAS_SIMPLE_COMPLETA"] = df_flags.apply(
                has_simple_complete, axis=1, promos_multi=promos_multi
            )
            df_flags["HAS_COMBINED_COMPLETA"] = df_flags.apply(
                has_combined_complete, axis=1, promos_multi=promos_multi
            )

            total_filas_flags = len(df_flags)
            logger.info("=== RESUMEN FILAS (ADD_TO_CART) ===")
            logger.info("Total filas funnel: %d", total_filas_flags)
            logger.info("Filas con promo simple incompleta: %d",
                        df_flags["HAS_SIMPLE_INCOMPLETE"].sum())
            logger.info("Filas con promo combinada incompleta: %d",
                        df_flags["HAS_COMBINED_INCOMPLETE"].sum())
            logger.info("Filas con ambas (simple y combinada incompleta): %d",
                        len(df_flags[(df_flags["HAS_SIMPLE_INCOMPLETE"])
                                     & (df_flags["HAS_COMBINED_INCOMPLETE"])]))

            # Agregación a nivel sesión
            sesion_flags = (
                df_flags
                .groupby(["user_pseudo_id", "session_id"])
                .agg(
                    HAS_SIMPLE_INCOMPLETE_SESION=("HAS_SIMPLE_INCOMPLETE", "any"),
                    HAS_COMBINED_INCOMPLETE_SESION=("HAS_COMBINED_INCOMPLETE", "any"),
                    HAS_SIMPLE_COMPLETA_SESION=("HAS_SIMPLE_COMPLETA", "any"),
                    HAS_COMBINED_COMPLETA_SESION=("HAS_COMBINED_COMPLETA", "any"),
                )
                .reset_index()
            )

            total_sesiones_flags = len(sesion_flags)

            logger.info("=== RESUMEN SESIONES (ADD_TO_CART) ===")
            logger.info("Total sesiones (para promos simples/combinadas): %d",
                        total_sesiones_flags)
            logger.info("Sesiones con simple incompleta: %d",
                        sesion_flags["HAS_SIMPLE_INCOMPLETE_SESION"].sum())
            logger.info("Sesiones con combinada incompleta: %d",
                        sesion_flags["HAS_COMBINED_INCOMPLETE_SESION"].sum())
            logger.info("Sesiones con ambas (simple y combinada incompleta): %d",
                        len(
                            sesion_flags[
                                (sesion_flags["HAS_SIMPLE_INCOMPLETE_SESION"])
                                & (sesion_flags["HAS_COMBINED_INCOMPLETE_SESION"])
                            ]
                        ))

            # Porcentajes sobre total de sesiones
            resumen_pct = {
                "pct_sesiones_simple_incompleta": sesion_flags[
                    "HAS_SIMPLE_INCOMPLETE_SESION"
                ].mean(),
                "pct_sesiones_combinada_incompleta": sesion_flags[
                    "HAS_COMBINED_INCOMPLETE_SESION"
                ].mean(),
                "pct_sesiones_ambas": (
                    (
                        (sesion_flags["HAS_SIMPLE_INCOMPLETE_SESION"])
                        & (sesion_flags["HAS_COMBINED_INCOMPLETE_SESION"])
                    ).mean()
                ),
            }

            logger.info("=== PORCENTAJES SOBRE TOTAL DE SESIONES ===")
            for k, v in resumen_pct.items():
                logger.info("%s: %.2f %%", k, v * 100)

            # Condicional: solo sesiones donde existe cada tipo
            mask_sesion_tiene_simple = (
                sesion_flags["HAS_SIMPLE_COMPLETA_SESION"]
                | sesion_flags["HAS_SIMPLE_INCOMPLETE_SESION"]
            )
            total_sesiones_con_simple = mask_sesion_tiene_simple.sum()
            total_sesiones_simple_incomp = sesion_flags[
                "HAS_SIMPLE_INCOMPLETE_SESION"
            ].sum()

            if total_sesiones_con_simple > 0:
                pct_simple_incompleta_sobre_con_simple = (
                    total_sesiones_simple_incomp / total_sesiones_con_simple
                )
            else:
                pct_simple_incompleta_sobre_con_simple = 0.0

            mask_sesion_tiene_combinada = (
                sesion_flags["HAS_COMBINED_COMPLETA_SESION"]
                | sesion_flags["HAS_COMBINED_INCOMPLETE_SESION"]
            )
            total_sesiones_con_combinada = mask_sesion_tiene_combinada.sum()
            total_sesiones_combinada_incomp = sesion_flags[
                "HAS_COMBINED_INCOMPLETE_SESION"
            ].sum()

            if total_sesiones_con_combinada > 0:
                pct_combinada_incompleta_sobre_con_combinada = (
                    total_sesiones_combinada_incomp / total_sesiones_con_combinada
                )
            else:
                pct_combinada_incompleta_sobre_con_combinada = 0.0

            logger.info("=== Simples (condicionado a sesiones con simple) ===")
            logger.info(
                "Sesiones con alguna promo simple (completa o incompleta): %d",
                total_sesiones_con_simple,
            )
            logger.info(
                "Sesiones con promo simple incompleta: %d",
                total_sesiones_simple_incomp,
            )
            logger.info(
                "Proporción de simples incompletas dentro de sesiones con simple: %.2f %%",
                pct_simple_incompleta_sobre_con_simple * 100,
            )

            logger.info("=== Combinadas (condicionado a sesiones con combinada) ===")
            logger.info(
                "Sesiones con alguna promo combinada (completa o incompleta): %d",
                total_sesiones_con_combinada,
            )
            logger.info(
                "Sesiones con promo combinada incompleta: %d",
                total_sesiones_combinada_incomp,
            )
            logger.info(
                "Proporción de combinadas incompletas dentro de sesiones con combinada: %.2f %%",
                pct_combinada_incompleta_sobre_con_combinada * 100,
            )


        logger.info("======== EJECUCIÓN H1 PATRONES PROMOCIONES - FIN EXITOSO ========")


    except Exception as e:
        logger.exception("Ejecución abortada por error: %s", e)
        raise


if __name__ == "__main__":
    main()
