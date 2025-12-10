import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from time import perf_counter
import re

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import numpy as np


# ----------------------------
# Parámetros generales
# ----------------------------
PROJECT_ID = "sorteostec-ml"

CREDENTIALS_PATH_ML = "/home/sam.salinas/PythonProjects/H1/Data/credentials/sorteostec-ml-5f178b142b6f.json"
LOG_DIR = Path("/home/sam.salinas/PythonProjects/H1/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "h1Logs_post.log"

OUTPUT_CSV_FUNNEL = "/home/sam.salinas/PythonProjects/H1/Data/CSV/ga4_patrones_funnel_completo_post.csv"


# ----------------------------
# Configuración de logging
# ----------------------------
logger = logging.getLogger("h1_patrones_promociones_post")
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


def execute_query_to_df(query: str) -> pd.DataFrame:
    """Ejecuta un SELECT en BigQuery y devuelve un DataFrame."""
    job = clientML.query(query)
    return job.to_dataframe()


# ----------------------------
# Helpers de limpieza de ITEM
# ----------------------------
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
    # Quita duplicación de número final tipo "Sorteo XYZ 123 123"
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
        logger.info("======== EJECUCIÓN H1 POST-PROCESO PATRONES - INICIO ========")

        # Cargar queries necesarias
        with _time_block("Carga de archivos SQL"):
            query_sorteo = load_sql("./Data/queries/sorteo.sql")
            query_promociones_combinadas = load_sql("./Data/queries/promociones_combinadas.sql")
            query_patrones_funnel_completo = load_sql("./Data/queries/patrones_funnel_completo.sql")

        # Ejecutar queries base (solo lo mínimo)
        with _time_block("Ejecución de queries BigQuery (sorteo + promos_combinadas + funnel)"):
            logger.info("Ejecutando query_sorteo...")
            df_sorteo = execute_query_to_df(query_sorteo)
            logger.info(_df_stats(df_sorteo, "df_sorteo"))

            logger.info("Ejecutando query_promociones_combinadas...")
            df_promos_combinadas = execute_query_to_df(query_promociones_combinadas)
            logger.info(_df_stats(df_promos_combinadas, "df_promos_combinadas"))

            logger.info("Ejecutando query_patrones_funnel_completo...")
            df_patrones_funnel_completo = execute_query_to_df(query_patrones_funnel_completo)
            logger.info(_df_stats(df_patrones_funnel_completo, "df_patrones_funnel_completo"))

        # Normalizar promos combinadas -> promos_multi
        with _time_block("Normalización promociones combinadas (promos_multi)"):
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
            logger.info("Promociones multi-producto detectadas: %d", len(promos_multi))

        # Limpieza ITEM + inferencia precio_unitario + montos
        with _time_block("Limpieza ITEM, inferencia precio_unitario, montos y guardado CSV funnel"):
            dictCambiosNombre = {
                "LQ": "Sorteo Lo Quiero",
                "Sorteo Efectivo": "Efectivo",
            }

            df_filtrado_copy = df_patrones_funnel_completo.copy()

            # Limpieza de ITEM
            df_filtrado_copy["ITEM"] = df_filtrado_copy["ITEM"].apply(mover_numero_al_final)
            df_filtrado_copy["ITEM"] = df_filtrado_copy["ITEM"].apply(
                lambda x: reemplazar_prefijo(x, dictCambiosNombre)
            )
            df_filtrado_copy["ITEM"] = df_filtrado_copy["ITEM"].apply(limpiar_item)

            # Construir item_completo en sorteo y mapear precios
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

            # Cast a float
            df_filtrado_copy['qty_add_to_cart'] = df_filtrado_copy['qty_add_to_cart'].astype(float)
            df_filtrado_copy['qty_begin_checkout'] = df_filtrado_copy['qty_begin_checkout'].astype(float)
            df_filtrado_copy['qty_purchase'] = df_filtrado_copy['qty_purchase'].astype(float)
            df_filtrado_copy['precio_unitario_inferido'] = df_filtrado_copy['precio_unitario_inferido'].astype(float)

            # Montos
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

            # OJO: aquí usas el mismo texto que venga en tus datos ("SIN LOGIN EN SESIÓN" / "SIN LOGIN EN SESION")
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
                 sesiones_con_login_monto_begin_checkout
                 if sesiones_con_login_monto_begin_checkout > 0 else 0.0)
                * sesiones_sin_login_sin_purchase_con_patron_bc_monto_begin_checkout
            )

            logger.info("=== RESUMEN SESIONES / KPIs ===")
            logger.info("Total sesiones: %d", total_sesiones_count)
            logger.info("Total MONTO_BEGIN_CHECKOUT: %f", total_sesiones_monto_begin_checkout)
            logger.info("Total MONTO_PURCHASE: %f", total_sesiones_monto_purchase)

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

        logger.info("======== EJECUCIÓN H1 POST-PROCESO PATRONES - FIN EXITOSO ========")

    except Exception as e:
        logger.exception("Ejecución abortada por error: %s", e)
        raise


if __name__ == "__main__":
    main()
