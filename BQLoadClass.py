import logging
from typing import List, Optional
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Crear logger específico
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Evitar duplicar handlers
if not logger.handlers:
    # Crear directorio
    log_dir = '/home/sam.salinas/PythonProjects/DataIntelligenceRepositoryScripts/src/Dashboard_nacional/Scripts/Load/'
    os.makedirs(log_dir, exist_ok=True)
    
    # Formatter común
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    
    # Handler para archivo
    file_handler = logging.FileHandler(f'{log_dir}EjecucionLoad.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Handler para consola (opcional, para ver logs también en terminal)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.propagate = False


def standardize_date_columns(df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
    """
    Función reutilizable para convertir columnas de fecha usando múltiples formatos.
    
    Args:
        df: DataFrame con las columnas a convertir
        date_columns: Lista de nombres de columnas que contienen fechas
    
    Returns:
        DataFrame con las columnas de fecha convertidas a datetime
    """
    # Formatos de fecha identificados en el proyecto
    DATE_FORMATS = [
        '%Y-%m-%d',              # 2024-12-01
        '%d/%m/%Y',              # 01/12/2024  
        '%Y-%m-%d %H:%M:%S',     # 2024-12-01 15:30:45
        '%d/%m/%Y %H:%M',        # 11/05/2023 09:45
        'mixed'                  # Múltiples formatos mezclados (respaldo)
    ]
    
    df_copy = df.copy()
    
    for col in date_columns:
        if col not in df_copy.columns:
            logger.warning(f"⚠️ Columna {col} no encontrada en el DataFrame")
            continue
        
        converted = False
        original_values = df_copy[col].copy()
        original_na_count = df_copy[col].isna().sum()
        
        # Intentar cada formato hasta que uno funcione completamente
        for date_format in DATE_FORMATS:
            try:
                df_copy[col] = pd.to_datetime(df_copy[col], format=date_format)
                
                # Verificar que la conversión fue exitosa (no aumentaron mucho los NaN)
                new_na_count = df_copy[col].isna().sum()
                if new_na_count <= original_na_count + 1:  # Tolerancia mínima
                    logger.info(f"✅ Columna {col} convertida con formato {date_format}")
                    converted = True
                    break
                else:
                    # La conversión falló, restaurar valores originales para siguiente intento
                    df_copy[col] = original_values.copy()
                    
            except:
                # Si hay error, restaurar valores originales para siguiente intento
                df_copy[col] = original_values.copy()
                continue
        
        if not converted:
            logger.error(f"❌ No se pudo convertir la columna {col} con ningún formato disponible")
    
    return df_copy

class BQLoad:
    def __init__(
        self,
        credentials_path: Optional[str] = None,
        credentials: Optional[service_account.Credentials] = None,
        project: Optional[str] = None
    ):
        if credentials is None and credentials_path is None:
            raise ValueError("Debes proporcionar credenciales o ruta a credenciales")
        self.credentials = (
            credentials
            or service_account.Credentials.from_service_account_file(credentials_path)
        )
        self.client = bigquery.Client(
            credentials=self.credentials,
            project=project or self.credentials.project_id
        )

    def delete_tables(self, tables: List[str]) -> None:
        for table in tables:
            try:
                self.client.delete_table(table)
                logger.info(f"✅ Tabla eliminada: {table}")
            except Exception as e:
                logger.warning(f"⚠️ No se pudo eliminar o no existe la tabla {table}: {e}")

    def load_table(
        self,
        df: pd.DataFrame,
        destination: str,
        schema: List[bigquery.SchemaField],
        write_disposition: str = "WRITE_TRUNCATE"
    ) -> None:
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=write_disposition
        )
        logger.info(f"🔄 Cargando {destination} ({len(df)} filas)…")
        job = self.client.load_table_from_dataframe(
            df, destination, job_config=job_config
        )
        job.result()  # Espera a que termine
        logger.info(f"✅ Carga completada: {destination}")

    def load_from_csv(
        self,
        csv_path: str,
        destination: str,
        schema: List[bigquery.SchemaField],
        read_csv_kwargs: dict = None
    ) -> None:
        read_csv_kwargs = read_csv_kwargs or {}
        df = pd.read_csv(csv_path, **read_csv_kwargs)

        # Identificar columnas de fecha del esquema
        date_columns = [field.name for field in schema if field.field_type in ("DATE", "DATETIME")]
        
        # Aplicar estandarización de fechas
        if date_columns:
            logger.info(f"🔄 Estandarizando fechas para {destination}: {date_columns}")
            df = standardize_date_columns(df, date_columns)


        self.load_table(df, destination, schema)