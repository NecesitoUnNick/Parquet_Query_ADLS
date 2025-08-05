import logging
import time
import polars as pl
from typing import Dict, Any, Optional

from app.core.config import settings
from app.services.adls import get_adls_client, download_parquet_file_to_buffer

# Configurar un logger específico para este módulo
logger = logging.getLogger(__name__)

# Usamos un diccionario como un simple almacén de datos en memoria.
# Esto sigue un patrón similar a un Singleton y es accesible desde toda la aplicación.
data_store: Dict[str, Any] = {
    "dataframe": None,      # Contendrá el DataFrame principal de Polars
    "filter_index": None, # Contendrá nuestro índice de hash para búsquedas rápidas
    "stats": None,          # Contendrá estadísticas precalculadas del dataset
}

async def load_data_into_memory():
    """
    Función principal que se ejecuta al inicio de la aplicación.

    Orquesta la descarga del archivo Parquet desde ADLS, lo carga en un DataFrame de Polars,
    y crea un índice en memoria (hash map) para acelerar las consultas de filtrado.
    """
    logger.info("Iniciando el proceso de carga de datos en memoria.")
    start_time = time.time()

    try:
        # 1. Obtener el cliente de ADLS
        adls_client = await get_adls_client()

        # 2. Descargar el archivo a un buffer en memoria
        parquet_buffer = await download_parquet_file_to_buffer(adls_client)

        # 3. Cargar el buffer en un DataFrame de Polars
        logger.info("Decodificando el archivo Parquet con Polars.")
        df = pl.read_parquet(parquet_buffer)
        data_store["dataframe"] = df

        logger.info(f"DataFrame cargado. Columnas: {df.columns}, Filas: {df.height}")
        logger.info(f"Uso de memoria del DataFrame: {df.estimated_size('mb'):.2f} MB")

        # 4. Crear el índice de hash para filtrado rápido
        _create_filter_index(df)

        # 5. Pre-calcular estadísticas
        _calculate_stats(df)

        end_time = time.time()
        logger.info(f"Proceso de carga de datos completado exitosamente en {end_time - start_time:.2f} segundos.")

    except Exception as e:
        logger.critical(f"No se pudo cargar los datos en memoria. La aplicación no puede iniciar. Error: {e}", exc_info=True)
        # Es crucial relanzar la excepción para que FastAPI sepa que el inicio falló.
        raise

def _create_filter_index(df: pl.DataFrame):
    """
    Crea un índice de hash (diccionario) para búsquedas ultra-rápidas.

    Args:
        df: El DataFrame de Polars principal.
    """
    filter_column = settings.FILTER_FIELD_NAME
    if filter_column not in df.columns:
        raise ValueError(f"La columna de filtro '{filter_column}' no se encuentra en el archivo Parquet.")

    logger.info(f"Creando índice en memoria para la columna: '{filter_column}'...")

    # Usamos group_by para crear un iterador de (clave, sub-dataframe).
    # La clave que devuelve group_by es una tupla, por lo que usamos key[0] para obtener el valor escalar.
    data_store["filter_index"] = {
        key[0]: group_df for key, group_df in df.group_by(filter_column)
    }

    logger.info(f"Índice creado con {len(data_store['filter_index'])} claves únicas.")

def _calculate_stats(df: pl.DataFrame):
    """
    Calcula y almacena estadísticas básicas sobre el DataFrame.
    """
    logger.info("Calculando estadísticas del dataset...")
    stats = {
        "total_records": df.height,
        "total_columns": df.width,
        "columns": df.columns,
        "memory_usage_mb": df.estimated_size('mb'),
        "schema": {name: str(dtype) for name, dtype in df.schema.items()},
    }
    data_store["stats"] = stats
    logger.info("Estadísticas calculadas y almacenadas.")


# --- Funciones de acceso a los datos ---

def get_dataframe() -> Optional[pl.DataFrame]:
    """Devuelve el DataFrame principal."""
    return data_store["dataframe"]

def get_stats() -> Optional[Dict[str, Any]]:
    """Devuelve las estadísticas precalculadas."""
    return data_store["stats"]

def find_records_by_value(value: Any) -> Optional[pl.DataFrame]:
    """
    Busca registros usando el índice de hash pre-calculado.
    Esta función es la clave para la alta velocidad de respuesta.

    Args:
        value: El valor a buscar en la columna de filtro.

    Returns:
        Un DataFrame de Polars con los registros encontrados, o None si no se encuentra.
    """
    # La búsqueda en el diccionario es, en promedio, O(1)
    return data_store["filter_index"].get(value)
