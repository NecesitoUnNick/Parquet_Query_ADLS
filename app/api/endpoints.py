import time
import logging
from functools import lru_cache
from datetime import datetime, timezone
from typing import List, Any, Dict, Optional

from fastapi import APIRouter, Query, HTTPException, status
from pydantic import BaseModel, Field

from app.services.data_processing import get_stats, find_records_by_value, get_dataframe
from app.core.config import settings
import polars as pl

# Configurar un logger específico para este módulo
logger = logging.getLogger(__name__)

# Crear un router de FastAPI para organizar los endpoints
router = APIRouter()

# --- Modelos de Respuesta (Pydantic) ---
# Usar Pydantic nos asegura que las respuestas de la API siempre tengan una estructura consistente.

class HealthResponse(BaseModel):
    status: str = "ok"
    message: str

class StatsResponse(BaseModel):
    total_records: int
    total_columns: int
    columns: List[str]
    memory_usage_mb: float
    schema: Dict[str, str]

class FilterResponse(BaseModel):
    data: List[Dict[str, Any]]
    total_records: int
    query_time_ms: float
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# --- Implementación de los Endpoints ---

@router.get("/health", response_model=HealthResponse, tags=["Monitoring"])
async def health_check():
    """
    Endpoint de Health Check.

    Verifica si el DataFrame principal ha sido cargado en memoria.
    Esencial para sistemas de monitoreo (como Kubernetes liveness/readiness probes).
    """
    if get_dataframe() is not None:
        return HealthResponse(message="Servicio operativo y datos cargados.")
    else:
        # Si los datos no están cargados, el servicio no está listo para recibir tráfico.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="El servicio no está disponible. Los datos aún no se han cargado.",
        )

@router.get("/stats", response_model=StatsResponse, tags=["Data"])
async def get_dataset_stats():
    """
    Endpoint de Estadísticas.

    Devuelve metadatos pre-calculados sobre el dataset cargado en memoria.
    """
    stats_data = get_stats()
    if stats_data:
        return StatsResponse(**stats_data)
    else:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Las estadísticas no están disponibles. Los datos aún no se han cargado.",
        )

# LRU Cache para la función de búsqueda.
# maxsize=1024 significa que guardará los resultados de las 1024 consultas más recientes.
# Esto es extremadamente útil si los usuarios consultan los mismos valores repetidamente.
@lru_cache(maxsize=1024)
def _execute_filter(value: Any) -> Optional[Dict[str, Any]]:
    """
    Función interna para ejecutar el filtro, decorada con caché.

    Args:
        value: El valor a buscar.

    Returns:
        Un diccionario con los datos encontrados o None.
    """
    logger.debug(f"Ejecutando búsqueda en el índice para el valor: {value}")
    # find_records_by_value hace la búsqueda O(1) en nuestro hash map
    result_df = find_records_by_value(value)

    if result_df is None or result_df.is_empty():
        return None

    # Convertimos el DataFrame de Polars a una lista de diccionarios
    return {
        "data": result_df.to_dicts(),
        "total_records": len(result_df)
    }

@router.get("/data/filter", response_model=FilterResponse, tags=["Data"])
async def filter_data(value: str = Query(..., description="Valor a buscar en el campo de filtro configurado.")):
    """
    Endpoint principal de filtrado de datos.

    Busca registros donde el campo `FILTER_FIELD_NAME` (configurado en el entorno)
    coincide con el `value` proporcionado. La respuesta es casi instantánea
    gracias al índice en memoria y la caché LRU.
    """
    start_time = time.perf_counter()

    # Validar que los datos estén cargados antes de proceder
    if get_dataframe() is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="El servicio no está listo para realizar consultas.",
        )

    # La columna de filtro puede ser numérica. Intentamos convertir el valor.
    # Esta es una heurística simple. Una implementación más robusta podría
    # inspeccionar el tipo de dato de la columna del DataFrame.
    filter_value: Any = value
    try:
        # Primero intenta convertir a entero
        filter_value = int(value)
    except ValueError:
        try:
            # Si falla, intenta convertir a flotante
            filter_value = float(value)
        except ValueError:
            # Si ambos fallan, se queda como string
            pass

    # Llamamos a la función cacheada
    result = _execute_filter(filter_value)

    end_time = time.perf_counter()
    query_time = (end_time - start_time) * 1000  # Convertir a milisegundos

    if not result:
        return FilterResponse(
            data=[],
            total_records=0,
            query_time_ms=query_time
        )

    return FilterResponse(
        data=result["data"],
        total_records=result["total_records"],
        query_time_ms=query_time
    )
