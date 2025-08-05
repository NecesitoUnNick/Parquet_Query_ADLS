import logging
import logging.config
import time
import sys

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.endpoints import router as api_router
from app.core.config import settings
from app.services.data_processing import load_data_into_memory

# --- Configuración de Logging Estructurado ---
# Usamos un formato JSON para que los logs sean fácilmente procesables por sistemas de monitoreo.
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
        },
    },
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "formatter": "json",
            "stream": sys.stdout,
        },
    },
    "root": {
        "handlers": ["default"],
        "level": settings.LOG_LEVEL.upper(),
    },
}

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

# --- Creación de la Aplicación FastAPI ---
app = FastAPI(
    title="FastAPI High-Speed Parquet Query Service",
    description="Un microservicio para consultar datos de un archivo Parquet en ADLS a alta velocidad.",
    version="1.0.0",
)

# --- Middleware de Rendimiento y Logging ---
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """
    Middleware para medir el tiempo de procesamiento de cada solicitud
    y para loggear información de la solicitud y respuesta.
    """
    start_time = time.perf_counter()

    # Procesar la solicitud
    response = await call_next(request)

    process_time_ms = (time.perf_counter() - start_time) * 1000
    response.headers["X-Process-Time-Ms"] = f"{process_time_ms:.2f}"

    logger.info(
        "Request handled",
        extra={
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "process_time_ms": f"{process_time_ms:.2f}",
        },
    )
    return response

# --- Eventos de Ciclo de Vida de la Aplicación ---
@app.on_event("startup")
async def startup_event():
    """
    Evento que se ejecuta al iniciar la aplicación.
    Llama a la función para cargar los datos en memoria.
    """
    logger.info("La aplicación está iniciando...")
    try:
        await load_data_into_memory()
    except Exception as e:
        # Si la carga de datos falla, loggeamos el error crítico y detenemos el proceso.
        # Esto evita que la aplicación se ejecute en un estado inválido.
        logger.critical(f"Error fatal durante el inicio: {e}", exc_info=True)
        # Salir del proceso para que el orquestador de contenedores (e.g., Kubernetes)
        # sepa que el pod falló al iniciar.
        sys.exit(1)

# --- Registrar Rutas de la API ---
# Incluimos el router que contiene todos nuestros endpoints.
app.include_router(api_router, prefix="/api")

# --- Endpoint Raíz ---
@app.get("/", tags=["Root"])
async def read_root():
    """
    Endpoint raíz que redirige a la documentación de la API.
    """
    return {"message": "Bienvenido al servicio de consulta de datos. Visite /docs para la documentación de la API."}
