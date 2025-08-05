import io
import logging
from azure.storage.filedatalake.aio import DataLakeServiceClient
from azure.identity.aio import DefaultAzureCredential
from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.config import settings

# Configurar un logger específico para este módulo
logger = logging.getLogger(__name__)

async def get_adls_client() -> DataLakeServiceClient:
    """
    Crea y devuelve un cliente asíncrono para Azure Data Lake Storage.

    Intenta autenticarse usando la cadena de conexión si está disponible.
    Si no, recurre a DefaultAzureCredential, que es ideal para entornos de producción
    (e.g., usando identidades administradas en Azure App Service o VMs).

    Returns:
        Un cliente de DataLakeServiceClient listo para usar.

    Raises:
        ValueError: Si no se puede crear un cliente por falta de credenciales.
    """
    account_url = f"https://{settings.AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"

    if settings.AZURE_CONNECTION_STRING:
        logger.info("Autenticando con la cadena de conexión.")
        # from_connection_string no es un método async, pero el cliente que devuelve sí lo es.
        return DataLakeServiceClient.from_connection_string(
            conn_str=settings.AZURE_CONNECTION_STRING
        )
    else:
        logger.info("Autenticando con DefaultAzureCredential.")
        # DefaultAzureCredential necesita ser asíncrona para el cliente asíncrono.
        credential = DefaultAzureCredential()
        return DataLakeServiceClient(account_url=account_url, credential=credential)

@retry(
    stop=stop_after_attempt(3),  # Reintentar hasta 3 veces
    wait=wait_exponential(multiplier=1, min=4, max=10),  # Espera exponencial entre reintentos
    reraise=True # Volver a lanzar la excepción si todos los reintentos fallan
)
async def download_parquet_file_to_buffer(client: DataLakeServiceClient) -> io.BytesIO:
    """
    Descarga el archivo Parquet desde ADLS a un buffer en memoria.

    Utiliza una estrategia de reintentos para manejar errores transitorios de red,
    haciendo la descarga más robusta.

    Args:
        client: El cliente de DataLakeServiceClient.

    Returns:
        Un buffer de BytesIO que contiene los datos del archivo Parquet.

    Raises:
        Exception: Si la descarga falla después de todos los reintentos.
    """
    try:
        logger.info(f"Iniciando la descarga del archivo: {settings.PARQUET_FILE_PATH}")
        file_client = client.get_file_client(
            settings.AZURE_STORAGE_CONTAINER_NAME,
            settings.PARQUET_FILE_PATH
        )

        # Descargar el contenido del archivo
        download = await file_client.download_file()
        file_bytes = await download.readall()

        logger.info("Archivo descargado exitosamente en memoria.")
        return io.BytesIO(file_bytes)
    except Exception as e:
        logger.error(f"Error al descargar el archivo desde ADLS: {e}", exc_info=True)
        # La anotación @retry se encargará de reintentar y, si falla, relanzará la excepción.
        raise
    finally:
        # Es una buena práctica cerrar el cliente si se usó DefaultAzureCredential
        if hasattr(client, 'credential') and isinstance(client.credential, DefaultAzureCredential):
            await client.credential.close()
        await client.close()
