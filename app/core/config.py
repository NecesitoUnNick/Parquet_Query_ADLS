from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional

class Settings(BaseSettings):
    """
    Configuraciones de la aplicación, cargadas desde variables de entorno.

    Utiliza Pydantic para validar los tipos de datos y asegurar que todas las
    configuraciones necesarias estén presentes al iniciar la aplicación.
    """
    # --- Configuración de Azure Data Lake Storage (ADLS) ---
    AZURE_STORAGE_ACCOUNT_NAME: str = Field(
        ...,
        description="Nombre de la cuenta de almacenamiento de Azure (ADLS Gen2)."
    )
    AZURE_STORAGE_CONTAINER_NAME: str = Field(
        ...,
        description="Nombre del contenedor dentro de la cuenta de almacenamiento."
    )
    PARQUET_FILE_PATH: str = Field(
        ...,
        description="Ruta completa al archivo Parquet dentro del contenedor."
    )
    AZURE_CONNECTION_STRING: Optional[str] = Field(
        None,
        description="Cadena de conexión de Azure. Opcional, pero necesaria si no se usa DefaultAzureCredential."
    )

    # --- Configuración de la aplicación ---
    FILTER_FIELD_NAME: str = Field(
        ...,
        description="El nombre de la columna en el archivo Parquet que se usará para filtrar y crear un índice en memoria."
    )
    LOG_LEVEL: str = Field(
        "INFO",
        description="Nivel de logging para la aplicación (e.g., DEBUG, INFO, WARNING, ERROR)."
    )
    APP_PORT: int = Field(
        8000,
        description="Puerto en el que se ejecutará el servidor Uvicorn."
    )
    APP_WORKERS: int = Field(
        1,
        description="Número de workers para Uvicorn."
    )


    class Config:
        # El nombre del archivo .env a leer.
        env_file = ".env"
        # Permite la lectura de variables de entorno sin distinción de mayúsculas/minúsculas.
        case_sensitive = False

# Se crea una instancia única de la configuración para ser importada en otros módulos.
# Esto sigue el patrón singleton, asegurando que la configuración se carga una sola vez.
settings = Settings()
