# Servicio de Consultas de Alta Velocidad para Parquet en ADLS

## Descripción

Este proyecto es un microservicio de alto rendimiento construido con **FastAPI** que sirve para consultar datos de un archivo Parquet alojado en **Azure Data Lake Storage (ADLS) Gen2**.

La principal característica de esta aplicación es su **velocidad de respuesta casi instantánea**. Al iniciar, el servicio carga el archivo Parquet completo en memoria utilizando la potente librería **Polars**. Luego, crea un índice (hash map) sobre una columna específica, permitiendo que las consultas de filtrado se resuelvan en tiempo constante (O(1)).

Este enfoque es ideal para escenarios donde se necesita acceso de lectura ultra-rápido a un dataset de tamaño mediano que no cambia con frecuencia.

## Características Principales

- **Backend Asíncrono con FastAPI**: Ofrece una base robusta y de alto rendimiento para la API.
- **Procesamiento de Datos con Polars**: Utiliza Polars para la manipulación de datos en memoria, una de las librerías más rápidas disponibles en el ecosistema de Python.
- **Índice en Memoria para Consultas O(1)**: Crea un índice de hash al iniciar para que las búsquedas por valor sean extremadamente rápidas.
- **Cache de Consultas (LRU)**: Almacena en caché los resultados de las consultas más frecuentes para acelerar aún más las respuestas a peticiones repetidas.
- **Integración con Azure Data Lake Storage**: Descarga de forma segura y robusta el archivo Parquet desde ADLS Gen2.
- **Autenticación Flexible en Azure**: Soporta autenticación mediante cadena de conexión (para desarrollo) o `DefaultAzureCredential` (para producción, compatible con Identidades Administradas).
- **Logging Estructurado**: Emite logs en formato JSON, facilitando la integración con sistemas de monitoreo y análisis de logs como ELK Stack o Datadog.
- **Contenerización con Docker**: Incluye un `Dockerfile` multi-etapa optimizado para una compilación eficiente y una imagen de producción ligera.
- **Código Asíncrono de Extremo a Extremo**: Desde la descarga del archivo hasta la respuesta de la API, todo el flujo es asíncrono para maximizar el rendimiento.

## Arquitectura

El flujo de trabajo de la aplicación es el siguiente:

1.  **Inicio del Servicio**: Al ejecutar la aplicación, esta se conecta a Azure Data Lake Storage.
2.  **Descarga de Datos**: Descarga el archivo Parquet especificado en la configuración a un buffer en memoria. Utiliza una estrategia de reintentos para manejar fallos de red transitorios.
3.  **Carga en Polars**: El buffer se carga en un DataFrame de Polars.
4.  **Creación de Índice**: Se crea un diccionario (hash map) donde las claves son los valores únicos de la columna de filtro (`FILTER_FIELD_NAME`) y los valores son sub-DataFrames que contienen todas las filas para esa clave.
5.  **Pre-cálculo de Estadísticas**: Se calculan y almacenan metadatos básicos del dataset (número de filas, columnas, uso de memoria, etc.).
6.  **Servicio Listo**: Una vez que los datos están en memoria y el índice está creado, la aplicación está lista para recibir peticiones a través de sus endpoints. Las consultas de filtrado simplemente acceden al diccionario, lo que resulta en una operación muy rápida.

## Instalación y Ejecución

### Prerrequisitos

-   Docker y Docker Compose
-   (Opcional, para desarrollo local) Python 3.11 o superior y `pip`.

### Configuración

La aplicación se configura mediante variables de entorno. Puedes crear un archivo `.env` en la raíz del proyecto para gestionarlas.

```
# .env.example

# --- Configuración de Azure Data Lake Storage (ADLS) ---
# Obligatorio: Nombre de tu cuenta de almacenamiento ADLS Gen2
AZURE_STORAGE_ACCOUNT_NAME="<tu-storage-account>"
# Obligatorio: Nombre del contenedor donde se encuentra el archivo
AZURE_STORAGE_CONTAINER_NAME="<tu-container>"
# Obligatorio: Ruta completa al archivo Parquet dentro del contenedor
PARQUET_FILE_PATH="data/mi_archivo.parquet"
# Opcional: Necesario para desarrollo local si no usas 'az login'
# AZURE_CONNECTION_STRING="<tu-connection-string>"

# --- Configuración de la aplicación ---
# Obligatorio: La columna del Parquet que se usará para el índice y los filtros
FILTER_FIELD_NAME="id_usuario"
# Opcional: Nivel de logging (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL="INFO"
# Opcional: Puerto en el que se ejecutará el servidor
APP_PORT=8000
# Opcional: Número de workers para Uvicorn (en producción se recomienda > 1)
APP_WORKERS=1
```

### Ejecución con Docker (Recomendado)

1.  **Crea el archivo `.env`** a partir del ejemplo anterior con tus valores.
2.  **Construye la imagen de Docker:**
    ```bash
    docker build -t parquet-query-service .
    ```
3.  **Ejecuta el contenedor:**
    ```bash
    docker run --rm -p 8000:8000 --env-file .env parquet-query-service
    ```
    El servicio estará disponible en `http://localhost:8000`.

### Ejecución Local (Para Desarrollo)

1.  **Crea y activa un entorno virtual:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # En Windows: venv\Scripts\activate
    ```
2.  **Instala las dependencias:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Configura las variables de entorno:**
    -   Crea un archivo `.env` como se describe arriba.
    -   O bien, autentícate con la CLI de Azure (`az login`), que será detectado por `DefaultAzureCredential`.
4.  **Inicia la aplicación:**
    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
    ```
    La opción `--reload` es útil para desarrollo, ya que reinicia el servidor automáticamente cuando detecta cambios en el código.

## Endpoints de la API

La API está disponible en el prefijo `/api`.

### Health Check

-   **Endpoint**: `GET /api/health`
-   **Descripción**: Verifica si el servicio está operativo y si los datos han sido cargados en memoria. Ideal para *liveness/readiness probes* en orquestadores como Kubernetes.
-   **Respuesta Exitosa (200 OK)**:
    ```json
    {
      "status": "ok",
      "message": "Servicio operativo y datos cargados."
    }
    ```
-   **Ejemplo con cURL**:
    ```bash
    curl -X GET http://localhost:8000/api/health
    ```

### Estadísticas del Dataset

-   **Endpoint**: `GET /api/stats`
-   **Descripción**: Devuelve metadatos pre-calculados sobre el dataset cargado.
-   **Respuesta Exitosa (200 OK)**:
    ```json
    {
      "total_records": 1000000,
      "total_columns": 5,
      "columns": ["id_usuario", "nombre", "evento", "timestamp", "valor"],
      "memory_usage_mb": 125.7,
      "schema": {
        "id_usuario": "Int64",
        "nombre": "String",
        "evento": "String",
        "timestamp": "Datetime(time_unit='us', time_zone=None)",
        "valor": "Float64"
      }
    }
    ```
-   **Ejemplo con cURL**:
    ```bash
    curl -X GET http://localhost:8000/api/stats
    ```

### Filtrado de Datos

-   **Endpoint**: `GET /api/data/filter`
-   **Descripción**: El endpoint principal. Busca y devuelve todos los registros donde la columna `FILTER_FIELD_NAME` coincide con el `value` proporcionado. La búsqueda es casi instantánea gracias al índice en memoria y la caché LRU.
-   **Parámetros**:
    -   `value` (query string, **obligatorio**): El valor a buscar en la columna de filtro.
-   **Respuesta Exitosa (200 OK)**:
    ```json
    {
      "data": [
        {
          "id_usuario": 12345,
          "nombre": "Alice",
          "evento": "login",
          "timestamp": "2023-10-27T10:00:00Z",
          "valor": 99.9
        }
      ],
      "total_records": 1,
      "query_time_ms": 0.52,
      "timestamp": "2023-10-27T12:34:56.789Z"
    }
    ```
-   **Ejemplo con cURL**:
    ```bash
    curl -X GET "http://localhost:8000/api/data/filter?value=12345"
    ```

## Testing

Para ejecutar la suite de tests, asegúrate de haber instalado las dependencias de desarrollo y luego ejecuta `pytest`:

```bash
pytest
```
