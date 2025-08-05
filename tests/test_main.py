import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock
import os

# --- Configuración Inicial ---
# Establecer una variable de entorno ANTES de que se importen los módulos de la app.
# Esto asegura que 'settings' use nuestro archivo de configuración de prueba.
os.environ['FILTER_FIELD_NAME'] = 'country_code'
os.environ['AZURE_STORAGE_ACCOUNT_NAME'] = 'testaccount'
os.environ['AZURE_STORAGE_CONTAINER_NAME'] = 'testcontainer'
os.environ['PARQUET_FILE_PATH'] = 'test/path.parquet'

# Ahora importamos la app y otros componentes
from app.main import app
from app.services.data_processing import data_store, _create_filter_index, _calculate_stats
from app.api.endpoints import _execute_filter
import polars as pl

# --- Fixture de Pytest para Configuración de Pruebas ---
@pytest.fixture(scope="function")
def test_app(monkeypatch):
    """
    Fixture que prepara la aplicación para una prueba.
    - Mockea la carga de datos desde ADLS.
    - Carga datos de prueba locales en su lugar.
    - Proporciona un TestClient para hacer solicitudes.
    """
    # 1. Mockear la función de carga de datos para que no intente contactar a Azure.
    # Usamos AsyncMock porque la función original es asíncrona.
    mock_load = AsyncMock()
    monkeypatch.setattr("app.main.load_data_into_memory", mock_load)

    # 2. Cargar los datos de prueba desde el archivo Parquet local.
    # Esto simula el estado de la aplicación DESPUÉS de un inicio exitoso.
    test_data_path = os.path.join(os.path.dirname(__file__), "test_data.parquet")
    df = pl.read_parquet(test_data_path)

    # 3. Poblar manualmente el data_store, tal como lo haría la aplicación real.
    data_store["dataframe"] = df
    _create_filter_index(df) # Crear el índice de filtro con los datos de prueba
    _calculate_stats(df)     # Calcular estadísticas con los datos de prueba

    # 4. Limpiar la caché antes de cada prueba para asegurar el aislamiento.
    _execute_filter.cache_clear()

    # 5. Crear y devolver el cliente de prueba.
    with TestClient(app) as client:
        yield client

    # 5. Limpieza (se ejecuta después de que la prueba termina).
    # Reseteamos el data_store para asegurar que las pruebas estén aisladas.
    data_store["dataframe"] = None
    data_store["filter_index"] = None
    data_store["stats"] = None


# --- Casos de Prueba ---

def test_root_endpoint(test_app):
    """Prueba que el endpoint raíz '/' funciona."""
    response = test_app.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Bienvenido al servicio de consulta de datos. Visite /docs para la documentación de la API."}

def test_health_check_success(test_app):
    """Prueba el endpoint de health cuando los datos están cargados."""
    response = test_app.get("/api/health")
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["status"] == "ok"
    assert "Servicio operativo" in json_response["message"]

def test_health_check_fail(test_app):
    """Prueba el endpoint de health cuando los datos NO están cargados."""
    # Simular fallo de carga de datos vaciando el dataframe
    data_store["dataframe"] = None

    response = test_app.get("/api/health")
    assert response.status_code == 503
    assert "aún no se han cargado" in response.json()["detail"]

def test_stats_endpoint(test_app):
    """Prueba el endpoint de estadísticas."""
    response = test_app.get("/api/stats")
    assert response.status_code == 200
    stats = response.json()
    assert stats["total_records"] == 5
    assert stats["total_columns"] == 4
    assert stats["columns"] == ["client_id", "product_name", "amount", "country_code"]

def test_filter_endpoint_success(test_app):
    """Prueba el endpoint de filtro con un valor que existe."""
    # El FILTER_FIELD_NAME se estableció en 'country_code'
    response = test_app.get("/api/data/filter?value=US")
    assert response.status_code == 200
    json_response = response.json()

    assert json_response["total_records"] == 3
    assert len(json_response["data"]) == 3
    # Verificar que todos los resultados tienen el country_code correcto
    for record in json_response["data"]:
        assert record["country_code"] == "US"
    assert "query_time_ms" in json_response

def test_filter_endpoint_not_found(test_app):
    """Prueba el endpoint de filtro con un valor que NO existe."""
    response = test_app.get("/api/data/filter?value=JP")
    assert response.status_code == 200
    json_response = response.json()

    assert json_response["total_records"] == 0
    assert len(json_response["data"]) == 0

def test_filter_endpoint_numeric_value(test_app, monkeypatch):
    """Prueba el endpoint de filtro con un campo numérico."""
    # 1. Parcheamos directamente el objeto de settings.
    # Monkeypatch se encargará de revertir el cambio automáticamente después del test.
    monkeypatch.setattr("app.core.config.settings.FILTER_FIELD_NAME", "client_id")

    # 2. Volvemos a crear el índice con la nueva columna de filtro.
    # Es importante hacerlo DESPUÉS de parchear la configuración.
    df = data_store["dataframe"]
    _create_filter_index(df)

    # 3. Realizamos la prueba.
    # El endpoint debe poder manejar un valor numérico como string.
    response = test_app.get("/api/data/filter?value=101")
    assert response.status_code == 200
    json_response = response.json()

    assert json_response["total_records"] == 2
    assert json_response["data"][0]["client_id"] == 101
    assert json_response["data"][1]["client_id"] == 101
