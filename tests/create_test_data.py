import polars as pl
import os

def create_test_parquet_file():
    """
    Crea un archivo Parquet de prueba para usar en los tests.
    """
    # Datos de ejemplo
    data = {
        "client_id": [101, 102, 103, 101, 104],
        "product_name": ["Laptop", "Mouse", "Keyboard", "Monitor", "Webcam"],
        "amount": [1200.50, 25.00, 75.75, 300.00, 90.25],
        "country_code": ["US", "CA", "MX", "US", "US"]
    }
    df = pl.DataFrame(data)

    # Asegurarse de que el directorio de tests existe
    test_dir = os.path.dirname(__file__)

    # Ruta del archivo de salida
    file_path = os.path.join(test_dir, "test_data.parquet")

    # Escribir el archivo Parquet
    df.write_parquet(file_path)
    print(f"Archivo de prueba creado en: {file_path}")

if __name__ == "__main__":
    create_test_parquet_file()
