# --- Etapa 1: Build - Instalar dependencias ---
FROM python:3.11-slim as builder

# Establecer el directorio de trabajo
WORKDIR /app

# Actualizar pip
RUN pip install --upgrade pip

# Crear un entorno virtual para aislar las dependencias
RUN python -m venv /opt/venv

# Activar el entorno virtual para los comandos siguientes
ENV PATH="/opt/venv/bin:$PATH"

# Copiar el archivo de requerimientos e instalar las dependencias
# Esto se hace en un paso separado para aprovechar el cache de Docker
COPY requirements.txt .
RUN pip install -r requirements.txt


# --- Etapa 2: Final - Crear la imagen de producción ---
FROM python:3.11-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar el entorno virtual con las dependencias desde la etapa de build
COPY --from=builder /opt/venv /opt/venv

# Copiar el código de la aplicación
COPY ./app ./app

# Activar el entorno virtual para la ejecución
ENV PATH="/opt/venv/bin:$PATH"

# Exponer el puerto en el que la aplicación se ejecutará
# El puerto por defecto es 8000, pero se puede sobrescribir con la variable de entorno APP_PORT
EXPOSE 8000

# Comando para ejecutar la aplicación
# Escucha en 0.0.0.0 para ser accesible desde fuera del contenedor.
# El número de workers y el puerto se pueden configurar a través de variables de entorno.
# Proporcionamos valores predeterminados por si no se especifican.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "${APP_PORT:-8000}", "--workers", "${APP_WORKERS:-1}"]
