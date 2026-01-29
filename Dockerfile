# Imagen base ligera con Python 3.11
FROM python:3.11-slim

# Evita bytecode, buffer, y define zona horaria
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=America/Argentina/Buenos_Aires

# Instala dependencias del sistema (incluye ODBC por si usás pyodbc con SQL Server)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates \
    unixodbc unixodbc-dev \
 && rm -rf /var/lib/apt/lists/*

# Crea el directorio de trabajo
WORKDIR /app

# Copia e instala dependencias de Python
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copia TODO el código fuente (no solo src/)
COPY . .

# Dar permisos de ejecución al entrypoint
RUN chmod +x ./docker-entrypoint.sh

# Configurar PYTHONPATH para que Python encuentre los módulos
ENV PYTHONPATH=/app
ENV PORT=8000
ENV APP_MODULE=src.main:app

EXPOSE 8000

# Usar ENTRYPOINT en lugar de CMD para mejor manejo de señales
ENTRYPOINT ["./docker-entrypoint.sh"]