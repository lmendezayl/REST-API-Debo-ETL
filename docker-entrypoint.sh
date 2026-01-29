#!/usr/bin/env sh
set -e

PORT_ENV="${PORT:-8000}"
APP_MODULE="${APP_MODULE:-src.main:app}"

echo ">> Verificando dependencias..."

# Verificar que uvicorn esté disponible
if ! python -c "import uvicorn" >/dev/null 2>&1; then
    echo "ERROR: uvicorn no está instalado. Instalar con: pip install uvicorn"
    exit 1
fi

# Verificar que el módulo de la app existe
MODULE_PATH=$(echo "${APP_MODULE}" | cut -d':' -f1)
if ! python -c "import ${MODULE_PATH}" >/dev/null 2>&1; then
    echo "ERROR: No se puede importar el módulo ${MODULE_PATH}"
    echo "Estructura de directorios disponible:"
    ls -la /app/
    if [ -d "/app/src" ]; then
        echo "Contenido de /app/src/:"
        ls -la /app/src/
    fi
    exit 1
fi

echo ">> Iniciando FastAPI con uvicorn en ${APP_MODULE} (puerto ${PORT_ENV})"
exec uvicorn "${APP_MODULE}" --host 0.0.0.0 --port "${PORT_ENV}" --log-level info