import logging
import os
import dotenv
import time

from fastapi import FastAPI, Query, HTTPException
from pyodbc import Connection
from datetime import datetime, timedelta
from typing import TypeAlias

try:
    # Si está en src/
    from .api_client import api_client
    from .connection import get_connection
    from .procesamiento import procesar_datos
    from .utils import get_fechas_procesadas, get_fechas_procesadas_descargas
except ImportError or ModuleNotFoundError:
    # Si se ejecuta desde raíz
    from api_client import api_client
    from connection import get_connection
    from procesamiento import procesar_datos
    from utils import get_fechas_procesadas, get_fechas_procesadas_descargas


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
dotenv.load_dotenv()
app = FastAPI()

Dia: TypeAlias = str

@app.get("/")
async def root(token: str = Query()):
    token_auth = os.environ.get("TOKEN_AUTH")
    if not token_auth:
        log.error("TOKEN_AUTH no está configurado en variables de entorno")
        raise HTTPException(status_code=500, detail="Error de configuración del servidor")
    log.info("Verificando token de autenticación...")
    if token != token_auth:
        log.warning("Token de autenticación inválido.")
        raise HTTPException(status_code=401, detail="No autorizado. Token inválido.")
    return {"message": "API de procesamiento de datos"}

@app.get("/health")
async def health_check(token: str = Query()):
    token_auth = os.environ.get("TOKEN_AUTH")
    if not token_auth:
        log.error("TOKEN_AUTH no está configurado en variables de entorno")
        raise HTTPException(status_code=500, detail="Error de configuración del servidor")
    log.info("Verificando token de autenticación...")
    if token != token_auth:
        log.warning("Token de autenticación inválido.")
        raise HTTPException(status_code=401, detail="No autorizado. Token inválido.")
    if not token:
        raise HTTPException(status_code=400, detail="Falta el token de autenticación.")
    return {"status": "healthy", "timestamp": datetime.now().isoformat() + "Z"}

@app.post("/jobs/ventas/{idTurno}")
# baseURL/jobs/ventas/{idTurno}?token=xxxx
async def ventas(
    idTurno: int,
    token: str = Query(),
    fecha_desde: str | None = Query(None),
    fecha_hasta: str | None = Query(None),
):
    try:
        # Verificamos que el token de autenticación sea correcto
        token_auth = os.environ.get("TOKEN_AUTH")
        if not token_auth:
            log.error("TOKEN_AUTH no está configurado en variables de entorno")
            raise HTTPException(status_code=500, detail="Error de configuración del servidor")
        
        log.info("Verificando token de autenticación...")
        if token != token_auth:
            log.warning("Token de autenticación inválido.")
            raise HTTPException(status_code=401, detail="No autorizado. Token inválido.")

        log.info("Obteniendo token de autenticación a la API...")
        token_api: str = api_client.get_token()
        log.info("Token de API obtenido con éxito.")
        
        log.info("Conectando a la base de datos...")
        conexion: Connection = get_connection()
        log.info("Conexión a la base de datos exitosa.")
        
        fecha_desde, fecha_hasta = get_fechas_procesadas(idTurno, fecha_desde, fecha_hasta) #type: ignore
        log.info(f"Procesando ventas para turno {idTurno} desde {fecha_desde} hasta {fecha_hasta}")

        procesar_datos(token_api, fecha_desde, fecha_hasta, 'ventas', conexion) #type: ignore
        
        # Cerrar conexión
        conexion.close()
        log.info("Proceso de ventas completado exitosamente")
        
        return {"status": "ok", "message": "proceso completado"}
    except HTTPException as http_exc:
        log.error(f"Error HTTP: {http_exc.detail}")
        raise
    except Exception as e:
        log.error(f"Error inesperado: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@app.post("/jobs/descargas")
async def descargas(
    token: str = Query(),
    fecha_desde: str | None = Query(None),
    fecha_hasta: str | None = Query(None)
):

    try: 
        # Verificamos que el token de autenticación sea correcto
        token_auth = os.environ.get("TOKEN_AUTH")
        if not token_auth:
            log.error("TOKEN_AUTH no está configurado en variables de entorno")
            raise HTTPException(status_code=500, detail="Error de configuración del servidor")
        
        log.info("Verificando token de autenticación...")
        if token != token_auth:
            log.warning("Token de autenticación inválido.")
            raise HTTPException(status_code=401, detail="No autorizado. Token inválido.")

        log.info("Obteniendo token de autenticación a la API...")
        token_api: str = api_client.get_token()
        log.info("Token de API obtenido con éxito.")
        
        log.info("Conectando a la base de datos...")
        conexion: Connection = get_connection()
        log.info("Conexión a la base de datos exitosa.")
        
        if fecha_desde and fecha_hasta:
            # Lista de todos los dias pasados por parámetro de 00:00 a 23:59
            lista_fechas = get_fechas_procesadas_descargas(fecha_desde, fecha_hasta)
            log.info(f"Fechas procesadas: {lista_fechas}")
        else: 
            # Obtener lista de tuplas, cada una con (fecha_desde, fecha_hasta) para los últimos 5 días
            lista_fechas = get_fechas_procesadas(None, fecha_desde, fecha_hasta)
            
        if not lista_fechas:
            log.warning("No hay fechas para procesar")
            return {"status": "ok", "message": "No hay fechas para procesar"}
        
        log.info(f"Procesando descargas desde {lista_fechas[0][0]} hasta {lista_fechas[-1][1]}")
        # Iteramos sobre cada par desde hasta
        for fecha_desde, fecha_hasta in lista_fechas: 
            procesar_datos(token_api, fecha_desde, fecha_hasta, 'descargas', conexion)
            time.sleep(2)
        conexion.close()
        log.info("Proceso de descargas completado exitosamente")
        return {"status": "ok", "message": "proceso completado"}
    
    except HTTPException:
        raise  # Re-lanzar HTTPExceptions
    except Exception as e:
        log.error(f"Error procesando descargas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")