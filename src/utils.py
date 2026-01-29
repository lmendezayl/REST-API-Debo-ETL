from datetime import datetime, timedelta
from pyodbc import Cursor 
import logging
from typing import TypeAlias


log = logging.getLogger(__name__)

Dia: TypeAlias = str  # 'dd/MM/yyyy HH:mm'

def get_date_today(turno: int | None = None) -> tuple[str, str] | list[tuple[str, str]]:
    """
    Devuelve una tupla (fecha_desde, fecha_hasta) en formato 'dd/MM/yyyy HH:mm' si turno es no es None (ventas)
    Devuelve una lista de tuplas (fecha_desde, fecha_hasta) para los últimos 5 días si turno es None (descargas)

    - Si turno es None:
        fecha_desde: 5 dias anteriores a las 00:00
        fecha_hasta: ayer a las 23:59
        (pensado para descargas diarias)

    - Si turno es 1:
        fecha_desde: hoy a las 06:00
        fecha_hasta: hoy a las 14:00
        (turno mañana)

    - Si turno es 2:
        fecha_desde: hoy a las 14:00
        fecha_hasta: hoy a las 22:00
        (turno tarde)

    - Si turno es 3:
        fecha_desde: ayer a las 22:00
        fecha_hasta: hoy a las 06:00
        (turno noche)
    """
    hoy: datetime = datetime.now()
    ayer: datetime = hoy - timedelta(days=1) 
    dias: list[datetime] = [hoy - timedelta(days=i) for i in range(2, 7)][::-1] # [hace 6 días, ..., hace 2 días]
    
    if turno == 1:
        inicio = hoy.replace(hour=6, minute=0, second=0, microsecond=0)
        fin = hoy.replace(hour=14, minute=0, second=0, microsecond=0)
    elif turno == 2:
        inicio = hoy.replace(hour=14, minute=0, second=0, microsecond=0)
        fin = hoy.replace(hour=22, minute=0, second=0, microsecond=0)
    elif turno == 3:
        inicio = ayer.replace(hour=22, minute=0, second=0, microsecond=0)
        fin = hoy.replace(hour=6, minute=0, second=0, microsecond=0)
        
    elif turno is None: # este es para endpoint de descargas 
        lista_dias: list[tuple[Dia, Dia]] = [(dia.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%d/%m/%Y %H:%M"),
                                              dia.replace(hour=23, minute=59, second=59, microsecond=0).strftime("%d/%m/%Y %H:%M"))
                                            for dia in dias]
        return lista_dias
    
    else: 
        raise ValueError("El parámetro 'turno' debe ser 1, 2, 3 o None.")
    
    fecha_desde = inicio.strftime("%d/%m/%Y %H:%M")
    fecha_hasta = fin.strftime("%d/%m/%Y %H:%M")
    return fecha_desde, fecha_hasta


def get_columnas(tabla: str, cursor: Cursor) -> list:
    """Obtiene los nombres de las columnas de una tabla específica."""
    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tabla}'"
    cursor.execute(query)
    columnas = [row[0] for row in cursor.fetchall()]
    return columnas


def parse_fecha_param(fecha_str: str) -> str:
    """
    Convierte formato 'ddMMyyyyHHmm' a 'dd/MM/yyyy HH:mm'.
    Acepta formato:
    - '021020251430' → '02/10/2025 14:30'
    """
    try:
        # Parsear el formato ddMMyyyyHHmm (todo junto, sin separadores)
        dt = datetime.strptime(fecha_str, '%d%m%Y%H%M')
        
        # Convertir a formato dd/MM/yyyy HH:mm (con separadores)
        return dt.strftime("%d/%m/%Y %H:%M")
    except ValueError:
        raise ValueError(f"Formato de fecha inválido: {fecha_str}. Use formato 'ddMMyyyyHHmm'")


def get_fechas_procesadas(
    turno: int | None, 
    fecha_desde: str | None, 
    fecha_hasta: str | None) -> tuple[str, str] | list[tuple[str, str]]:
    """
    Obtiene las fechas procesadas, usando parámetros custom o por defecto según turno.
    """
    if fecha_desde and fecha_hasta:
        # Usar fechas custom parseadas
        return parse_fecha_param(fecha_desde), parse_fecha_param(fecha_hasta)
    else:
        # Usar fechas por defecto según turno
        return get_date_today(turno)
    
    
def get_fechas_procesadas_descargas(
    fecha_desde: str, 
    fecha_hasta: str) -> list[tuple[str, str]]:
    """
    Obtiene las fechas procesadas para descargas, usando parámetros custom o por defecto.
    Devuelve una lista de tuplas (fecha_desde, fecha_hasta).
    """
    desde = datetime.strptime(fecha_desde, "%d%m%Y%H%M")
    hasta = datetime.strptime(fecha_hasta, "%d%m%Y%H%M")
    
    cantidad_dias = (hasta.date() - desde.date()).days + 1  # +1 para incluir el día final
    lista_fechas = []
    
    for i in range(0, cantidad_dias):
        dia = desde + timedelta(days=i)

        fecha_inicio = dia.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%d/%m/%Y %H:%M")
        fecha_fin = dia.replace(hour=23, minute=59, second=0, microsecond=0).strftime("%d/%m/%Y %H:%M")

        lista_fechas.append((fecha_inicio, fecha_fin))
                                          
    return lista_fechas