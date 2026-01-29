import os
import pyodbc
from dotenv import load_dotenv  

load_dotenv()

def get_connection() -> pyodbc.Connection:
    """Retorna una conexión a la base de datos SQL Server. Usa variables de entorno definidas en .env."""
    server = os.environ.get("SQL_SERVER")
    database = os.environ.get("SQL_DATABASE")
    username = os.environ.get("SQL_USER")
    password = os.environ.get("SQL_PASSWORD")
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;TrustServerCertificate=yes;" 
        "ConnectRetryCount=3;"
        "ConnectRetryInterval=10;"
    )
    conn = pyodbc.connect(connection_string)
    return conn



