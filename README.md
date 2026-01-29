# REST API Debo 

Backend desarrollado con **FastAPI** para la sincronización y procesamiento de datos de ventas y descargas (compras) entre una API externa y una base de datos SQL Server local.

## 🚀 Tecnologías

- **Python 3.10+**
- **FastAPI**: Framework web moderno y rápido.
- **Docker**: Containerización de la aplicación.
- **MSSQL (ODBC Driver 18)**: Conexión a base de datos SQL Server.
- **PyODBC**: Driver para conexión DB.

## 📋 Características Principales

- **Sincronización de Ventas**: Endpoint para procesar ventas por turno (`/jobs/ventas`).
- **Sincronización de Descargas**: Endpoint para procesar remitos de combustible (`/jobs/descargas`).
- **Autenticación**: Protección de endpoints mediante `TOKEN_AUTH`.
- **Health Checks**: Monitoreo de estado del servicio (`/health`).

## ⚙️ Configuración (Variables de Entorno)

Crea un archivo `.env` en la raíz (o configura las variables en tu entorno de despliegue/Docker):

```env
TOKEN_AUTH=tu_token_secreto
BASE_URL=https://api.externa.com/v1
CLIENT_ID=tu_client_id
SQL_SERVER=ip_o_host_sql_server
SQL_DATABASE=nombre_base_datos
SQL_USER=usuario_sql
SQL_PASSWORD=password_sql
PORT=8000
```

## 🛠️ Instalación y Ejecución

### Ejecución Local

1.  **Instalar dependencias**:
    ```bash
    pip install -r requirements.txt
    ```
2.  **Instalar Drivers ODBC 18**:
    Asegúrate de tener instalado el *ODBC Driver 18 for SQL Server*.

3.  **Ejecutar servidor**:
    ```bash
    uvicorn src.main:app --reload
    ```

### Ejecución con Docker

1.  **Construir imagen**:
    ```bash
    docker build -t backend-grupo-penna .
    ```
2.  **Correr contenedor**:
    ```bash
    docker run -d -p 8092:8000 --env-file .env backend-grupo-penna
    ```

## 🔌 Endpoints

| Método | Endpoint | Descripción |
| :--- | :--- | :--- |
| `GET` | `/` | Verificar estado básico. Requiere `?token=`. |
| `GET` | `/health` | Healthcheck completo. Requiere `?token=`. |
| `POST` | `/jobs/ventas/{idTurno}` | Procesa ventas de un turno. Params: `fecha_desde`, `fecha_hasta`. |
| `POST` | `/jobs/descargas` | Procesa descargas de combustible. Params: `fecha_desde`, `fecha_hasta`. |

Todas las llamadas requieren el parámetro `token` igual al `TOKEN_AUTH` configurado.
