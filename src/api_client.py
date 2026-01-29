import json
import os
import requests
from requests import Response
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.environ.get("BASE_URL", " ")

class api_client:
    """Módulo para interactuar con la API externa de DEBO."""

    @staticmethod
    def check_url() -> None:
        """Verifica si la URL base es correcta o errónea."""
        if BASE_URL is None:
            raise ValueError(
                "No se encuentra BASE_URL en archivo .env, revisar si el archivo está en el proyecto o si la variable está bien definida"
            )
        pass

    @staticmethod
    def get_token() -> str:
        """Obtiene el token de autenticación desde la API"""
        CLIENT_ID: str | None = os.environ.get("CLIENT_ID")
        api_client.check_url()
        url: str = BASE_URL + "token"
        payload = json.dumps({"client_id": CLIENT_ID})
        headers = {"Content-Type": "application/json"}
        response: Response = requests.request("GET", url, headers=headers, data=payload)
        token: str = response.json().get("token")
        return token

    @staticmethod
    def test_connection(token: str | None) -> int:
        """Prueba la conexión a la API"""
        api_client.check_url()
        url: str = BASE_URL + "test"

        payload = {}
        headers = {"token": token}

        response: Response = requests.request(
            "GET", url, headers=headers, data=payload, allow_redirects=False
        )
        return response.status_code

    @staticmethod
    def get_sectores(token: str):
        """Obtiene los sectores desde la API"""
        url: str = BASE_URL + "sectores"
        payload = {}
        headers = {"token": token}

        sectores: Response = requests.request(
            "GET", url, headers=headers, data=payload, allow_redirects=False
        )

        return sectores.json()

    @staticmethod
    def get_ventas(token: str, fechaDesde: str, fechaHasta: str, lugar: int = -1):
        """
        Obtiene las ventas por fecha desde la API

        Parámetros:

            fechaDesde (str) en formato "dd/MM/yyyy HH:mm"
            fechaHasta (str) en formato "dd/MM/yyyy HH:mm"
        """
        payload = json.dumps(
            {"fechaDesde": fechaDesde, "fechaHasta": fechaHasta, "lugar": lugar}
        )
        headers = {"token": token, "Content-Type": "application/json"}

        ventas: Response = requests.request(
            "GET", BASE_URL + "ventas-fechas", headers=headers, data=payload
        )

        return ventas.json()

    @staticmethod
    def get_compras(token: str, fechaDesde: str, fechaHasta: str):
        """Obtiene las compras por fecha desde la API"""
        url = BASE_URL + "compras-fechas"

        payload = json.dumps(
            {
                "fechaDesde": fechaDesde,
                "fechaHasta": fechaHasta,
            }
        )
        headers = {"token": token, "Content-Type": "application/json"}

        compras: Response = requests.request(
            "GET", url, headers=headers, data=payload, allow_redirects=False
        )

        return compras.json()

    @staticmethod
    def get_articulos(
        token: str,
        id: int = -1,
        sector: int = -1,
        rubro: int = -1,
        rubroMayor: int = -1,
    ):
        """Obtiene los articulos desde la API"""
        url = BASE_URL + "articulos"

        payload = {"id": id, "sector": sector, "rubro": rubro, "rubroMayor": rubroMayor}
        headers = {"token": token}

        articulos: Response = requests.request(
            "GET", url, headers=headers, data=payload, allow_redirects=False
        )

        return articulos.json()
