from pyodbc import Connection, Cursor
import logging

try:
    # Si está en src/
    from api_client import api_client
    from insertar_datos import insertar
except ImportError or ModuleNotFoundError:
    # Si se ejecuta desde raíz
    from src.api_client import api_client
    from src.insertar_datos import insertar


log = logging.getLogger(__name__)

def procesar_datos(token: str, fecha_desde: str, fecha_hasta: str, tabla: str, conexion: Connection):
    """Procesa los datos obtenidos de la API y los inserta en la base de datos."""
    cursor: Cursor = conexion.cursor()

    match tabla:
        
        case 'ventas':
            log.info("Procesando datos para las tablas de ventas...")
            log.info("Obteniendo datos de ventas desde la API...")
            ventas = api_client.get_ventas(token, fecha_desde, fecha_hasta)
            log.info(f"Datos obtenidos. {len(ventas)} ventas a procesar...")
            
            # Preparar todas las ventas formateadas
            ventas_formateadas = []
            for venta in ventas:
                data_ventas, data_ventas_cuerpo, data_ventas_formaspago_detalle = formatear_json_venta(venta)
                ventas_formateadas.append({
                    'data': data_ventas,
                    'data_cuerpo': data_ventas_cuerpo,
                    'data_formaspago': data_ventas_formaspago_detalle
                })
            
            # Procesar en lotes para mejor rendimiento y manejo de memoria
            TAMANIO_LOTE = 500
            total_ventas = len(ventas_formateadas)

            for i in range(0, total_ventas, TAMANIO_LOTE): # para cada lote
                lote = ventas_formateadas[i:i+TAMANIO_LOTE] # obtener la sublista del lote
                log.info(f"Procesando lote {i//TAMANIO_LOTE + 1} ({i+1}-{min(i+TAMANIO_LOTE, total_ventas)} de {total_ventas} ventas)...") 
                
                try:
                    insertar.ventas_bulk(lote, cursor)
                    conexion.commit()
                    log.info(f"Lote {i//TAMANIO_LOTE + 1} insertado exitosamente")
                except Exception as e:
                    log.error(f"Error procesando lote {i//TAMANIO_LOTE + 1}: {e}")
                    conexion.rollback()
                    raise

            log.info("Datos de ventas procesados e insertados en las tablas de ventas con éxito")

        case 'descargas':
            log.info(f"Procesando datos para la tabla de descargas en {fecha_desde} a {fecha_hasta}")
            log.info("Obteniendo datos de descargas desde la API...")
            descargas = api_client.get_compras(token, fecha_desde, fecha_hasta)
            log.info("Datos obtenidos. Cargando datos a base de datos...")
            
            for remito in descargas:
                data_descargas_comb: list = formatear_json_descargas(remito)
                if data_descargas_comb: # si no es remito de combustible, no hacemos nada
                    for item in data_descargas_comb: # por cada dict cuerpo de combustible
                        insertar.descargas(item, cursor)
            
            conexion.commit()
            log.info("Datos insertados en la tabla de descargas con éxito.")

        case _:
            raise ValueError(
                f"Tabla {tabla} no reconocida para procesamiento.")

    cursor.close()


def formatear_json_venta(venta):
    '''
    Formatea y prepara el JSON de una venta en tres diccionarios distintos, uno por tabla.
    Retorna `data_ventas`, `data_ventas_cuerpo`, `data_ventas_formaspago_detalle`
    '''
    # Complejidad: O(1) implementando dict con tabla de hash
    # Construimos un diccionario por cada tabla a llenar
    
    data_ventas = {
        "letra": venta.get("letra"),
        "tipo": venta.get("tipo"),
        "sucursal": venta.get("sucursal"),
        "numero": venta.get("numero"),
        "fechaHora": venta.get("fechaHora"),
        "clienteId": venta.get("cliente", {}).get("id"),
        "clienteRazonSocial": venta.get("cliente", {}).get("razon_social"),
        "clienteCuit": venta.get("cliente", {}).get("cuit"),
        "clienteBloqueado": venta.get("cliente", {}).get("bloqueado"),
        "clienteHabilitado": venta.get("cliente", {}).get("habilitado"),
        "clienteIdDeboCloud": venta.get("cliente", {}).get("id_debo_cloud"),
        "razonSocialClienteHistorico": venta.get("razonSocialClienteHistorico"),
        "CUITHistorico": venta.get("CUITHistorico"),
        "responsabilidadId": venta.get("responsabilidad", {}).get("id"),
        "responsabilidadDescripcion": venta.get("responsabilidad", {}).get("descripcion"),
        "responsabilidadAbreviatura": venta.get("responsabilidad", {}).get("abreviatura"),
        "formaDePagoId": venta.get("formaDePago", {}).get("id"),
        "formaDePagoDescripcion": venta.get("formaDePago", {}).get("descripcion"),
        "importeDescuento": venta.get("importeDescuento"),
        "importeNetoGravado": venta.get("importeNetoGravado"),
        "importeNetoExento": venta.get("importeNetoExento"),
        "importeIVA": venta.get("importeIVA"),
        "importeIVAServicios": venta.get("importeIVAServicios"),
        "importeImpuestoInterno1": venta.get("importeImpuestoInterno1"),
        "importeImpuestoInterno2": venta.get("importeImpuestoInterno2"),
        "importeImpuestoInterno3": venta.get("importeImpuestoInterno3"),
        "importePercepcion": venta.get("importePercepcion"),
        "importeRedondeo": venta.get("importeRedondeo"),
        "importePercepcionIVA": venta.get("importePercepcionIVA"),
        "importeTotal": venta.get("importeTotal"),
        "idTurno": venta.get("idTurno"),
        "idEmpresa": venta.get("idEmpresa"),
        "fechaVencimiento": venta.get("fechaVencimiento"),
        "vendedorId": venta.get("Vendedor", {}).get("Id"),
        "vendedorNombre": venta.get("Vendedor", {}).get("nombre"),
        "vendedorEsEncargado": venta.get("Vendedor", {}).get("esEncargado"),
        "vendedorDni": venta.get("Vendedor", {}).get("dni"),
        "estado": venta.get("estado"),
        "numeroPlanilla": venta.get("numeroPlanilla"),
        "lugarDeVentaId": venta.get("lugarDeVenta", {}).get("Id"),
        "lugarDeVentaDescripcion": venta.get("lugarDeVenta", {}).get("descripcion"),
        "lugarDeVentaEsFranquiciado": venta.get("lugarDeVenta", {}).get("esFranquiciado"),
        "lugarDeVentaHabilitadoMultiplesFormasPago": venta.get("lugarDeVenta", {}).get("HabilitadoMultiplesFormasdePago"),
        "preparado": venta.get("preparado"),
        "cuentaMadreId": venta.get("CuentaMadre", {}).get("Id"),
        "cuentaMadreDescripcion": venta.get("CuentaMadre", {}).get("descripcion"),
        "cuentaMadreIdDeboCloud": venta.get("CuentaMadre", {}).get("id_debo_cloud"),
        "esFacturaRemito": venta.get("esFacturaRemito"),
        "numeroCAI": venta.get("numeroCAI"),
        "fechaVencimientoCAI": venta.get("fechaVencimientoCAI"),
        "numeroNotaLiquidoProducto": venta.get("numeroNotaLiquidoProducto"),
        "multiplicadorSucursalComprobanteRepetido": venta.get("multiplicadorSucursalComprobanteRepetido"),
        "notaDePedido": venta.get("notaDePedido"),
        "numeroCAE": venta.get("numeroCAE"),
        "fechaVencimientoCAE": venta.get("fechaVencimientoCAE"),
        "tipoCalculoPercepcion": venta.get("tipoCalculoPercepcion"),
        "idLoteVenta": venta.get("idLoteVenta"),
        "tipoOperacionId": venta.get("tipoOperacion", {}).get("id"),
        "tipoOperacionDescripcion": venta.get("tipoOperacion", {}).get("descripcion"),
        "monedaId": venta.get("Moneda", {}).get("Id"),
        "monedaDescripcion": venta.get("Moneda", {}).get("descripcion"),
        "monedaSimbolo": venta.get("Moneda", {}).get("simbolo"),
        "monedaCodigoAFIP": venta.get("Moneda", {}).get("codigoAFIP"),
        "cotizacionHistorica": venta.get("cotizacionHistorica"),
        "observacionFacturaElectronica": venta.get("observacionFacturaElectronica"),
        "esFacturaElectronicaCAEA": venta.get("esFacturaElectronicaCAEA"),
        "CAEAInformado": venta.get("CAEAInformado"),
        "numeroComanda": venta.get("numeroComanda"),
        "esFacturaComplemento": venta.get("esFacturaComplemento"),
        "conductor": venta.get("datosConductorVehiculo", {}).get("conductor"),
        "marcaVehiculo": venta.get("datosConductorVehiculo", {}).get("marcaVehiculo"),
        "patente": venta.get("datosConductorVehiculo", {}).get("patente"),
        "reparticion_kms": venta.get("datosConductorVehiculo", {}).get("reparticion_kms"),
        "vale": venta.get("datosConductorVehiculo", {}).get("vale"),
        "orden": venta.get("datosConductorVehiculo", {}).get("orden"),
        "jsonOriginal": venta
    }

    data_ventas_cuerpo = []
    for item in venta.get("Cuerpo"):
        cuerpo_dict = {
            "item": item.get("item"),
            "idSector": item.get("idSector"),
            "idArticulo": item.get("idArticulo"),
            "idRubro": item.get("idRubro"),
            "descripcion": item.get("descripcion"),
            "cantidad": item.get("cantidad"),
            "precioVentaCobrado": item.get("precioVentaCobrado"),
            "precioVentaLista": item.get("precioVentaLista"),
            "precioCosto": item.get("precioCosto"),
            "tasaIVA": item.get("tasaIVA"),
            "impuestoInterno1": item.get("impuestoInterno1"),
            "impuestoInterno2": item.get("impuestoInterno2"),
            "impuestoInterno3": item.get("impuestoInterno3"),
            "idIVA": item.get("idIVA"),
            "idCodigoSurtidor": item.get("idCodigoSurtidor")
        }
        data_ventas_cuerpo.append(cuerpo_dict)

    # Si pagó al contado, creamos dict de None porque detalle es lista vacía
    detalle = venta.get("formaDePago", {}).get("detalle", [])

    if detalle and len(detalle) > 0:
        fp_detalle = detalle[0]
        data_ventas_formaspago_detalle = {
            "fpa": fp_detalle.get("fpa"),
            "descripcion": fp_detalle.get("descripcion"),
            "importe": fp_detalle.get("importe"),
            "tarjetaDescripcion": fp_detalle.get("tarjetaDescripcion"),
            "tarjetaNumeroCupon": fp_detalle.get("tarjetaNumeroCupon"),
            "tarjetaNumeroLote": fp_detalle.get("tarjetaNumeroLote"),
            "chequesLibrador": fp_detalle.get("chequesLibrador"),
            "chequesBancoDescripcion": fp_detalle.get("chequesBancoDescripcion"),
            "chequesNumero": fp_detalle.get("chequesNumero"),
            "AppYPFNumeroTransaccion": fp_detalle.get("AppYPFNumeroTransaccion"),
            "valesSucursal": fp_detalle.get("valesSucursal"),
            "valesNumero": fp_detalle.get("valesNumero"),
            "MercadoPagoNumeroTransaccion": fp_detalle.get("MercadoPagoNumeroTransaccion"),
            "AppYPF_gateways": fp_detalle.get("AppYPF_gateways"),
            "AppYPF_gateways_id": fp_detalle.get("AppYPF_gateways_id")
        }
    else:
        data_ventas_formaspago_detalle = {
            "fpa": None,
            "descripcion": None,
            "importe": None,
            "tarjetaDescripcion": None,
            "tarjetaNumeroCupon": None,
            "tarjetaNumeroLote": None,
            "chequesLibrador": None,
            "chequesBancoDescripcion": None,
            "chequesNumero": None,
            "AppYPFNumeroTransaccion": None,
            "valesSucursal": None,
            "valesNumero": None,
            "MercadoPagoNumeroTransaccion": None,
            "AppYPF_gateways": None,
            "AppYPF_gateways_id": None
        }

    return data_ventas, data_ventas_cuerpo, data_ventas_formaspago_detalle


def formatear_json_descargas(remito) -> list:
    ''' 
    Formatea y prepara el JSON de un remito de compra para insertar en la tabla `descargas_comb`.
    - Entrada: `remito` (dict) - JSON del remito de compra obtenido de la API.
    - Salida: `data_descargas_comb` (lista de dict) - uno por cada cuerpo de combustible en el remito.
    '''
    # solo queremos considerar los que son de combustible
    data_descargas_comb = []
    for item in remito.get("Cuerpo"):
        # solo agregamos el cuerpo si es de combustible
        if item.get("idTanque") != 0:
            cuerpo_dict = {
                "letra": remito.get("letra"),
                "tipo": remito.get("tipo"),
                "sucursal": remito.get("sucursal"),
                "numero": remito.get("numero"),
                "proveedorRazonSocial": remito.get("proveedor").get("razon_social"),
                "proveedorCUIT": remito.get("proveedor").get("cuit"),
                "fechaComprobante": remito.get("fechaComprobante"),
                "vendedorID": remito.get("Vendedor").get("Id"),
                "vendedorNombre": remito.get("Vendedor").get("nombre"),
                "vendedorEsEncargado": remito.get("Vendedor").get("esEncargado"),
                "item": item.get("item"),
                "idSector": item.get("idSector"),
                "idArticulo": item.get("idArticulo"),
                "idRubro": item.get("idRubro"),
                "descripcion": item.get("descripcion"),
                "cantidad": item.get("cantidad"),
                "importeCompra": item.get("importeCompra"),
                "importeImpuestoInterno": item.get("importeImpuestoInterno"),
                "importeIVA": item.get("importeIVA"),
                "importeDescuentoItem": item.get("importeDescuentoItem"),
                "esCombustible": item.get("esCombustible"),
                "idTanque": item.get("idTanque"),
                "precioCosto": item.get("precioCosto"),
                "tasaIVA": item.get("tasaIVA")
            }
            data_descargas_comb.append(cuerpo_dict)
            
    return data_descargas_comb