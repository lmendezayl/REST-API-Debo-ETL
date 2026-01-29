import json
from pyodbc import Cursor


class insertar:
    """Módulo para insertar datos en la base de datos."""
    
    @staticmethod
    def ventas_bulk(ventas_data: list[dict], cursor: Cursor):
        """
        Procesa múltiples ventas en batch.
        Cada elemento en ventas_data debe tener: data, data_cuerpo, data_formaspago
        """
        for venta in ventas_data:
            insertar.ventas(
                venta['data'], 
                venta['data_cuerpo'], 
                venta['data_formaspago'], 
                cursor
            )
        # El commit se hace una sola vez desde fuera
    
    @staticmethod
    def ventas(data: dict, data_cuerpo: list[dict], data_formaspago: dict, cursor: Cursor):
        """
        Inserta o actualiza en base de datos en tabla `ventas` usando MERGE.
        Serializa cualquier campo tipo dict antes de insertar.
        
        Usa letra, tipo, sucursal y numero como clave única.
        Obtiene el id (insertado o existente) para las tablas relacionadas.
        """
        for k, v in data.items():
            if isinstance(v, dict):
                data[k] = json.dumps(v)

        claves_unicas = ['letra', 'tipo', 'sucursal', 'numero']
        columnas = list(data.keys())
        
        on_clause = ' AND '.join([f"target.{col} = source.{col}" for col in claves_unicas])
        update_set = ', '.join([f"target.{col} = source.{col}" for col in columnas if col not in claves_unicas])
        insert_cols = ', '.join(columnas)
        insert_vals = ', '.join([f"source.{col}" for col in columnas])
        
        query = f"""
        MERGE ventas AS target
        USING (SELECT {', '.join([f'? AS {col}' for col in columnas])}) AS source
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
        OUTPUT INSERTED.id;
        """
        
        cursor.execute(query, tuple(data.values()))
        venta_id = cursor.fetchone()[0] #type: ignore

        insertar.ventas_cuerpo(data_cuerpo, cursor, venta_id)
        insertar.ventas_formaspago_detalle(data_formaspago, cursor, venta_id)

    @staticmethod
    def ventas_cuerpo(data_cuerpo: list[dict], cursor: Cursor, venta_id):
        """
        Inserta o actualiza múltiples registros en la tabla `ventas_cuerpo` usando MERGE.
        Serializa dicts y agrega ventaId como FK.
        Usa ventaId + item como clave única compuesta.
        Procesa todos los items en una sola query.
        """
        if not data_cuerpo:
            return
        
        # Preparar todos los datos
        valores = []
        for data in data_cuerpo:
            item_data = data.copy()
            for k, v in item_data.items():
                if isinstance(v, dict):
                    item_data[k] = json.dumps(v)
            item_data['ventaId'] = venta_id
            valores.append(item_data)
        
        claves_unicas = ['ventaId', 'item']
        columnas = list(valores[0].keys())
        
        # Construir la cláusula SOURCE con UNION ALL para múltiples registros
        placeholders_por_fila = ', '.join([f'? AS {col}' for col in columnas])
        source_values = ' UNION ALL '.join([f'SELECT {placeholders_por_fila}' for _ in valores])
        
        on_clause = ' AND '.join([f"target.{col} = source.{col}" for col in claves_unicas])
        update_set = ', '.join([f"target.{col} = source.{col}" for col in columnas if col not in claves_unicas])
        insert_cols = ', '.join(columnas)
        insert_vals = ', '.join([f"source.{col}" for col in columnas])
        
        query = f"""
        MERGE ventas_cuerpo AS target
        USING ({source_values}) AS source ({', '.join(columnas)})
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals});
        """
        
        params = tuple(v for item in valores for v in item.values())
        cursor.execute(query, params)

    @staticmethod
    def ventas_formaspago_detalle(data_formaspago: dict, cursor: Cursor, venta_id):
        """
        Inserta o actualiza en base de datos en tabla `ventas_formaspago_detalle` usando MERGE.
        Serializa dicts y agrega ventaId como FK.
        Usa ventaId como clave única (solo hay un detalle de forma de pago por venta).
        """
        for k, v in data_formaspago.items():
            if isinstance(v, dict):
                data_formaspago[k] = json.dumps(v)
        data_formaspago['ventaId'] = venta_id
        
        claves_unicas = ['ventaId']
        columnas = list(data_formaspago.keys())
        
        on_clause = ' AND '.join([f"target.{col} = source.{col}" for col in claves_unicas])
        update_set = ', '.join([f"target.{col} = source.{col}" for col in columnas if col not in claves_unicas])
        insert_cols = ', '.join(columnas)
        insert_vals = ', '.join([f"source.{col}" for col in columnas])
        
        query = f"""
        MERGE ventas_formaspago_detalle AS target
        USING (SELECT {', '.join([f'? AS {col}' for col in columnas])}) AS source
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals});
        """
        cursor.execute(query, tuple(data_formaspago.values()))

    @staticmethod
    def descargas(data: dict, cursor: Cursor):
        """
        Inserta o actualiza en base de datos en tabla `descargas_comb` usando MERGE.
        Serializa dicts antes de insertar.
        Usa letra, tipo, sucursal, numero e item como clave única compuesta.
        """
        for k, v in data.items():
            if isinstance(v, dict):
                data[k] = json.dumps(v)
        
        claves_unicas = ['letra', 'tipo', 'sucursal', 'numero', 'item']
        columnas = list(data.keys())
        
        on_clause = ' AND '.join([f"target.{col} = source.{col}" for col in claves_unicas])
        update_set = ', '.join([f"target.{col} = source.{col}" for col in columnas if col not in claves_unicas])
        insert_cols = ', '.join(columnas)
        insert_vals = ', '.join([f"source.{col}" for col in columnas])
        
        query = f"""
        MERGE descargas_comb AS target
        USING (SELECT {', '.join([f'? AS {col}' for col in columnas])}) AS source
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals});
        """
        cursor.execute(query, tuple(data.values()))
