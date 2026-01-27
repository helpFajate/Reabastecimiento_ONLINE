
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

#Conectar con BD Reportes
def conectar_sqlalchemy():
    try:
        connection_string = "mssql+pyodbc://amaterasu\\siesa/reportes?trusted_connection=yes&driver=SQL+Server"
        engine = create_engine(connection_string)
        print('Conexión exitosa')
        return engine
    except Exception as ex:
        print(f'Error en la conexión: {ex}')
        return None
    
#Conectar con BD UnoEE
def conectar_sql():
    try:
        # Cadena de conexión usando SQLAlchemy
        connection_string = "mssql+pyodbc://amaterasu\\siesa/UnoEE?trusted_connection=yes&driver=SQL+Server"
        engine = create_engine(connection_string)
        print('Conexión exitosa')
        return engine
    except Exception as ex:
        print(f'Error en la conexión: {ex}')
        return None

# Obtener ventas
def obtener_ventas():
    engine = conectar_sqlalchemy()
    if engine:
        try:
            primer_dia_ano = datetime(datetime.now().year-1, 1, 1).strftime('%Y-%m-%d')
            print(primer_dia_ano)

            query = f"""
            SELECT * 
            FROM FactVentas 
            INNER JOIN DimProductos ON DimProductos.SnkProductos = FactVentas.Snk_Producto 
            INNER JOIN DimAlmacenes ON DimAlmacenes.SnkAlmacenes = FactVentas.Snk_Almacen 
            WHERE FechaFacturaTime >= '{primer_dia_ano}' AND EnumerarDescuento = '1'
            """

            df_ventas = pd.read_sql(text(query), engine)
            return df_ventas
        except Exception as ex:
            print(f'Error al ejecutar la consulta: {ex}')
            return None
        finally:
            engine.dispose()  # Cierra la conexión
    else:
        return None

# Obtener inventarios  
def obtener_inv():
    engine = conectar_sql()
    if engine:
        query = f'''exec sp_cons_inv_exitencias @p_cia=1,
                        @p_tipo_inv=N'',
                        @p_grupo=N'',
                        @p_instalacion=N'',
                        @p_bdg=0,
                        @p_detallar=0,
                        @p_empaque=0,
                        @p_saldos=1,
                        @p_fecha=NULL,
                        @p_permiso_costos=1,
                        @p_rowid_item=0,
                        @p_id_lista_precio=NULL,
                        @p_cons_tipo=10017,
                        @p_cons_nombre=N'AF-INVENTARIO',
                        @p_rowid_usuario=421,
                        @p_id_moneda=N'COP',
                        @p_id_grp_co=NULL,
                        @p_id_co=N'',
                        @p_ind_disponible=1,
                        @p_ind_ubica_inac=0'''
        
        rem = pd.read_sql(query, engine)
        engine.dispose()  # Cierra la conexión
        return rem
    else:
        return None
    
# Obtener datos de productos
def obtener_productos():
    engine = conectar_sqlalchemy()
    if engine:
        try:
            query = """
            SELECT * 
            FROM DimProductos
            """
            df_productos = pd.read_sql(text(query), engine)
            return df_productos
        except Exception as ex:
            print(f'Error al ejecutar la consulta: {ex}')
            return None
        finally:
            engine.dispose()
    else:
        return None

# Obtener datos de almacenes
def obtener_dim_almacenes():
    engine = conectar_sqlalchemy()
    if engine:
        try:
            query = """
            SELECT * 
            FROM DimAlmacenes
            """
            df_almacenes = pd.read_sql(text(query), engine)
            return df_almacenes
        except Exception as ex:
            print(f'Error al ejecutar la consulta: {ex}')
            return None
        finally:
            engine.dispose()
    else:
        return None
    
# Obtener datos de transferencias en tránsito
def obtener_datos_tran():
    engine = conectar_sql()
    if engine:
        query = f'''
        exec sp_cons_inv_transito @p_cia=1,@p_bodega_entrada=0,@p_bodega_salida=0,
        @p_cons_tipo=10188,@p_cons_nombre=N'tata',@p_fecha_corte=NULL,
        @p_permiso_costos=1,@p_grupo_bodega=N'',@p_rowid_usuario=421,
        @p_id_lista_precio=NULL
        '''        
        rem = pd.read_sql(query, engine)
        engine.dispose()  # Cierra la conexión
        return rem
    else:
        return None
    
def obtener_inv_be(bodega):
    engine = conectar_sql()
    if engine:
        query = f'''exec sp_cons_inv_exitencias @p_cia=1,
                        @p_tipo_inv=N'',
                        @p_grupo=N'',
                        @p_instalacion=N'',
                        @p_bdg={bodega},
                        @p_detallar=0,
                        @p_empaque=0,
                        @p_saldos=1,
                        @p_fecha=NULL,
                        @p_permiso_costos=1,
                        @p_rowid_item=0,
                        @p_id_lista_precio=NULL,
                        @p_cons_tipo=10017,
                        @p_cons_nombre=N'AF-INVENTARIO',
                        @p_rowid_usuario=421,
                        @p_id_moneda=N'COP',
                        @p_id_grp_co=NULL,
                        @p_id_co=N'',
                        @p_ind_disponible=1,
                        @p_ind_ubica_inac=0'''
        
        rem = pd.read_sql(query, engine)
        engine.dispose()  # Cierra la conexión
        return rem
    else:
        return None