# ============================================================
# Autor y mantenimiento
#
# Área de Sistemas — Vivell S.A.S
#
# Desarrollado por:
#
# Juan Sebastián Jaramillo (Ing. Industrial) aux.bi1@vivell.co
# Melina Muñoz M. (Desarrolladora de Software) help.desk@vivell.co
#
# Donde la visión estratégica y la implementación técnica se unen.
# ============================================================
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

# ============================================================
# FUNCIONES DE CONEXIÓN A BASES DE DATOS
# ============================================================

# Conexión a la base de datos de Reportes (Data Warehouse)
def conectar_sqlalchemy():
    try:
        # Cadena de conexión a SQL Server usando autenticación integrada
        connection_string = "mssql+pyodbc://amaterasu\\siesa/reportes?trusted_connection=yes&driver=SQL+Server"
        engine = create_engine(connection_string)
        print('Conexión exitosa')
        return engine
    except Exception as ex:
        # Manejo de errores en la conexión
        print(f'Error en la conexión: {ex}')
        return None
    

# Conexión a la base de datos operativa UnoEE
def conectar_sql():
    try:
        # Cadena de conexión a SQL Server usando autenticación integrada
        connection_string = "mssql+pyodbc://amaterasu\\siesa/UnoEE?trusted_connection=yes&driver=SQL+Server"
        engine = create_engine(connection_string)
        print('Conexión exitosa')
        return engine
    except Exception as ex:
        # Manejo de errores en la conexión
        print(f'Error en la conexión: {ex}')
        return None


# ============================================================
# FUNCIONES DE EXTRACCIÓN DE DATOS
# ============================================================

# Obtener información histórica de ventas
def obtener_ventas():
    engine = conectar_sqlalchemy()
    if engine:
        try:
            # Se calcula el primer día del año anterior para filtrar las ventas
            primer_dia_ano = datetime(datetime.now().year - 1, 1, 1).strftime('%Y-%m-%d')
            print(primer_dia_ano)

            # Consulta SQL que integra ventas, productos y almacenes
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
            # Manejo de errores durante la ejecución de la consulta
            print(f'Error al ejecutar la consulta: {ex}')
            return None

        finally:
            # Cierre explícito de la conexión
            engine.dispose()
    else:
        return None


# Obtener inventario general desde el sistema UnoEE
def obtener_inv():
    engine = conectar_sql()
    if engine:
        # Ejecución del procedimiento almacenado de inventarios
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
        engine.dispose()
        return rem
    else:
        return None
    

# Obtener catálogo de productos
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


# Obtener catálogo de almacenes
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
    

# Obtener transferencias en tránsito entre bodegas
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
        engine.dispose()
        return rem
    else:
        return None
    

# Obtener inventario de una bodega específica
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
        engine.dispose()
        return rem
    else:
        return None
