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

def preprocesamiento(bodega=7):
    """
    Función de preprocesamiento para redistribución de inventario.
    Obtiene ventas, inventario y productos, limpia datos,
    cruza información y genera tablas finales para redistribución.
    """

    import pandas as pd
    import X_fun as fun

    # =========================
    # OBTENER DATOS BASE
    # =========================

    df_ventas = fun.obtener_ventas()       # Ventas históricas
    inv = fun.obtener_inv()                # Inventario general

    # Excluir inventario de encaje
    inv = inv[inv['f_desc_bodega'] != 'INVENTARIO ENCAJE COMPRESION MEDIA']

    # Selección de columnas relevantes de ventas
    df_ventas = df_ventas[['Referencia','ColorAbreviatura','DescripcionTalla',
                            'Id_Producto','Descripcion_Almacen','Id_Almacen','Cantidad']]

    # Eliminar referencias indeterminadas
    df_ventas = df_ventas[df_ventas['Referencia'] != 'Indeterminado']

    # Agrupar ventas por producto y almacén
    df_ventas = df_ventas.groupby(
        ['Referencia','ColorAbreviatura','DescripcionTalla','Id_Producto','Id_Almacen']
    )['Cantidad'].sum().reset_index()

    df_inv = inv

    # =========================
    # CREAR BASE DE COMBINACIONES
    # =========================

    alm = df_inv['f_co_bodega'].unique()
    alm = pd.DataFrame(alm)
        
    df3 = fun.obtener_productos()

    # Renombrar columnas del inventario para estandarizar
    df_inv.rename(columns={
        'f_referencia':'Referencia',
        'f_ext_detalle_1':'ColorAbreviatura',
        'f_ext_detalle_2':'DescripcionTalla'
    }, inplace=True)

    # Producto cartesiano entre almacenes y productos
    CantidadExistencia = alm.merge(df3, how='cross')
    CantidadExistencia = CantidadExistencia[[0,'Referencia','ColorAbreviatura','DescripcionTalla']]
    df2 = CantidadExistencia
    df2['CantidadExistencia'] = 0

    # Renombrar cantidad disponible
    df_inv.rename(columns={'f_cant_disponible_1':'CantidadExistencia'}, inplace=True)

    # =========================
    # CRUCE INVENTARIO REAL
    # =========================

    keys = [0, 'Referencia', 'ColorAbreviatura', 'DescripcionTalla']

    merged_df = df2.merge(
        df_inv, 
        left_on=keys, 
        right_on=['f_co_bodega', 'Referencia', 'ColorAbreviatura', 'DescripcionTalla'],
        how='left',
        indicator=True
    )

    merged_df = merged_df[[0, 'Referencia', 'ColorAbreviatura',
                           'DescripcionTalla','CantidadExistencia_x','CantidadExistencia_y']]

    # Reemplazar nulos por cero
    merged_df['CantidadExistencia_y'] = merged_df['CantidadExistencia_y'].fillna(0)

    lo = sorted(merged_df[0].unique())
        
    # =========================
    # OBTENER DESCRIPCIÓN DE ALMACENES
    # =========================

    df4 = fun.obtener_dim_almacenes()
    df4 = df4[['Id_Almacen','Descripcion_Almacen']]

    merged_df = merged_df.merge(df4, left_on=0, right_on='Id_Almacen', how='left')

    # =========================
    # FILTRAR ALMACENES NO USADOS
    # =========================

    categorias_a_filtrar = [
        'BTOB BOGOTA- CENTRO',
        'BTOB BUCARAMANGA - ORIENTE',
        'BTOB CALI - SUR',
        'BTOB COSTA  - NORTE',
        'BTOB MEDELLIN',
        'EVENTOS BOGOTA (POS)',
        'EVENTOS MEDELLIN (POS)',
        'INTERNACIONAL',
        'PRINCIPAL BARRANQUILLA',
        'PRINCIPAL BOGOTA',
        'PRINCIPAL BUCARAMANGA',
        'PRINCIPAL CALI',
        'PRINCIPAL MEDELLIN',
        'METROPOLITANO'
    ]

    merged_df = merged_df[~merged_df['Descripcion_Almacen'].isin(categorias_a_filtrar)].reset_index(drop=True)

    df_ventas = df_ventas.merge(df4, left_on='Id_Almacen', right_on='Id_Almacen', how='left')

    df_ventas = df_ventas[~df_ventas['Descripcion_Almacen'].isin(categorias_a_filtrar)].reset_index(drop=True)

    # =========================
    # GENERAR LLAVES DE CRUCE
    # =========================

    df_ventas.rename(columns={'Id_Producto':'SKU'}, inplace=True)

    df_ventas['concatenado'] = df_ventas['Id_Almacen'].astype(str) + df_ventas['SKU'].str.strip()

    merged_df['SKU'] = (
        merged_df['Referencia'].str.strip() +
        merged_df['ColorAbreviatura'].str.strip() +
        merged_df['DescripcionTalla'].str.strip()
    )

    merged_df['concatenado'] = merged_df[0].astype(str) + merged_df['SKU'].str.strip()

    # Eliminar referencia específica
    df_ventas = df_ventas[df_ventas['Referencia'] != '717'].reset_index(drop=True)

    # =========================
    # CRUCE FINAL VENTAS + INVENTARIO
    # =========================

    VenInv = merged_df.merge(df_ventas, on='concatenado', how='left')

    VenInv = VenInv[['Descripcion_Almacen_x','Referencia_x',
                     'ColorAbreviatura_x','DescripcionTalla_x',
                     'CantidadExistencia_y','Cantidad']]

    # =========================
    # INVENTARIO CEDI
    # =========================

    ced = fun.obtener_inv_be(bodega)
    ced['f_cant_disponible_1'] = ced['f_cant_disponible_1'].astype(int)

    ced = ced[['f_desc_bodega','f_referencia','f_ext_detalle_1',
               'f_ext_detalle_2','f_cant_disponible_1']]

    ced.rename(columns={
        'f_referencia':'Referencia',
        'f_ext_detalle_1':'ColorAbreviatura',
        'f_ext_detalle_2':'DescripcionTalla',
        'f_desc_bodega':'Descripcion_Almacen_x',
        'f_cant_disponible_1':'CantidadExistencia_y'
    }, inplace=True)

    # =========================
    # FILTRADO POR LÍMITE
    # =========================

    lim_min_ref = 0

    VenInv['VenInv'] = VenInv['CantidadExistencia_y'] + VenInv['Cantidad']

    res = VenInv.groupby(['Referencia_x', 'Descripcion_Almacen_x'])['VenInv'].sum().reset_index()
    res = res.sort_values(by='VenInv', ascending=True).reset_index(drop=True)
    res['concatenado'] = res['Descripcion_Almacen_x'].astype(str) + res['Referencia_x'].str.strip()
    res = res[res['VenInv'] < lim_min_ref].reset_index(drop=True)
    res = res[['concatenado']]

    VenInv['concatenado'] = VenInv['Descripcion_Almacen_x'].astype(str) + VenInv['Referencia_x'].str.strip()
    VenInv = VenInv[~VenInv['concatenado'].isin(res['concatenado'])].reset_index(drop=True)

    VenInv.rename(columns={'Bodega':'Bodega','CantidadExistencia_y':'CantidadExistencia'}, inplace=True)
    VenInv.drop(columns=['concatenado','VenInv'], inplace=True)

    VenInv['Cantidad'] = VenInv['Cantidad'].fillna(0)
    VenInv[['CantidadExistencia', 'Cantidad']] = VenInv[['CantidadExistencia', 'Cantidad']].astype(int)

    # =========================
    # ELIMINAR PRODUCTOS SIN MOVIMIENTO
    # =========================

    ab = VenInv.groupby(['Descripcion_Almacen_x','Referencia_x','ColorAbreviatura_x'])[
        ['Cantidad', 'CantidadExistencia']
    ].sum().reset_index()

    ab = ab[(ab['Cantidad'] == 0) & (ab['CantidadExistencia'] == 0)]

    ab['con'] = (ab['Descripcion_Almacen_x'].str.strip() +
                 ab['Referencia_x'].str.strip() +
                 ab['ColorAbreviatura_x'].str.strip())

    ab = ab['con']

    VenInv['con'] = (VenInv['Descripcion_Almacen_x'].str.strip() +
                     VenInv['Referencia_x'].str.strip() +
                     VenInv['ColorAbreviatura_x'].str.strip())

    VenInv = VenInv[~VenInv['con'].isin(ab)].reset_index(drop=True)

    # Excluir CEDI
    VenInv = VenInv[(VenInv['Descripcion_Almacen_x'] != 'CEDI')]

    # Mantener solo inventario positivo en CEDI
    ced = ced[ced['CantidadExistencia_y'] > 0]

    # =========================
    # DESCRIPCIÓN DE PRODUCTOS
    # =========================

    descripcion = df3.groupby(
        ['Referencia','Categoria','Genero','Linea','Sublinea','Marca','Coleccion']
    )['Fuente'].count().reset_index()

    descripcion.drop(columns=['Fuente'], inplace=True)

    VenInv.drop(columns=['con'], inplace=True)

    print('Proceso ETL terminado ✅')

    return VenInv, descripcion, ced
