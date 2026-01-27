
def preprocesamiento(bodega=7):

    import pandas as pd
    import X_fun as fun

    df_ventas=fun.obtener_ventas()

    inv=fun.obtener_inv()

    inv=inv[inv['f_desc_bodega']!= 'INVENTARIO ENCAJE COMPRESION MEDIA']

    df_ventas=df_ventas[['Referencia','ColorAbreviatura','DescripcionTalla','Id_Producto','Descripcion_Almacen','Id_Almacen','Cantidad']]

    df_ventas = df_ventas[df_ventas['Referencia'] != 'Indeterminado']

    df_ventas=df_ventas.groupby(['Referencia','ColorAbreviatura','DescripcionTalla','Id_Producto','Id_Almacen'])['Cantidad'].sum().reset_index()

    df_inv=inv

    alm=df_inv['f_co_bodega'].unique()
    alm=pd.DataFrame(alm)
        
    df3=fun.obtener_productos()

    df_inv.rename(columns={'f_referencia':'Referencia','f_ext_detalle_1':'ColorAbreviatura','f_ext_detalle_2':'DescripcionTalla'}, inplace=True)

    CantidadExistencia=alm.merge(df3, how='cross')
    CantidadExistencia=CantidadExistencia[[0,'Referencia','ColorAbreviatura','DescripcionTalla']]
    df2=CantidadExistencia
    df2['CantidadExistencia']=0

    df_inv.rename(columns={'f_cant_disponible_1':'CantidadExistencia'}, inplace=True)

    # Asegurar que las claves coincidan entre las dos tablas
    keys = [0, 'Referencia', 'ColorAbreviatura', 'DescripcionTalla']

    # Hacer un LEFT JOIN de df2 con df_inv usando las claves
    merged_df = df2.merge(
        df_inv, 
        left_on=keys, 
        right_on=['f_co_bodega', 'Referencia', 'ColorAbreviatura', 'DescripcionTalla'],
        how='left',
        indicator=True  # Agrega una columna que indica la presencia en ambas tablas
    )

    merged_df=merged_df[[ 0, 'Referencia', 'ColorAbreviatura', 'DescripcionTalla','CantidadExistencia_x','CantidadExistencia_y']]

    merged_df['CantidadExistencia_y']=merged_df['CantidadExistencia_y'].fillna(0)

    lo = sorted(merged_df[0].unique())
        
    df4=fun.obtener_dim_almacenes()

    df4 = df4[['Id_Almacen','Descripcion_Almacen']]

    merged_df=merged_df.merge(df4, left_on=0, right_on='Id_Almacen', how='left')

    # Lista de categorías a filtrar
    categorias_a_filtrar = [  'BTOB BOGOTA- CENTRO',
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

    # Filtrar el dataframe
    merged_df = merged_df[~merged_df['Descripcion_Almacen'].isin(categorias_a_filtrar)].reset_index(drop=True)

    df_ventas=df_ventas.merge(df4, left_on='Id_Almacen', right_on='Id_Almacen', how='left')

    # Lista de categorías a filtrar
    categorias_a_filtrar = [  'BTOB BOGOTA- CENTRO',
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

    # Filtrar el dataframe
    df_ventas = df_ventas[~df_ventas['Descripcion_Almacen'].isin(categorias_a_filtrar)].reset_index(drop=True)

    df_ventas.rename(columns={'Id_Producto':'SKU'},inplace=True)

    df_ventas['concatenado']=df_ventas['Id_Almacen'].astype(str)+df_ventas['SKU'].str.strip()

    merged_df['SKU']=merged_df['Referencia'].str.strip() + merged_df['ColorAbreviatura'].str.strip() + merged_df['DescripcionTalla'].str.strip()

    merged_df['concatenado']=merged_df[0].astype(str)+merged_df['SKU'].str.strip()

    df_ventas=df_ventas[df_ventas['Referencia']!='717'].reset_index(drop=True)

    VenInv=merged_df.merge(df_ventas, on='concatenado', how='left')

    VenInv=VenInv[['Descripcion_Almacen_x','Referencia_x','ColorAbreviatura_x','DescripcionTalla_x','CantidadExistencia_y','Cantidad']]

    ced=fun.obtener_inv_be(bodega)
    ced['f_cant_disponible_1']=ced['f_cant_disponible_1'].astype(int)
    ced=ced[['f_desc_bodega','f_referencia','f_ext_detalle_1','f_ext_detalle_2','f_cant_disponible_1']]
    ced.rename(columns={'f_referencia':'Referencia','f_ext_detalle_1':'ColorAbreviatura','f_ext_detalle_2':'DescripcionTalla','f_desc_bodega':'Descripcion_Almacen_x','f_cant_disponible_1':'CantidadExistencia_y'}, inplace=True)

    lim_min_ref=0

    VenInv['VenInv']=VenInv['CantidadExistencia_y']+VenInv['Cantidad']

    res=VenInv.groupby(['Referencia_x', 'Descripcion_Almacen_x'])['VenInv'].sum().reset_index()
    res=res.sort_values(by='VenInv',ascending=True).reset_index(drop=True)
    res['concatenado'] = res['Descripcion_Almacen_x'].astype(str) + res['Referencia_x'].str.strip()
    res=res[res['VenInv']<lim_min_ref].reset_index(drop=True)
    res=res[['concatenado']]

    VenInv['concatenado'] = VenInv['Descripcion_Almacen_x'].astype(str) + VenInv['Referencia_x'].str.strip()
    VenInv = VenInv[~VenInv['concatenado'].isin(res['concatenado'])].reset_index(drop=True)
    VenInv.rename(columns={'Bodega':'Bodega','CantidadExistencia_y':'CantidadExistencia'},inplace=True)
    VenInv.drop(columns=['concatenado','VenInv'],inplace=True)

    VenInv['Cantidad']=VenInv['Cantidad'].fillna(0)

    VenInv[['CantidadExistencia', 'Cantidad']] = VenInv[['CantidadExistencia', 'Cantidad']].astype(int)

    ab=VenInv.groupby(['Descripcion_Almacen_x','Referencia_x','ColorAbreviatura_x'])[['Cantidad', 'CantidadExistencia']].sum().reset_index()

    ab=ab[(ab['Cantidad']==0)&(ab['CantidadExistencia']==0)]

    ab['con']=ab['Descripcion_Almacen_x'].str.strip()+ab['Referencia_x'].str.strip()+ab['ColorAbreviatura_x'].str.strip()

    ab=ab['con']

    VenInv['con']=VenInv['Descripcion_Almacen_x'].str.strip()+VenInv['Referencia_x'].str.strip()+VenInv['ColorAbreviatura_x'].str.strip()

    VenInv = VenInv[~VenInv['con'].isin(ab)].reset_index(drop=True)

    VenInv=VenInv[(VenInv['Descripcion_Almacen_x']!='CEDI')]

    ced=ced[ced['CantidadExistencia_y']>0]

    descripcion=df3.groupby(['Referencia','Categoria','Genero','Linea','Sublinea','Marca','Coleccion'])['Fuente'].count().reset_index()

    descripcion.drop(columns=['Fuente'], inplace=True)

    VenInv.drop(columns=['con'],inplace=True)

    print('Proceso ETL terminado ✅')
    return VenInv, descripcion, ced