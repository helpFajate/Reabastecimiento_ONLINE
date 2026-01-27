

def transferencias_trasito(ven_inv):
    import pandas as pd
    import pandas as pd
    from sqlalchemy import create_engine
    from datetime import datetime as dt, timedelta
    import X_fun as fun

    ven_inv = ven_inv.dropna(subset=['DescripcionTalla_x'])
    ven_inv=ven_inv[ven_inv['DescripcionTalla_x']!='01005']
 
    tran=fun.obtener_datos_tran()

    tran=tran[tran['f_referencia']!='01007                                             ']
    ven_inv=ven_inv[ven_inv['Referencia_x']!='717']

    tran=tran.dropna(subset={'f_fecha'})

    tran=tran.replace({'URABA LO NUESTRO':'URABA LO NUESTRO','PORTAL LA 80':'PORTAL DE LA 80','LA 100':'LA 100 (POS)','OUTLET LA 100 (POS)':'OUTLET LA 100','AMERICAS 1':'AMERICAS I','TIENDA ON LINE':'TIENDA ON LINE(POS)','BUENAVISTA':'BUENAVISTA STA MARTA','BUENAVISTA 2':'BUENAVISTA BLLA','EL LIMONAR':'LIMONAR'})

    tran_ent=tran.groupby(['f_referencia', 'f_ext_detalle_1', 'f_ext_detalle_2','f_desc_bodega_ent'])['f_cant_1_sal'].sum().reset_index()
    tran_sal=tran.groupby(['f_referencia', 'f_ext_detalle_1', 'f_ext_detalle_2','f_desc_bodega_sal'])['f_cant_1_sal'].sum().reset_index()

    tran_sal['concatenado']=tran_sal['f_desc_bodega_sal'].str.strip()+tran_sal['f_referencia'].str.strip()+tran_sal['f_ext_detalle_1'].str.strip()+tran_sal['f_ext_detalle_2'].str.strip()
    tran_ent['concatenado']=tran_ent['f_desc_bodega_ent'].str.strip()+tran_ent['f_referencia'].str.strip()+tran_ent['f_ext_detalle_1'].str.strip()+tran_ent['f_ext_detalle_2'].str.strip()

    ven_inv['con'] = ven_inv['Descripcion_Almacen_x'].str.strip() +ven_inv['Referencia_x'].str.strip() +ven_inv['ColorAbreviatura_x'].str.strip()
    ven_inv['concatenado']=ven_inv['con']+ven_inv['DescripcionTalla_x'].str.strip()

    coincidencias = tran_sal['concatenado'].isin(ven_inv['concatenado'])
    cantidad = coincidencias.sum()
    print(f"Cantidad de coincidencias salidas: {cantidad}")

    coincidencias = tran_ent['concatenado'].isin(ven_inv['concatenado'])
    cantidad = coincidencias.sum()
    print(f"Cantidad de coincidencias entradas: {cantidad}")

    # 1. Seleccionamos las columnas útiles de tran_ent para que, si se agrega una fila nueva,
    # sepamos de qué referencia/bodega se trata (no solo el concatenado y la cantidad).
    cols_tran = ['concatenado', 'f_cant_1_sal', 'f_referencia', 
                'f_ext_detalle_1', 'f_ext_detalle_2', 'f_desc_bodega_ent']

    # 2. Hacemos el merge con how='outer'
    df_final = ven_inv.merge(
        tran_ent[cols_tran], 
        on='concatenado', 
        how='outer'
    )

    # 3. Renombramos la columna de cantidad y llenamos los NaN de cantidad con 0
    df_final = df_final.rename(columns={'f_cant_1_sal': 'salidas_transito'})
    df_final['salidas_transito'] = df_final['salidas_transito'].fillna(0)

    # 4. (Opcional) Limpieza de datos
    # Como las filas nuevas vendrán con las columnas originales de 'ven_inv' vacías (NaN),
    # puedes intentar rellenarlas con la info que trajimos de 'tran_ent'.
    # Ejemplo: Si en ven_inv tu columna de referencia se llama 'Referencia':
    # df_final['Referencia'] = df_final['Referencia'].fillna(df_final['f_referencia'])

    df_final['Descripcion_Almacen_x'] = df_final['Descripcion_Almacen_x'].fillna(df_final['f_desc_bodega_ent'])
    df_final['Referencia_x'] = df_final['Referencia_x'].fillna(df_final['f_referencia'])
    df_final['DescripcionTalla_x'] = df_final['DescripcionTalla_x'].fillna(df_final['f_ext_detalle_2'])
    df_final['ColorAbreviatura_x'] = df_final['ColorAbreviatura_x'].fillna(df_final['f_ext_detalle_1'])
    df_final['CantidadExistencia'] = df_final['CantidadExistencia'].fillna(0)
    df_final['Cantidad'] = df_final['Cantidad'].fillna(0)

    l=['ALEGRA',
    'AMERICAS 2',
    'AMERICAS I',
    'ARKADIA',
    'BOCAGRANDE',
    'BUENAVISTA STA MARTA',
    'BUENAVISTA BLLA',
    'CABECERA',
    'CACIQUE',
    'CARIBE PLAZA',
    'CAÑAVERAL',
    'CENTRO',
    'CENTRO CHIA',
    'CENTRO MAYOR',
    'CHIPICHAPE',
    'EL TESORO',
    'FLORIDA',
    'FUNDADORES',
    'GRAN ESTACION',
    'GRAN PLAZA',
    'GRAN PLAZA DEL SOL',
    'HAYUELOS',
    'INTERMEDICAS',
    'JARDIN PLAZA CALI',
    'JARDIN PLAZA CUCUTA',
    'LA 10',
    'LA 100 (POS)',
    'MAYALES',
    'MAYORCA 2',
    'MAYORCA 3',
    'MERCURIO',
    'MOLINOS',
    'MONTERIA',
    'NUESTRO BOGOTA',
    'OUTLET LA 100',
    'PALMETO',
    'PLAZA CENTRAL',
    'PLAZA FABRICATO',
    'PLAZA IMPERIAL',
    'PORTAL DE LA 80',
    'PRADO',
    'PREMIUM PLAZA',
    'PUERTA DEL NORTE',
    'SALITRE',
    'SAN JOSE',
    'SAN NICOLAS',
    'SANTA FE',
    'TEQUENDAMA',
    'TITAN',
    'UNICENTRO BOGOTA 1',
    'UNICENTRO BOGOTA 2',
    'UNICENTRO CALI',
    'UNICENTRO PASTO',
    'UNICENTRO TUNJA',
    'URABA LO NUESTRO',
    'VENTURA',
    'VIVA ENVIGADO',
    'ZAZUE PLAZA',
    'BELEN',
    'TIENDA ON LINE(POS)',
    'LIMONAR']
    df_final2=df_final[~df_final['Descripcion_Almacen_x'].isin(l)]
    df_final=df_final[df_final['Descripcion_Almacen_x'].isin(l)]
    print(sorted(df_final2['Descripcion_Almacen_x'].unique()))

    df_final['CantidadExistencia']=df_final['CantidadExistencia'].astype(int)+df_final['salidas_transito'].astype(int)
    df_final=df_final[['Descripcion_Almacen_x', 'Referencia_x', 'ColorAbreviatura_x', 'DescripcionTalla_x','CantidadExistencia', 'Cantidad']]

    df_final['con'] = df_final['Descripcion_Almacen_x'].str.strip() +df_final['Referencia_x'].str.strip() +df_final['ColorAbreviatura_x'].str.strip()
    print('Adición de transferencias en tránsito finalizada ✅')
    return df_final