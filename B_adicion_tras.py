"""
Autor y mantenimiento

Área de Sistemas — Vivell S.A.S

Desarrollado por:

Juan Sebastián Jaramillo (Ing. Industrial) aux.bi1@vivell.co  
Melina Muñoz M. (Desarrolladora de Software) help.desk@vivell.co  

Donde la visión estratégica y la implementación técnica se unen.
"""

def transferencias_trasito(ven_inv):
    import pandas as pd
    import pandas as pd
    from sqlalchemy import create_engine
    from datetime import datetime as dt, timedelta
    import X_fun as fun

    # =========================
    # LIMPIEZA INICIAL DE DATOS
    # =========================

    # Eliminar registros sin talla
    ven_inv = ven_inv.dropna(subset=['DescripcionTalla_x'])

    # Excluir talla específica
    ven_inv = ven_inv[ven_inv['DescripcionTalla_x'] != '01005']
 
    # Obtener transferencias desde sistema origen
    tran = fun.obtener_datos_tran()

    # Excluir referencias específicas
    tran = tran[tran['f_referencia'] != '01007                                             ']
    ven_inv = ven_inv[ven_inv['Referencia_x'] != '717']

    # Eliminar registros sin fecha
    tran = tran.dropna(subset={'f_fecha'})

    # Normalización de nombres de bodegas
    tran = tran.replace({
        'URABA LO NUESTRO':'URABA LO NUESTRO',
        'PORTAL LA 80':'PORTAL DE LA 80',
        'LA 100':'LA 100 (POS)',
        'OUTLET LA 100 (POS)':'OUTLET LA 100',
        'AMERICAS 1':'AMERICAS I',
        'TIENDA ON LINE':'TIENDA ON LINE(POS)',
        'BUENAVISTA':'BUENAVISTA STA MARTA',
        'BUENAVISTA 2':'BUENAVISTA BLLA',
        'EL LIMONAR':'LIMONAR'
    })

    # =========================
    # AGRUPACIÓN DE TRANSFERENCIAS
    # =========================

    # Transferencias entrantes agrupadas
    tran_ent = tran.groupby(
        ['f_referencia', 'f_ext_detalle_1', 'f_ext_detalle_2','f_desc_bodega_ent']
    )['f_cant_1_sal'].sum().reset_index()

    # Transferencias salientes agrupadas
    tran_sal = tran.groupby(
        ['f_referencia', 'f_ext_detalle_1', 'f_ext_detalle_2','f_desc_bodega_sal']
    )['f_cant_1_sal'].sum().reset_index()

    # Generación de llaves concatenadas
    tran_sal['concatenado'] = (
        tran_sal['f_desc_bodega_sal'].str.strip() +
        tran_sal['f_referencia'].str.strip() +
        tran_sal['f_ext_detalle_1'].str.strip() +
        tran_sal['f_ext_detalle_2'].str.strip()
    )

    tran_ent['concatenado'] = (
        tran_ent['f_desc_bodega_ent'].str.strip() +
        tran_ent['f_referencia'].str.strip() +
        tran_ent['f_ext_detalle_1'].str.strip() +
        tran_ent['f_ext_detalle_2'].str.strip()
    )

    # Llave concatenada para inventario base
    ven_inv['con'] = (
        ven_inv['Descripcion_Almacen_x'].str.strip() +
        ven_inv['Referencia_x'].str.strip() +
        ven_inv['ColorAbreviatura_x'].str.strip()
    )

    ven_inv['concatenado'] = ven_inv['con'] + ven_inv['DescripcionTalla_x'].str.strip()

    # =========================
    # VALIDACIÓN DE COINCIDENCIAS
    # =========================

    coincidencias = tran_sal['concatenado'].isin(ven_inv['concatenado'])
    cantidad = coincidencias.sum()
    print(f"Cantidad de coincidencias salidas: {cantidad}")

    coincidencias = tran_ent['concatenado'].isin(ven_inv['concatenado'])
    cantidad = coincidencias.sum()
    print(f"Cantidad de coincidencias entradas: {cantidad}")

    # =========================
    # UNIÓN CON TRANSFERENCIAS
    # =========================

    # Columnas necesarias para conservar información al unir
    cols_tran = [
        'concatenado', 
        'f_cant_1_sal', 
        'f_referencia', 
        'f_ext_detalle_1', 
        'f_ext_detalle_2', 
        'f_desc_bodega_ent'
    ]

    # Unión tipo outer para conservar productos sin inventario base
    df_final = ven_inv.merge(
        tran_ent[cols_tran], 
        on='concatenado', 
        how='outer'
    )

    # Renombrar cantidad de tránsito
    df_final = df_final.rename(columns={'f_cant_1_sal': 'salidas_transito'})
    df_final['salidas_transito'] = df_final['salidas_transito'].fillna(0)

    # =========================
    # COMPLETAR CAMPOS VACÍOS
    # =========================

    df_final['Descripcion_Almacen_x'] = df_final['Descripcion_Almacen_x'].fillna(df_final['f_desc_bodega_ent'])
    df_final['Referencia_x'] = df_final['Referencia_x'].fillna(df_final['f_referencia'])
    df_final['DescripcionTalla_x'] = df_final['DescripcionTalla_x'].fillna(df_final['f_ext_detalle_2'])
    df_final['ColorAbreviatura_x'] = df_final['ColorAbreviatura_x'].fillna(df_final['f_ext_detalle_1'])
    df_final['CantidadExistencia'] = df_final['CantidadExistencia'].fillna(0)
    df_final['Cantidad'] = df_final['Cantidad'].fillna(0)

    # =========================
    # FILTRADO DE BODEGAS VÁLIDAS
    # =========================

    l = [
        'ALEGRA','AMERICAS 2','AMERICAS I','ARKADIA','BOCAGRANDE',
        'BUENAVISTA STA MARTA','BUENAVISTA BLLA','CABECERA','CACIQUE',
        'CARIBE PLAZA','CAÑAVERAL','CENTRO','CENTRO CHIA','CENTRO MAYOR',
        'CHIPICHAPE','EL TESORO','FLORIDA','FUNDADORES','GRAN ESTACION',
        'GRAN PLAZA','GRAN PLAZA DEL SOL','HAYUELOS','INTERMEDICAS',
        'JARDIN PLAZA CALI','JARDIN PLAZA CUCUTA','LA 10','LA 100 (POS)',
        'MAYALES','MAYORCA 2','MAYORCA 3','MERCURIO','MOLINOS','MONTERIA',
        'NUESTRO BOGOTA','OUTLET LA 100','PALMETO','PLAZA CENTRAL',
        'PLAZA FABRICATO','PLAZA IMPERIAL','PORTAL DE LA 80','PRADO',
        'PREMIUM PLAZA','PUERTA DEL NORTE','SALITRE','SAN JOSE',
        'SAN NICOLAS','SANTA FE','TEQUENDAMA','TITAN',
        'UNICENTRO BOGOTA 1','UNICENTRO BOGOTA 2','UNICENTRO CALI',
        'UNICENTRO PASTO','UNICENTRO TUNJA','URABA LO NUESTRO','VENTURA',
        'VIVA ENVIGADO','ZAZUE PLAZA','BELEN','TIENDA ON LINE(POS)','LIMONAR'
    ]

    df_final2 = df_final[~df_final['Descripcion_Almacen_x'].isin(l)]
    df_final = df_final[df_final['Descripcion_Almacen_x'].isin(l)]

    print(sorted(df_final2['Descripcion_Almacen_x'].unique()))

    # =========================
    # CÁLCULO FINAL DE EXISTENCIAS
    # =========================

    df_final['CantidadExistencia'] = (
        df_final['CantidadExistencia'].astype(int) +
        df_final['salidas_transito'].astype(int)
    )

    df_final = df_final[
        ['Descripcion_Almacen_x', 'Referencia_x',
         'ColorAbreviatura_x', 'DescripcionTalla_x',
         'CantidadExistencia', 'Cantidad']
    ]

    # Llave auxiliar
    df_final['con'] = (
        df_final['Descripcion_Almacen_x'].str.strip() +
        df_final['Referencia_x'].str.strip() +
        df_final['ColorAbreviatura_x'].str.strip()
    )

    print('Adición de transferencias en tránsito finalizada ✅')

    return df_final
