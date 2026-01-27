import pandas as pd 
import numpy as np
def redistribucion(lim, VenInv, ced):

    ced.rename(columns={'Referencia_x':'Referencia','ColorAbreviatura_x':'ColorAbreviatura','DescripcionTalla_x':'DescripcionTalla'},inplace=True)
    ced['SKU']=ced['Referencia'].str.strip() + ced['ColorAbreviatura'].str.strip() + ced['DescripcionTalla'].str.strip()

    VenInv.rename(columns={'Referencia_x':'Referencia','ColorAbreviatura_x':'ColorAbreviatura','DescripcionTalla_x':'DescripcionTalla'},inplace=True)
    VenInv['SKU']=VenInv['Referencia'].str.strip() + VenInv['ColorAbreviatura'].str.strip() + VenInv['DescripcionTalla'].str.strip()

    VenInv.sort_values(by=['Cantidad','Descripcion_Almacen_x','Referencia'],ascending=False,inplace=True)

    VenInv['Cantidad Enviar'] = 0
    VenInv['Cant Original']=VenInv['CantidadExistencia']

    df2=VenInv.sort_values(by=['Cantidad'], ascending=False)

    df2['con']=df2['Descripcion_Almacen_x'].str.strip() + df2['Referencia'].str.strip() + df2['ColorAbreviatura'].str.strip()+df2['DescripcionTalla'].str.strip()
    ced['con']=ced['Descripcion_Almacen_x'].str.strip() + ced['Referencia'].str.strip() + ced['ColorAbreviatura'].str.strip()+ced['DescripcionTalla'].str.strip()

    df3=df2.groupby(['con'])[['Cantidad']].sum().reset_index()

    df2=df2.merge(df3, on='con', how='left')

    # Iterar sobre cada fila de ced
    for index, row in ced.iterrows():
        cantidad_a_distribuir = row['CantidadExistencia_y']
        sku = row['SKU']

        # Obtener todas las bodegas con este SKU, ordenadas por mayor número de ventas
        matching_rows = df2[df2['SKU'] == sku].sort_values(by='Cantidad_x', ascending=False)

        # Iterar sobre las bodegas disponibles hasta distribuir todas las unidades
        for bodega_index, bodega_row in matching_rows.iterrows():
            if cantidad_a_distribuir <= 0:
                break  # Si ya no hay unidades, salimos

            # Obtener el espacio disponible en la bodega respetando el límite `lim`
            espacio_disponible = max(0, lim - (df2.at[bodega_index, 'Cantidad Enviar'] + df2.at[bodega_index, 'Cant Original']))
            
            if espacio_disponible > 0:
                # Determinar cuántas unidades podemos asignar
                asignar = min(cantidad_a_distribuir, espacio_disponible)
                
                # Asignar unidades a la bodega
                df2.at[bodega_index, 'Cantidad Enviar'] += asignar
                
                # Restar del total disponible en `ced`
                ced.at[index, 'CantidadExistencia_y'] -= asignar
                cantidad_a_distribuir -= asignar

    # Reiniciar índices de los dataframes
    ced.reset_index(drop=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)

    df2.rename(columns={'Descripcion_Almacen_x':'almacen', 'ColorAbreviatura':'color','DescripcionTalla':'talla','Cantidad_x':'ventas'},inplace=True)

    df2.drop(columns={'con','Cantidad_y','CantidadExistencia'}, inplace =True)

    df2=df2[df2['Cantidad Enviar']>0]

    print('Redistribución finalizada ✅')

    return df2