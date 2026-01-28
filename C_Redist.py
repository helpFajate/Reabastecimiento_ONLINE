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

import pandas as pd 
import numpy as np

def redistribucion(lim, VenInv, ced):
    """
    Realiza la redistribución de inventario desde bodegas con exceso hacia bodegas
    con mayor rotación de ventas, respetando un límite máximo por bodega.

    Parámetros:
    lim    : límite máximo de unidades permitidas por SKU en cada bodega
    VenInv : DataFrame con inventario y ventas por bodega
    ced    : DataFrame con cantidades disponibles para redistribuir

    Retorna:
    DataFrame con las cantidades a enviar por bodega y SKU
    """

    # Normalización de nombres de columnas para trabajar con un mismo esquema
    ced.rename(columns={
        'Referencia_x':'Referencia',
        'ColorAbreviatura_x':'ColorAbreviatura',
        'DescripcionTalla_x':'DescripcionTalla'
    }, inplace=True)

    # Construcción del identificador único de producto (SKU)
    ced['SKU'] = (
        ced['Referencia'].str.strip() +
        ced['ColorAbreviatura'].str.strip() +
        ced['DescripcionTalla'].str.strip()
    )

    VenInv.rename(columns={
        'Referencia_x':'Referencia',
        'ColorAbreviatura_x':'ColorAbreviatura',
        'DescripcionTalla_x':'DescripcionTalla'
    }, inplace=True)

    VenInv['SKU'] = (
        VenInv['Referencia'].str.strip() +
        VenInv['ColorAbreviatura'].str.strip() +
        VenInv['DescripcionTalla'].str.strip()
    )

    # Ordenar por ventas, bodega y referencia
    VenInv.sort_values(
        by=['Cantidad','Descripcion_Almacen_x','Referencia'],
        ascending=False,
        inplace=True
    )

    # Inicializar columnas de control
    VenInv['Cantidad Enviar'] = 0
    VenInv['Cant Original'] = VenInv['CantidadExistencia']

    # Copia ordenada por ventas
    df2 = VenInv.sort_values(by=['Cantidad'], ascending=False)

    # Clave compuesta por bodega + producto
    df2['con'] = (
        df2['Descripcion_Almacen_x'].str.strip() +
        df2['Referencia'].str.strip() +
        df2['ColorAbreviatura'].str.strip() +
        df2['DescripcionTalla'].str.strip()
    )

    ced['con'] = (
        ced['Descripcion_Almacen_x'].str.strip() +
        ced['Referencia'].str.strip() +
        ced['ColorAbreviatura'].str.strip() +
        ced['DescripcionTalla'].str.strip()
    )

    # Agrupación por clave para consolidar ventas
    df3 = df2.groupby(['con'])[['Cantidad']].sum().reset_index()

    # Unión con el consolidado
    df2 = df2.merge(df3, on='con', how='left')

    # ============================
    # PROCESO DE REDISTRIBUCIÓN
    # ============================

    # Iterar sobre cada fila del DataFrame ced (origen de unidades)
    for index, row in ced.iterrows():
        cantidad_a_distribuir = row['CantidadExistencia_y']
        sku = row['SKU']

        # Filtrar bodegas que venden ese SKU, ordenadas por mayor rotación
        matching_rows = df2[df2['SKU'] == sku].sort_values(
            by='Cantidad_x',
            ascending=False
        )

        # Distribuir unidades en las bodegas disponibles
        for bodega_index, bodega_row in matching_rows.iterrows():
            if cantidad_a_distribuir <= 0:
                break  # No hay más unidades por distribuir

            # Espacio disponible respetando el límite por bodega
            espacio_disponible = max(
                0,
                lim - (df2.at[bodega_index, 'Cantidad Enviar'] +
                       df2.at[bodega_index, 'Cant Original'])
            )
            
            if espacio_disponible > 0:
                # Cantidad a asignar sin superar límite ni disponibilidad
                asignar = min(cantidad_a_distribuir, espacio_disponible)
                
                # Registrar asignación en la bodega
                df2.at[bodega_index, 'Cantidad Enviar'] += asignar
                
                # Descontar del stock disponible en ced
                ced.at[index, 'CantidadExistencia_y'] -= asignar
                cantidad_a_distribuir -= asignar

    # Reinicio de índices tras el procesamiento
    ced.reset_index(drop=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)

    # Renombrar columnas para salida final
    df2.rename(columns={
        'Descripcion_Almacen_x':'almacen',
        'ColorAbreviatura':'color',
        'DescripcionTalla':'talla',
        'Cantidad_x':'ventas'
    }, inplace=True)

    # Eliminación de columnas auxiliares
    df2.drop(columns={'con','Cantidad_y','CantidadExistencia'}, inplace=True)

    # Filtrar únicamente registros con cantidad asignada
    df2 = df2[df2['Cantidad Enviar'] > 0]

    print('Redistribución finalizada ✅')

    return df2
