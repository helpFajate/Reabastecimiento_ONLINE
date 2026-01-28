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

import A_preprocesamiento as pre
import B_adicion_tras as ttt
import C_Redist as red

# ============================
# PARÁMETROS DE CONFIGURACIÓN
# ============================

# Código de la bodega de interés:
# 23  → Tienda Online
# 7   → CEDI
# 118 → Bodega Outlet
bodega = 7

# Límite máximo permitido por bodega para cada SKU
limite = 2


# ============================
# EJECUCIÓN DEL PROCESO
# ============================

# Paso 1: Preprocesamiento de datos de inventario y ventas
VenInv, descripcion, ced = pre.preprocesamiento(bodega)

# Paso 2: Adición de transferencias en tránsito al inventario
VenInv = ttt.transferencias_trasito(VenInv)

# Paso 3: Filtrado de información únicamente para la tienda online
VenInv = VenInv[VenInv['Descripcion_Almacen_x'] == 'TIENDA ON LINE(POS)']

# Paso 4: Redistribución del inventario según límite por bodega
df = red.redistribucion(limite, VenInv, ced)

# ============================
# SALIDA DEL RESULTADO
# ============================

# Exportar el resultado final a un archivo Excel
df.to_excel('reabastecimiento_final.xlsx', index=False)
