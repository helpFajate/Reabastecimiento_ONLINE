import A_preprocesamiento as pre
import B_adicion_tras as ttt
import C_Redist as red

# donde esta el 23 es la bodega de interes
# para ONLINE 23
# para el CEDI 7
# para Bodega Outlet 118
bodega=7

# Donde el 2 es el limite maximo por bodega
limite=2


VenInv, descripcion, ced = pre.preprocesamiento(bodega)

VenInv=ttt.transferencias_trasito(VenInv)

VenInv=VenInv[VenInv['Descripcion_Almacen_x']=='TIENDA ON LINE(POS)']

df=red.redistribucion(limite, VenInv, ced)

df.to_excel('reabastecimiento_final.xlsx',index=False)