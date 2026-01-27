from flask import Flask, jsonify, send_file, send_from_directory, request
from flask_cors import CORS
import os
from datetime import datetime
import traceback
import sys

# Importar módulos locales
try:
    import A_preprocesamiento as pre
    import B_adicion_tras as ttt
    import C_Redist as red
except ImportError as e:
    print(f"Error al importar módulos: {e}")
    sys.exit(1)

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

RUTA_DESCARGAS = os.path.join('static', 'downloads')


# 🔥 MODO RADICAL: borrar TODOS los archivos antes de crear uno nuevo
def limpiar_archivos_antiguos():
    if not os.path.exists(RUTA_DESCARGAS):
        return
    for archivo in os.listdir(RUTA_DESCARGAS):
        ruta = os.path.join(RUTA_DESCARGAS, archivo)
        if os.path.isfile(ruta):
            os.remove(ruta)
            print(f"🗑 Archivo eliminado: {archivo}")


@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


@app.route('/api/ejecutar', methods=['POST'])
def ejecutar_proceso():
    try:
        data = request.json
        bodega = data.get('bodega', 7)
        limite = data.get('limite', 2)

        print("=" * 60)
        print("INICIANDO PROCESO")
        print(f"Bodega: {bodega}")
        print(f"Límite: {limite}")
        print("=" * 60)

        # Paso 1: Preprocesamiento
        print("\n[PASO 1/4] Ejecutando preprocesamiento...")
        VenInv, descripcion, ced = pre.preprocesamiento(bodega)

        if VenInv is None or ced is None:
            raise ValueError("Error en preprocesamiento")

        print(f"✓ Preprocesamiento completado. Registros: {len(VenInv)}")

        # Paso 2: Transferencias en tránsito
        print("\n[PASO 2/4] Añadiendo transferencias en tránsito...")
        VenInv = ttt.transferencias_trasito(VenInv)

        if VenInv is None:
            raise ValueError("transferencias_trasito retornó None")

        print(f"✓ Transferencias procesadas. Registros: {len(VenInv)}")

        # Paso 3: Filtrar por tienda online
        print("\n[PASO 3/4] Filtrando por TIENDA ON LINE(POS)...")

        if 'Descripcion_Almacen_x' not in VenInv.columns:
            print(f"Columnas disponibles: {VenInv.columns.tolist()}")
            raise ValueError("No existe columna 'Descripcion_Almacen_x'")

        VenInv = VenInv[VenInv['Descripcion_Almacen_x'] == 'TIENDA ON LINE(POS)']
        print(f"✓ Filtro aplicado. Registros: {len(VenInv)}")

        # Paso 4: Redistribución
        print("\n[PASO 4/4] Ejecutando redistribución...")
        df = red.redistribucion(limite, VenInv, ced)

        if df is None:
            raise ValueError("redistribucion retornó None")

        print(f"✓ Redistribución completada. Registros finales: {len(df)}")

        # 🧹 LIMPIEZA RADICAL
        limpiar_archivos_antiguos()

        # Guardar archivo
        print("\n[GUARDANDO] Generando archivo Excel...")
        filename = f'reabastecimiento_final_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        filepath = os.path.join(RUTA_DESCARGAS, filename)

        os.makedirs(RUTA_DESCARGAS, exist_ok=True)
        df.to_excel(filepath, index=False)

        print(f"✓ Archivo generado: {filename}")
        print("=" * 60)
        print("PROCESO COMPLETADO")
        print("=" * 60)

        return jsonify({
            'success': True,
            'message': 'Proceso completado exitosamente',
            'registros': len(df),
            'filename': filename
        })

    except Exception as e:
        error_msg = str(e)
        error_trace = traceback.format_exc()

        print("\n" + "=" * 60)
        print("ERROR EN EL PROCESO")
        print("=" * 60)
        print(f"Mensaje: {error_msg}")
        print("\nTraceback:")
        print(error_trace)
        print("=" * 60)

        return jsonify({
            'success': False,
            'error': error_msg,
            'details': error_trace
        }), 500


@app.route('/api/descargar/<filename>')
def descargar_archivo(filename):
    try:
        filepath = os.path.join(RUTA_DESCARGAS, filename)

        if not os.path.exists(filepath):
            return jsonify({'error': 'Archivo no encontrado'}), 404

        response = send_file(filepath, as_attachment=True)

        # 🗑 borrar después de descargar
        @response.call_on_close
        def borrar_archivo():
            if os.path.exists(filepath):
                os.remove(filepath)
                print(f"🗑 Archivo eliminado tras descarga: {filename}")

        return response

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    os.makedirs(RUTA_DESCARGAS, exist_ok=True)

    print("=" * 60)
    print("SERVIDOR INICIADO CORRECTAMENTE")
    print("URL: http://localhost:5000")
    print("=" * 60)

app.run(debug=True, port=5000, host='0.0.0.0')
erflrfgplr
