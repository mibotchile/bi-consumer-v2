import os
import requests
import json
import uuid
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv() 

# --- CONFIGURACIÓN ---
DORIS_HOST = os.getenv("DORIS_FE_HOST")
DORIS_PORT = os.getenv("DORIS_HTTP_PORT")
DB = os.getenv("DORIS_DB_GENERAL", "GENERAL")      
TABLE = "mibotair_results"
USER = os.getenv("DORIS_USER")
PASSWORD = os.getenv("DORIS_PASSWORD")

if not all([DORIS_HOST, DORIS_PORT, USER, PASSWORD]):
    print("❌ Error: Faltan variables en el archivo .env")
    sys.exit(1)

# URL Base
load_url = f"http://{DORIS_HOST}:{DORIS_PORT}/api/{DB}/{TABLE}/_stream_load"

# Headers para TRANSACCIÓN DE PRUEBA
headers = {
    "format": "json",
    "strip_outer_array": "true",
    "Expect": "100-continue",
    "label": f"healthcheck_{str(uuid.uuid4())}",
    "two_phase_commit": "true"  # <--- ESTO ACTIVA EL MODO TRANSACCIONAL
}

# Datos dummy
data = [{
    "uid": str(uuid.uuid4()),
    "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
    "client_uid": "HEALTHCHECK",
    "project_uid": "HEALTHCHECK",
    "observation": "Healthcheck Transaction - Should be aborted",
    "campaign_id": 0
}]

print(f"--- Iniciando Health Check (Simulacro de Escritura) en {DORIS_HOST} ---")

try:
    # ---------------------------------------------------------
    # FASE 1: PRE-COMMIT (Enviar datos sin guardar definitivamente)
    # ---------------------------------------------------------
    response = requests.put(
        load_url,
        data=json.dumps(data),
        headers=headers,
        auth=(USER, PASSWORD),
        allow_redirects=False 
    )

    final_response = response

    # Manejo de redirección (FE -> BE)
    if response.status_code == 307:
        redirect_url = response.headers.get("Location")
        final_response = requests.put(
            redirect_url,
            data=json.dumps(data),
            headers=headers,
            auth=(USER, PASSWORD), 
            allow_redirects=True
        )

    resp_dict = final_response.json()

    # ---------------------------------------------------------
    # FASE 2: VERIFICACIÓN Y ABORTO (Rollback)
    # ---------------------------------------------------------
    if resp_dict.get("Status") == "Success":
        txn_id = resp_dict.get("TxnId")
        print(f"✅ Conexión y Envío: OK (TxnId: {txn_id})")
        
        # Ahora procedemos a ABORTAR la transacción para no ensuciar la BD
        abort_url = f"http://{DORIS_HOST}:{DORIS_PORT}/api/{DB}/_stream_load_2pc"
        
        abort_headers = {
            "txn_operation": "abort",
            "txn_id": str(txn_id)
        }
        
        # Nota: La orden de abortar también puede ser redirigida, pero requests lo maneja bien aquí
        # usualmente porque va al mismo BE.
        abort_response = requests.put(
            abort_url,
            headers=abort_headers,
            auth=(USER, PASSWORD)
        )
        
        # Validamos que se haya cancelado correctamente
        # A veces Doris devuelve texto plano o JSON en el abort, manejamos ambos
        try:
            # Doris suele devolver JSON con msg: success
            if abort_response.status_code == 200:
                print("✅ Limpieza (Rollback): OK - No se guardaron datos.")
                print("\n🎉 RESULTADO: El sistema está 100% operativo y listo para escribir.")
            else:
                print(f"⚠️ Alerta: El envío funcionó, pero el Rollback falló (Code {abort_response.status_code}).")
        except:
             print("✅ Limpieza: Enviada.")

    else:
        print("\n❌ FALLÓ EL HEALTH CHECK")
        print(f"   - Mensaje: {resp_dict.get('Message')}")
        print(f"   - Error URL: {resp_dict.get('ErrorURL')}")
        sys.exit(1)

except Exception as e:
    print(f"\n❌ Error Crítico: {e}")
    sys.exit(1)