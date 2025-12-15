import os
import requests
import json
import uuid
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
 
load_dotenv()

app = FastAPI(title="BI Consumer 2 - Health Check")
 
DORIS_HOST = os.getenv("DORIS_FE_HOST")
DORIS_PORT = os.getenv("DORIS_HTTP_PORT")
DB = os.getenv("DORIS_DB_GENERAL", "GENERAL")
USER = os.getenv("DORIS_USER")
PASSWORD = os.getenv("DORIS_PASSWORD")
  
def get_cluster_health():
    """
    Consulta el estado de los nodos Backend mediante la API administrativa HTTP.
    Equivalente a: SHOW PROC '/backends';
    """
    url = f"http://{DORIS_HOST}:{DORIS_PORT}/api/show_proc?path=/backends"
    
    try:
        response = requests.get(url, auth=(USER, PASSWORD))
        
        if response.status_code != 200:
            raise Exception(f"Error conectando al Frontend: {response.text}")
            
        data = response.json() 
        headers = data.get("column_names", [])
        rows = data.get("rows", [])
        
        try:
            idx_host = headers.index("Host")
            idx_alive = headers.index("Alive")
            idx_be_port = headers.index("HttpPort")
        except ValueError:
            return {"status": "WARN", "message": "No se pudo interpretar la respuesta de Doris", "raw": data}

        backends_status = []
        alive_count = 0

        for row in rows:
            is_alive = row[idx_alive] == "true"
            if is_alive:
                alive_count += 1
            
            backends_status.append({
                "ip": row[idx_host],
                "http_port": row[idx_be_port],
                "alive": is_alive
            })

        status = "UP" if alive_count > 0 else "DOWN"
        
        return {
            "status": status,
            "message": f"Cluster operativo. {alive_count}/{len(rows)} backends activos.",
            "frontend_ip": DORIS_HOST,
            "backends": backends_status,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fallo en chequeo general: {str(e)}")

def run_stream_load_check(table_name):
    """
    Ejecuta la prueba de escritura transaccional (2PC) sobre una tabla específica.
    """
    load_url = f"http://{DORIS_HOST}:{DORIS_PORT}/api/{DB}/{table_name}/_stream_load"
    
    headers = {
        "format": "json",
        "strip_outer_array": "true",
        "Expect": "100-continue",
        "label": f"healthcheck_{str(uuid.uuid4())}",
        "two_phase_commit": "true" 
    }
 
    data = [{
        "uid": str(uuid.uuid4()),
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
        "client_uid": "HEALTHCHECK_API",
        "project_uid": "HEALTHCHECK_API",
        "document": "9999999",
        "observation": "API Healthcheck - Should be aborted",
        "campaign_id": 0
    }]

    log_pasos = []
    
    try: 
        response = requests.put(
            load_url,
            data=json.dumps(data),
            headers=headers,
            auth=(USER, PASSWORD),
            allow_redirects=False 
        )

        final_response = response

        if response.status_code == 307:
            redirect_url = response.headers.get("Location")
            log_pasos.append(f"Redirigido a Backend: {redirect_url}")
            
            final_response = requests.put(
                redirect_url,
                data=json.dumps(data),
                headers=headers,
                auth=(USER, PASSWORD), 
                allow_redirects=True
            )

        try:
            resp_dict = final_response.json()
        except:
            raise HTTPException(status_code=502, detail=f"Respuesta inválida de Doris: {final_response.text}")

        if resp_dict.get("Status") == "Success":
            txn_id = resp_dict.get("TxnId")
            
            abort_url = f"http://{DORIS_HOST}:{DORIS_PORT}/api/{DB}/_stream_load_2pc"
            abort_headers = {"txn_operation": "abort", "txn_id": str(txn_id)}
            
            requests.put(abort_url, headers=abort_headers, auth=(USER, PASSWORD))
            
            return {
                "status": "UP",
                "check_type": "write_simulation",
                "table": table_name,
                "message": "Escritura validada correctamente (Rollback ejecutado).",
                "doris_txn_id": txn_id,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return JSONResponse(
                status_code=500,
                content={
                    "status": "DOWN",
                    "message": "Fallo en validación de escritura",
                    "doris_error": resp_dict.get("Message"),
                    "url_log": resp_dict.get("ErrorURL")
                }
            )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "ERROR", "message": str(e), "trace": log_pasos}
        )
 

@app.get("/health/doris/{target}")
def check_doris_status(target: str):
    """
    Ruta única para validar estado.
    - /health/doris/general -> Devuelve estado de los Backends.
    - /health/doris/mibotair_results -> Valida escritura en esa tabla.
    - /health/doris/voicebot_results -> Valida escritura en esa tabla.
    """
    
    if not all([DORIS_HOST, DORIS_PORT, USER, PASSWORD]):
        raise HTTPException(status_code=500, detail="Faltan variables de entorno (.env)")
 
    if target.lower() == "general":
        return get_cluster_health()
    
    elif target in ["mibotair_results", "voicebot_results"]:
        return run_stream_load_check(target)
    
    else:
        result = get_cluster_health()
        result["note"] = f"El target '{target}' no es una tabla conocida, mostrando estado general."
        return result

if __name__ == "__main__":
    import uvicorn
    http_port = int(os.getenv("API_PORT",80))
    api_host = os.getenv("API_HOST", "0.0.0.0")
    uvicorn.run(app, host=api_host, port=http_port)