import os
import sys
import json
import logging
import time
import base64
import uuid
import requests
from datetime import datetime
from logging.handlers import WatchedFileHandler, TimedRotatingFileHandler
from requests.adapters import HTTPAdapter 
from urllib3.util.retry import Retry 

import pika
from dotenv import load_dotenv
import pytz

load_dotenv() 
 
RABBITMQ_URL = os.getenv("RABBITMQ_URL")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE_EMAIL_DORIS")
 
DORIS_HOST = os.getenv("DORIS_FE_HOST")
DORIS_PORT = os.getenv("DORIS_HTTP_PORT", "8030")
DORIS_DB = os.getenv("DORIS_DB_GENERAL", "GENERAL")
DORIS_TABLE = "email_results"
DORIS_USER = os.getenv("DORIS_USER")
DORIS_PASSWORD = os.getenv("DORIS_PASSWORD")
 
FIREBASE_EMAIL = os.getenv("FIREBASE_EMAIL")
FIREBASE_PASSWORD = os.getenv("FIREBASE_PASSWORD")
FIREBASE_KEY = os.getenv("FIREBASE_KEY")
AIM_API_URL = "https://apiaim.mibot.cl/v3/clients"

TARGET_ORIGIN = os.getenv("TARGET_ORIGIN_EMAIL", "Email")
BATCH_SIZE = int(os.getenv("BATCH_SIZE_EMAIL", 500))
BATCH_MAX_SECONDS = int(os.getenv("BATCH_MAX_SECONDS_EMAIL", 5))

# --- LOGGERS ---
def setup_logging():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logger = logging.getLogger("consumer_doris_email")
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    file_handler = WatchedFileHandler(os.path.join(log_dir, "consumer_email.log"))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger

def setup_raw_data_logger():
    """
    SEGURIDAD NIVEL 1: La Caja Negra.
    Guarda el mensaje crudo de RabbitMQ en un archivo diario.
    Si Doris explota y hay que truncar, se recupera desde aquí.
    """
    raw_logger = logging.getLogger("raw_data_saver_email")
    raw_logger.setLevel(logging.INFO)
    raw_logger.propagate = False # No mostrar en consola
    
    os.makedirs("raw_data_archive", exist_ok=True)
    
    # Rota cada medianoche, guarda 31 días de historia
    handler = TimedRotatingFileHandler(
        "raw_data_archive/email_backup.jsonl",
        when="midnight",
        interval=1,
        backupCount=31,
        encoding="utf-8"
    )
    
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    raw_logger.addHandler(handler)
    return raw_logger

logger = setup_logging()
raw_data_logger = setup_raw_data_logger()
 
class ProjectCache:
    """
    Descarga la estructura de Clientes/Cuentas/Proyectos al inicio
    """
    def __init__(self):
        self.cache = {} 
        self.load_data()

    def get_firebase_token(self):
        url = f"https://www.googleapis.com/identitytoolkit/v3/relyingparty/verifyPassword?key={FIREBASE_KEY}"
        payload = {
            "email": FIREBASE_EMAIL,
            "password": FIREBASE_PASSWORD,
            "returnSecureToken": True
        }
        try:
            resp = requests.post(url, json=payload)
            resp.raise_for_status()
            return resp.json().get("idToken")
        except Exception as e:
            logger.critical(f"Error obteniendo token Firebase: {e}")
            sys.exit(1)

    def load_data(self):
        logger.info("Cargando caché de proyectos desde AIM...")
        token = self.get_firebase_token()
        
        headers = {
            "Authorization": f"Bearer {token}",
            "auth-domain": "mibot-222814.firebaseapp.com",
            "mibot_session": '{"project_uid":"","client_uid":""}' 
        }
        
        try: 
            requests.packages.urllib3.disable_warnings()
            resp = requests.get(AIM_API_URL, headers=headers, verify=False)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            
            count = 0
            for client in data:
                c_uid = client.get("uid")
                billing_accounts = client.get("billing_accounts", [])
                
                if not billing_accounts: continue
                
                for account in billing_accounts:
                    projects = account.get("projects", [])
                    for project in projects:
                        p_uid = project.get("uid")
                        p_tz = project.get("timezone", "UTC") 
                        
                        self.cache[p_uid] = {
                            "client_uid": c_uid,
                            "timezone": p_tz
                        }
                        count += 1
            
            logger.info(f"Caché cargado exitosamente. {count} proyectos indexados.")
            
        except Exception as e:
            logger.critical(f"Error cargando datos de AIM: {e}")
            sys.exit(1)

    def get_info(self, project_uid):
        return self.cache.get(project_uid, {"client_uid": "UNKNOWN", "timezone": "UTC"})

# --- DORIS STREAM LOADER BLINDADO ---
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

class DorisStreamLoader:
    def __init__(self, host, port, db, table, user, password, batch_size, max_seconds):
        self.load_url = f"http://{host}:{port}/api/{db}/{table}/_stream_load"
        self.user = user
        self.password = password
        self.batch_size = batch_size
        self.max_seconds = max_seconds
        self.buffer = []
        self.last_flush_time = time.time()
        
        # --- SEGURIDAD NIVEL 3: Red Robusta ---
        self.session = requests.Session()
        # Reintentar 3 veces si hay error 500, 502, etc. Esperar 1s, 2s, 4s...
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        
        # Auth Pre-calculada
        auth_str = f"{self.user}:{self.password}"
        b64_auth = base64.b64encode(auth_str.encode()).decode()

        self.headers = {
            "format": "json",
            "strip_outer_array": "true",
            "Expect": "100-continue",
            "Authorization": f"Basic {b64_auth}",
            "enable_proxy": "true" 
        }

    def add(self, record):
        self.buffer.append(record)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush_if_needed(self):
        if self.buffer and (time.time() - self.last_flush_time > self.max_seconds):
            self.flush()

    def save_to_disk(self, json_payload, reason):
        """
        SEGURIDAD NIVEL 2: El Salvavidas.
        Si Doris rechaza el dato, se guarda aquí para reinyectar luego.
        """
        try:
            filename = f"failed_loads/email_{int(time.time())}_{uuid.uuid4()}.json"
            os.makedirs("failed_loads", exist_ok=True)
            with open(filename, "w", encoding="utf-8") as f:
                f.write(json_payload)
            logger.error(f"⚠️ FALLO DORIS. DATOS SALVADOS EN DISCO: {filename}. Razón: {reason}")
            return True
        except Exception as e:
            logger.critical(f"🔥 FALLO TOTAL (Doris y Disco): {e}")
            return False

    def flush(self):
        if not self.buffer: return True
        
        json_payload = ""
        try:
            json_payload = json.dumps(self.buffer, cls=DateTimeEncoder)
            label = f"email_{int(time.time()*1000)}_{len(self.buffer)}"
            headers = self.headers.copy()
            headers["label"] = label

            # Usamos session.put con timeout
            response = self.session.put(
                self.load_url, data=json_payload, headers=headers,
                allow_redirects=False, timeout=60
            )

            final_response = response

            # Manejo manual de redirección si enable_proxy es ignorado
            if response.status_code == 307:
                final_response = self.session.put(
                    response.headers.get("Location"),
                    data=json_payload, headers=headers,
                    allow_redirects=True, timeout=60
                )
            
            resp_dict = final_response.json()

            if resp_dict.get("Status") == "Success":
                logger.info(f"Doris Load OK. Label: {label}. Rows: {resp_dict.get('NumberLoadedRows')}")
                self.buffer.clear()
                self.last_flush_time = time.time()
                return True
            else:
                # Falló Doris (lógica o tabla rota) -> Guardar en disco
                error_msg = resp_dict.get('Message', 'Unknown Error')
                self.save_to_disk(json_payload, error_msg)
                
                self.buffer.clear()
                self.last_flush_time = time.time()
                return True # Mentimos a RabbitMQ porque ya está en disco

        except Exception as e:
            # Falló Red/Python -> Guardar en disco
            logger.error(f"Error de conexión/código: {e}")
            self.save_to_disk(json_payload, str(e))
            
            self.buffer.clear()
            self.last_flush_time = time.time()
            return True # Mentimos a RabbitMQ porque ya está en disco
            
    def close(self):
        self.flush()

# --- CALLBACK DE RABBITMQ ---
def create_callback(loader, project_cache):
    EVENT_WEIGHTS = {
        "bounce": 0,
        "processed": -2,
        "deferred": 1,
        "delivered": 2,
        "open": 3,
        "click": 4,
        "spamreport": -1
    }

    def callback(ch, method, properties, body):
        try:
            # A) SEGURIDAD NIVEL 1: Guardado inmediato del RAW
            try:
                raw_data_logger.info(body.decode('utf-8'))
            except Exception as e:
                logger.error(f"No se pudo escribir en raw log: {e}")

            # B) Procesamiento Normal
            message = json.loads(body)
            meta = message.get("meta", {})
            
            if meta.get("origin") != TARGET_ORIGIN:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            root_data = message.get("data", {})
            detail_data = root_data.get("data", {})
            
            project_uid = root_data.get("project")
            
            project_info = project_cache.get_info(project_uid)
            client_uid = project_info["client_uid"]
            timezone_str = project_info["timezone"]
            
            ts_value = detail_data.get("timestamp")
            
            date_utc = None
            date_local = None
            
            if ts_value:
                date_utc = datetime.fromtimestamp(ts_value, pytz.utc)
                try:
                    target_tz = pytz.timezone(timezone_str)
                    date_local = date_utc.astimezone(target_tz).replace(tzinfo=None)
                except:
                    date_local = date_utc.replace(tzinfo=None)
            else:
                date_utc = datetime.now(pytz.utc)
                date_local = datetime.now()

            event_type = detail_data.get("event")
            weight = EVENT_WEIGHTS.get(event_type, 0) 

            record = {
                "uid": str(uuid.uuid4()),
                "client_uid": client_uid,
                "project_uid": project_uid,
                "date": date_local,
                "campaign_id": root_data.get("campaign_id"),
                "document": root_data.get("document"),
                "email": detail_data.get("email"),
                "event": event_type,
                "weight": weight,
                "response": detail_data.get("response"),
                "ip": detail_data.get("ip"),
                "sg_event_id": detail_data.get("sg_event_id"),
                "sg_message_id": detail_data.get("sg_message_id"),
                "smtp_id": detail_data.get("smtp-id"),
                "tls": detail_data.get("tls"),
                "date_utc": date_utc,
                "timezone": timezone_str,
                "created_at": datetime.now()
            }
            
            loader.add(record)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError:
            logger.error("JSON inválido")
            # Raw logger ya lo guardó, así que podemos descartar
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error procesando email: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
    return callback

def main():
    project_cache = ProjectCache()
    
    loader = DorisStreamLoader(
        host=DORIS_HOST,
        port=DORIS_PORT,
        db=DORIS_DB,
        table=DORIS_TABLE,
        user=DORIS_USER,
        password=DORIS_PASSWORD,
        batch_size=BATCH_SIZE,
        max_seconds=BATCH_MAX_SECONDS
    )
    
    connection = None
    try:
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.basic_qos(prefetch_count=BATCH_SIZE)
 
        on_message_callback = create_callback(loader, project_cache)
        
        channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=on_message_callback, auto_ack=False)

        logger.info(f"Consumidor Email Doris iniciado. Cola: {RABBITMQ_QUEUE}")
        print(f"\n✅ Consumidor EMAIL activo.")
        
        while True: 
            connection.process_data_events(time_limit=1)
            loader.flush_if_needed()

    except KeyboardInterrupt: 
        print("\n\n🛑 Detención solicitada por usuario...")

    except Exception as e:
        logger.critical(f"Error Fatal: {e}")
    finally: 
        print("   -> Cerrando conexiones y vaciando buffers...", end="")
        try:
            if connection and connection.is_open:
                connection.close()
        except:
            pass

        try:
            loader.close()
        except Exception as e:
            logger.warning(f"No se pudo guardar el último lote al salir: {e}")

        print(" [OK]")
        print("👋 Consumidor detenido.") 
        sys.exit(0)

if __name__ == '__main__':
    main()