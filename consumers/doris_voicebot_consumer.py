import os
import sys
import json
import logging
import time
import base64
import uuid
import requests
from datetime import datetime, date
from logging.handlers import WatchedFileHandler, TimedRotatingFileHandler
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pika
from dotenv import load_dotenv
import pytz 

load_dotenv()  
RABBITMQ_URL = os.getenv("RABBITMQ_URL")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE_VOICEBOT_DORIS")

DORIS_HOST = os.getenv("DORIS_FE_HOST")
DORIS_PORT = os.getenv("DORIS_HTTP_PORT", "8030")
DORIS_DB = os.getenv("DORIS_DB_GENERAL", "GENERAL") 
DORIS_TABLE = "voicebot_results"
DORIS_USER = os.getenv("DORIS_USER")
DORIS_PASSWORD = os.getenv("DORIS_PASSWORD")

TARGET_ORIGIN = os.getenv("TARGET_ORIGIN_VOICEBOT")
BATCH_SIZE = int(os.getenv("BATCH_SIZE_VOICEBOT", 500))
BATCH_MAX_SECONDS = int(os.getenv("BATCH_MAX_SECONDS_VOICEBOT", 5))

DEFAULT_TIMEZONE = "America/Lima"

if not all([RABBITMQ_URL, RABBITMQ_QUEUE, TARGET_ORIGIN, DORIS_HOST, DORIS_USER]):
    print("Error: Faltan variables de entorno esenciales (.env)")
    sys.exit(1)

# --- LOGGERS ---
def setup_logging():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logger = logging.getLogger("consumer_doris_voicebot")
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    file_handler = WatchedFileHandler(os.path.join(log_dir, "consumer_voicebot.log"))
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
    """
    raw_logger = logging.getLogger("raw_data_saver_voicebot")
    raw_logger.setLevel(logging.INFO)
    raw_logger.propagate = False 
    
    os.makedirs("raw_data_archive", exist_ok=True)
    
    handler = TimedRotatingFileHandler(
        "raw_data_archive/voicebot_backup.jsonl",
        when="midnight",
        interval=1,
        backupCount=90,
        encoding="utf-8"
    )
    
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    raw_logger.addHandler(handler)
    return raw_logger

logger = setup_logging()
raw_data_logger = setup_raw_data_logger()

# --- Encoder para Fechas en JSON ---
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

# --- DORIS STREAM LOADER BLINDADO ---
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
        Si Doris rechaza el dato, se guarda aquí.
        """
        try:
            filename = f"failed_loads/voicebot_{int(time.time())}_{uuid.uuid4()}.json"
            os.makedirs("failed_loads", exist_ok=True)
            with open(filename, "w", encoding="utf-8") as f:
                f.write(json_payload)
            logger.error(f"⚠️ FALLO DORIS. DATOS SALVADOS EN DISCO: {filename}. Razón: {reason}")
            return True
        except Exception as e:
            logger.critical(f"🔥 FALLO TOTAL (Doris y Disco): {e}")
            return False

    def flush(self):
        if not self.buffer:
            return True

        json_payload = ""
        try:
            json_payload = json.dumps(self.buffer, cls=DateTimeEncoder)
            label = f"voicebot_{int(time.time()*1000)}_{len(self.buffer)}"
            headers = self.headers.copy()
            headers["label"] = label

            # Usamos session.put con timeout
            response = self.session.put(
                self.load_url,
                data=json_payload,
                headers=headers,
                allow_redirects=False,
                timeout=60
            )

            final_response = response

            if response.status_code == 307:
                final_response = self.session.put(
                    response.headers.get("Location"),
                    data=json_payload,
                    headers=headers,
                    allow_redirects=True,
                    timeout=60
                )
            
            resp_dict = final_response.json()

            if resp_dict.get("Status") == "Success":
                logger.info(f"Doris Load OK. Label: {label}. Loaded: {resp_dict.get('NumberLoadedRows')}")
                self.buffer.clear()
                self.last_flush_time = time.time()
                return True
            else:
                # Falló Doris (lógica o tabla rota) -> Guardar en disco
                error_msg = resp_dict.get('Message', 'Unknown Error')
                self.save_to_disk(json_payload, error_msg)
                
                self.buffer.clear()
                self.last_flush_time = time.time()
                return True # Mentimos a RabbitMQ

        except Exception as e:
            # Falló Red/Python -> Guardar en disco
            logger.error(f"Error de conexión/código: {e}")
            self.save_to_disk(json_payload, str(e))
            
            self.buffer.clear()
            self.last_flush_time = time.time()
            return True # Mentimos a RabbitMQ

    def close(self):
        self.flush()

# --- Callbacks RabbitMQ ---
def create_callback(loader):
    def callback(ch, method, properties, body):
        
        def clean_json(raw_value):
            if not raw_value: return None
            
            current_value = raw_value
            while isinstance(current_value, str):
                try:
                    current_value = json.loads(current_value)
                except json.JSONDecodeError:
                    return None
            
            if isinstance(current_value, (dict, list)):
                return current_value
            return None 
        
        try:
            # A) SEGURIDAD NIVEL 1: Guardado inmediato del RAW
            try:
                raw_data_logger.info(body.decode('utf-8'))
            except Exception as e:
                logger.error(f"No se pudo escribir en raw log: {e}")

            # B) Procesamiento Normal
            message = json.loads(body)
            data = message.get("data", {})
            meta = message.get("meta", {})
  
            if meta.get("origin") != TARGET_ORIGIN:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            timezone = data.get("timezone", DEFAULT_TIMEZONE)
            if not timezone:
                timezone = DEFAULT_TIMEZONE
            
            date_str_with_offset = data.get("time") 
            if not date_str_with_offset:
                logger.error(f"Mensaje descartado: falta 'time'. Body: {body}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            try:  
                dt_object_aware = datetime.fromisoformat(date_str_with_offset) 
                date_utc = dt_object_aware.astimezone(pytz.utc) 
                target_timezone = pytz.timezone(timezone)
                dt_in_target_tz = dt_object_aware.astimezone(target_timezone)
                date_local_naive = dt_in_target_tz.replace(tzinfo=None) 

                if date_local_naive.year < 2020:
                    logger.warning(f"Fecha anómala detectada ({date_local_naive}). Usando fecha actual.")
                    date_local_naive = datetime.now()
                
            except (ValueError, pytz.UnknownTimeZoneError) as e:
                logger.error(f"Error fechas/tz: {e}. Msg: {body}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
     
            date_str = data.get("fecha")
            time_str = data.get("hora") 
            if date_str and time_str:
                try:
                    datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass
 
            promise_date_str = data.get("fecha_compromiso")
            promise_date = None
            if promise_date_str and promise_date_str != "Invalid date":
                try:
                    promise_date = datetime.strptime(promise_date_str, "%Y-%m-%d")
                except (ValueError, TypeError):
                    logger.warning(f"Fecha compromiso inválida: {promise_date_str}")

            record_to_insert = {
                "uid": str(uuid.uuid4()),
                "client_uid": data.get("idcliente"),
                "project_uid": data.get("idproyect"),
                "date": date_local_naive, 
                "campaign_id": data.get("id"),
                "campaign_name": data.get("nombre"),
                "document": data.get("rut"),
                "phone": data.get("fono"),
                "management": data.get("gestion"),
                "sub_management": data.get("subgestion"),
                "weight": data.get("peso"),
                "promise_date": promise_date, 
                "interest": data.get("interes"),
                "promise": data.get("compromiso"),
                "observation": clean_json(data.get("observacion")),
                "interactions": clean_json(data.get("interactions", [])),
                "responses": clean_json(data.get("responses", [])),
                "duration": data.get("duration"),
                "uniqueid": data.get("uniqueid"),
                "telephony_id": data.get("id_telefonia"),
                "bot_extension": data.get("idBot"),
                "url": data.get("url_record_bot"),
                "id_record": data.get("registro"),
                "date_utc": date_utc,
                "timezone": timezone,
                "created_at": datetime.now()
            } 

            loader.add(record_to_insert)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError:
            logger.error("JSON inválido.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error procesando voz: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
    return callback

def main():
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

        on_message_callback = create_callback(loader)
        channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=on_message_callback, auto_ack=False)

        logger.info(f"Consumidor VoiceBot Doris iniciado. Cola: {RABBITMQ_QUEUE}")
        print(f"\n✅ Consumidor VoiceBot Doris activo.")
        
        while True: 
            connection.process_data_events(time_limit=1)
            loader.flush_if_needed()

    except KeyboardInterrupt: 
        print("\n\n🛑 Detención solicitada por usuario...")

    except Exception as e:
        logger.error(f"Error Fatal: {e}")
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