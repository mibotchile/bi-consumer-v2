import os
import sys
import json
import logging
import time
import base64
from datetime import datetime
from logging.handlers import WatchedFileHandler 
import uuid
import pika
import requests 
from dotenv import load_dotenv
import pytz
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
 
load_dotenv() 

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE_MIBOTAIR_DORIS")

DORIS_HOST = os.getenv("DORIS_FE_HOST", "127.0.0.1") 
DORIS_HTTP_PORT = os.getenv("DORIS_HTTP_PORT", "8030") 
DORIS_DB = os.getenv("DORIS_DB_GENERAL", "GENERAL")
DORIS_TABLE = "mibotair_results"
DORIS_USER = os.getenv("DORIS_USER", "root")
DORIS_PASSWORD = os.getenv("DORIS_PASSWORD", "")

TARGET_ORIGIN = os.getenv("TARGET_ORIGIN_MIBOTAIR")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000)) 
BATCH_MAX_SECONDS = int(os.getenv("BATCH_MAX_SECONDS", 10))

DEFAULT_TIMEZONE = "America/Santiago"

try:
    CHILE_TZ = ZoneInfo("America/Santiago")
    UTC_TZ = ZoneInfo("UTC")
except ZoneInfoNotFoundError:
    print("Error: tzdata no instalado.")
    sys.exit(1)

if not all([RABBITMQ_URL, RABBITMQ_QUEUE, TARGET_ORIGIN, DORIS_HOST]):
    print("Error: Faltan variables de entorno.")
    sys.exit(1)

# --- Logging ---
def setup_logging():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logger = logging.getLogger("consumer_doris_mibotair")
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = WatchedFileHandler(os.path.join(log_dir, "consumer_doris.log"))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
        
    return logger

logger = setup_logging()

# --- Custom JSON Encoder para Fechas ---
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, datetime.date)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

# --- Doris Stream Loader ---
class DorisStreamLoader:
    """
    Acumula registros y los envía a Apache Doris usando HTTP Stream Load.
    Esta es la forma más eficiente de insertar datos en Doris.
    """
    def __init__(self, host, port, db, table, user, password, batch_size, max_seconds):
        self.load_url = f"http://{host}:{port}/api/{db}/{table}/_stream_load"
        self.user = user
        self.password = password
        self.batch_size = batch_size
        self.max_seconds = max_seconds
        self.buffer = []
        self.last_flush_time = time.time()
        
        # Preparamos los headers base
        self.headers = {
            "format": "json",
            "strip_outer_array": "true",
            "Expect": "100-continue"
        }

    def add(self, record):
        self.buffer.append(record)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush_if_needed(self):
        if self.buffer and (time.time() - self.last_flush_time > self.max_seconds):
            self.flush()

    def flush(self):
        if not self.buffer:
            return True

        record_count = len(self.buffer) 
        try: 
            json_payload = json.dumps(self.buffer, cls=DateTimeEncoder)
            label = f"mibotair_{int(time.time()*1000)}_{record_count}"
            
            headers = self.headers.copy()
            headers["label"] = label 

            response = requests.put(
                self.load_url,
                data=json_payload,
                headers=headers,
                auth=(self.user, self.password),
                allow_redirects=False
            )

            final_response = response

            if response.status_code == 307:
                backend_url = response.headers.get("Location")
                
                final_response = requests.put(
                    backend_url,
                    data=json_payload,
                    headers=headers,
                    auth=(self.user, self.password),
                    allow_redirects=True
                )
            
            try:
                resp_dict = final_response.json()
            except:
                logger.error(f"Error parseando respuesta Doris: {final_response.text}")
                return False

            if resp_dict.get("Status") == "Success":
                logger.info(f"Doris Load OK. Label: {label}. Loaded: {resp_dict.get('NumberLoadedRows')}")
                self.buffer.clear()
                self.last_flush_time = time.time()
                return True
            else:
                logger.error(f"Doris Load Failed: {resp_dict}")
                logger.error(f"Error URL: {resp_dict.get('ErrorURL')}")
                return False

        except Exception as e:
            logger.error(f"Error crítico enviando a Doris: {e}")
            return False # Esto hará que RabbitMQ haga Nack y reintente

    def close(self):
        self.flush()

# --- Callbacks RabbitMQ ---
def create_callback(loader):
    def callback(ch, method, properties, body):
         
        def clean_extra_data(raw_value): 
            if not raw_value: return None 
            while isinstance(raw_value, str):
                try:
                    raw_value = json.loads(raw_value)
                except json.JSONDecodeError: 
                    return None
             
            if isinstance(raw_value, (dict, list)): 
                return json.dumps(raw_value)
            return None

        try:
            message = json.loads(body)
            data = message.get("data", {})  
            meta = message.get("meta", {})
  
            if meta.get("origin") != TARGET_ORIGIN:
                logger.warning(f"Mensaje ignorado (origen no coincide): {meta.get('origin')}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # ... Procesamiento de Fechas (Idéntico a tu lógica) ...
            timezone = data.get("timezone", DEFAULT_TIMEZONE)
            if not timezone:
                logger.warning(f"Zona horaria no especificada, usando {DEFAULT_TIMEZONE}") 
                timezone = DEFAULT_TIMEZONE
            
            try: 
                native_datetime_str = f"{data.get('fecha')} {data.get('hora')}"
                native_dt = datetime.strptime(native_datetime_str, "%Y-%m-%d %H:%M:%S") 
                dt_object_aware = native_dt.replace(tzinfo=CHILE_TZ)
                date_utc = dt_object_aware.astimezone(ZoneInfo("UTC"))
                date_local_native = native_dt 
            
            except (ValueError, pytz.UnknownTimeZoneError) as e:
                logger.error(f"Error al parsear fechas o zona horaria: {e}. Mensaje: {body}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return  
 
            promise_date_str = data.get("fecha_compromiso")
            promise_date = None
            if promise_date_str:
                try:
                    promise_date = datetime.strptime(promise_date_str, "%Y-%m-%d")
                except (ValueError, TypeError):
                    logger.error(f"Formato de fecha de compromiso inválido: {promise_date_str}")
            
            try:
                native_dt = datetime.strptime(native_datetime_str, "%Y-%m-%d %H:%M:%S") 
            except:
                native_dt = datetime.now() # Fallback

            cleaned_extra_data = clean_extra_data(data.get("campos_adicionales"))
            n1_value = data.get("n1")
            n2_value = data.get("n2")
            n3_value = data.get("n3")
            record_to_insert = {
                "uid": str(uuid.uuid4()),
                "campaign_id": data.get("id"),
                "campaign_name": data.get("nombre"),
                "document": data.get("rut"),
                "phone": data.get("fono"),
                "date": date_local_native,
                "date_utc": date_utc,
                "timezone": timezone,
                "management": data.get("gestion"),
                "sub_management": data.get("subgestion"),
                "management_id": data.get("id_gestion"),
                "weight": data.get("peso") or -9999,
                "promise_date": promise_date,
                "promise_amount": data.get("monto_compromiso") or 0, 
                "observation": data.get("observacion")[:255] if data.get("observacion") else "",
                "project_uid": data.get("idproyect"),
                "client_uid": data.get("idcliente"),
                "duration": data.get("duration") or 0,
                "telephony_id": data.get("id_telefonia") or 0,
                "extra_data": cleaned_extra_data,
                "uniqueid": data.get("uniqueid"),
                "url": data.get("url_record_bot"),
                "id_record": data.get("registro") or 0,
                "n1": n1_value[:100] if n1_value else "",
                "n2": n2_value[:100] if n2_value else "",
                "n3": n3_value[:100] if n3_value else "",
                "agent_name": data.get("nombre_agente"),
                "agent_email": data.get("correo_agente"),
                "created_at": datetime.now()
            } 
             
            loader.add(record_to_insert) 
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError:
            logger.error("JSON inválido")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
    return callback

def main():
    # Instanciar el Loader HTTP de Doris
    loader = DorisStreamLoader(
        host=DORIS_HOST,
        port=DORIS_HTTP_PORT,
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
        # Prefetch = Batch size para tener memoria suficiente
        channel.basic_qos(prefetch_count=BATCH_SIZE) 

        on_message_callback = create_callback(loader)
        channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=on_message_callback, auto_ack=False)

        logger.info(f"Consumidor Doris iniciado en cola {RABBITMQ_QUEUE}")
        
        while True: 
            # Process events es no bloqueante por el time_limit, permite hacer flush por tiempo
            connection.process_data_events(time_limit=1)
            loader.flush_if_needed()

    except Exception as e:
        logger.error(f"Error Fatal: {e}")
    finally:
        if connection and connection.is_open:
            connection.close()
        loader.close()

if __name__ == '__main__':
    main()