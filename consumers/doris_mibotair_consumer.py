import os
import sys
import json
import logging
import time
import uuid
from datetime import datetime, date
from logging.handlers import WatchedFileHandler, TimedRotatingFileHandler

import pika
from dotenv import load_dotenv
import pytz
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from doris_stream_loader import DorisStreamLoader
 
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
BATCH_SIZE = int(os.getenv("BATCH_SIZE_MIBOTAIR", 500))
BATCH_MAX_SECONDS = int(os.getenv("BATCH_MAX_SECONDS_MIBOTAIR", 10))
RABBITMQ_PREFETCH = int(os.getenv("RABBITMQ_PREFETCH_MIBOTAIR", str(BATCH_SIZE)))

DORIS_STREAM_LOAD_TIMEOUT = int(os.getenv("DORIS_STREAM_LOAD_TIMEOUT", "60"))
DORIS_CONNECT_TIMEOUT = float(os.getenv("DORIS_CONNECT_TIMEOUT", "3.05"))
DORIS_READ_TIMEOUT = float(os.getenv("DORIS_READ_TIMEOUT", "60"))
DORIS_RETRY_TOTAL = int(os.getenv("DORIS_RETRY_TOTAL", "5"))
DORIS_RETRY_BACKOFF = float(os.getenv("DORIS_RETRY_BACKOFF", "0.5"))
DORIS_RETRY_STATUSES = [
    int(x)
    for x in os.getenv("DORIS_RETRY_STATUSES", "408,425,429,500,502,503,504").split(",")
]
DORIS_POOL_MAXSIZE = int(os.getenv("DORIS_POOL_MAXSIZE", "10"))
DORIS_FLUSH_WORKERS = int(os.getenv("DORIS_FLUSH_WORKERS", "2"))
DORIS_MAX_IN_FLIGHT = int(os.getenv("DORIS_MAX_IN_FLIGHT", "4"))
DORIS_INFLIGHT_WAIT_SEC = float(os.getenv("DORIS_INFLIGHT_WAIT_SEC", "2"))
DORIS_ENABLE_GZIP = os.getenv("DORIS_ENABLE_GZIP", "false").lower() in ("1", "true", "yes")
DORIS_MAX_FILTER_RATIO = os.getenv("DORIS_MAX_FILTER_RATIO") or None
DORIS_PAUSE_ON_ERROR_SEC = float(os.getenv("DORIS_PAUSE_ON_ERROR_SEC", "0"))

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

# --- LOGGERS ---
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

def setup_raw_data_logger():
    """
    SEGURIDAD NIVEL 1: La Caja Negra.
    Guarda el mensaje crudo de RabbitMQ en un archivo diario.
    """
    raw_logger = logging.getLogger("raw_data_saver_mibotair")
    raw_logger.setLevel(logging.INFO)
    raw_logger.propagate = False 
    
    os.makedirs("raw_data_archive", exist_ok=True)
    
    handler = TimedRotatingFileHandler(
        "raw_data_archive/mibotair_backup.jsonl",
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

# --- Custom JSON Encoder para Fechas ---
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

# --- Callbacks RabbitMQ ---
def create_callback(loader):
    def callback(ch, method, properties, body):
         
        def clean_extra_data(raw_value): 
            if not raw_value: return None 
            
            current_value = raw_value
            while isinstance(current_value, str):
                try:
                    current_value = json.loads(current_value)
                except json.JSONDecodeError: 
                    return None
             
            if isinstance(current_value, (dict, list)): 
                return current_value # Devolver objeto puro para Doris
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
            
            try: 
                native_datetime_str = f"{data.get('fecha')} {data.get('hora')}"
                native_dt = datetime.strptime(native_datetime_str, "%Y-%m-%d %H:%M:%S") 
                dt_object_aware = native_dt.replace(tzinfo=CHILE_TZ)
                date_utc = dt_object_aware.astimezone(ZoneInfo("UTC"))
                date_local_native = native_dt 
            
            except (ValueError, pytz.UnknownTimeZoneError) as e:
                logger.error(f"Error al parsear fechas: {e}. Msg: {body}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return  
 
            promise_date_str = data.get("fecha_compromiso")
            promise_date = None
            if promise_date_str:
                try:
                    promise_date = datetime.strptime(promise_date_str, "%Y-%m-%d")
                except (ValueError, TypeError):
                    logger.warning(f"Fecha compromiso inválida: {promise_date_str}")
            
            try:
                native_dt = datetime.strptime(native_datetime_str, "%Y-%m-%d %H:%M:%S") 
            except:
                native_dt = datetime.now()

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
    loader = DorisStreamLoader(
        host=DORIS_HOST,
        port=DORIS_HTTP_PORT,
        db=DORIS_DB,
        table=DORIS_TABLE,
        user=DORIS_USER,
        password=DORIS_PASSWORD,
        batch_size=BATCH_SIZE,
        max_seconds=BATCH_MAX_SECONDS,
        encoder_cls=DateTimeEncoder,
        logger=logger,
        label_prefix="mibotair",
        stream_load_timeout=DORIS_STREAM_LOAD_TIMEOUT,
        connect_timeout=DORIS_CONNECT_TIMEOUT,
        read_timeout=DORIS_READ_TIMEOUT,
        retry_total=DORIS_RETRY_TOTAL,
        retry_backoff=DORIS_RETRY_BACKOFF,
        retry_statuses=DORIS_RETRY_STATUSES,
        pool_maxsize=DORIS_POOL_MAXSIZE,
        flush_workers=DORIS_FLUSH_WORKERS,
        max_in_flight=DORIS_MAX_IN_FLIGHT,
        inflight_wait_sec=DORIS_INFLIGHT_WAIT_SEC,
        enable_gzip=DORIS_ENABLE_GZIP,
        max_filter_ratio=DORIS_MAX_FILTER_RATIO,
        pause_on_error_sec=DORIS_PAUSE_ON_ERROR_SEC,
    )
    
    connection = None
    try:
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.basic_qos(prefetch_count=RABBITMQ_PREFETCH)

        on_message_callback = create_callback(loader)
        channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=on_message_callback, auto_ack=False)

        logger.info(f"Consumidor Doris MibotAir iniciado. Cola: {RABBITMQ_QUEUE}")
        print(f"\n✅ Consumidor MibotAir activo.")
        
        while True:
            if loader.should_pause():
                time.sleep(1)
                continue
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
