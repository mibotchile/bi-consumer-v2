import os
import sys
import json
import logging
import time
from datetime import datetime
from logging.handlers import WatchedFileHandler 

import pika
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import pytz
from zoneinfo import ZoneInfo
 
load_dotenv() 
RABBITMQ_URL = os.getenv("RABBITMQ_URL")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE_MIBOTAIR")

# Configuración de PostgreSQL
POSTGRES_DSN = os.getenv("POSTGRES_DSN")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public") 
DB_TABLE_NAME = "mibotair_results" 

# Configuración del Consumidor
TARGET_ORIGIN = os.getenv("TARGET_ORIGIN_MIBOTAIR")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 500)) #default 500
BATCH_MAX_SECONDS = int(os.getenv("BATCH_MAX_SECONDS", 5)) #default 5

DEFAULT_TIMEZONE = "America/Santiago"

try:
    CHILE_TZ = ZoneInfo("America/Santiago")
    UTC_TZ = ZoneInfo("UTC")
except ZoneInfoNotFoundError:
    logger.critical("La base de datos de zonas horarias (tzdata) no está instalada. Ejecuta 'pip install tzdata'.")
    sys.exit(1)

# Validar que las variables esenciales existen
if not all([RABBITMQ_URL, RABBITMQ_QUEUE, POSTGRES_DSN, TARGET_ORIGIN]):
    print("Error: Faltan variables de entorno. Revisa tu archivo .env")
    sys.exit(1)

# --- Configuración del Logging ---
def setup_logging():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logger = logging.getLogger("consumer_logger_mibotair")
    log_level = logging.DEBUG if os.getenv("LOG_DEBUG_ACTIVE") == "true" else logging.INFO
    logger.setLevel(log_level)
     
    if not logger.handlers:
        handler = WatchedFileHandler(
            os.path.join(log_dir, "consumer_mibotair.log")
        )
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        
    return logger

logger = setup_logging()
 
class PostgresBatchInserter:
    """
    Gestiona una conexión persistente a PostgreSQL e inserta registros en lotes.
    Es responsable de conectar, reconectar, acumular registros y escribirlos.
    """
    def __init__(self, dsn, schema, table_name, batch_size, max_seconds):
        self.dsn = dsn
        self.qualified_table_name = sql.Identifier(schema, table_name)
        self.batch_size = batch_size
        self.max_seconds = max_seconds
        self.buffer = []
        self.last_flush_time = time.time()
        self.conn = None
        self.connect()

    def connect(self):
        """Establece o reestablece la conexión a la base de datos."""
        try:
            # Si ya hay una conexión, cerrarla limpiamente antes de abrir una nueva
            if self.conn and not self.conn.closed:
                self.conn.close()
            self.conn = psycopg2.connect(self.dsn)
            self.conn.autocommit = False # Controlamos las transacciones manualmente
            logger.info("Conexión a PostgreSQL establecida/reestablecida exitosamente.")
        except psycopg2.OperationalError as e:
            logger.error(f"No se pudo conectar a PostgreSQL: {e}. Se reintentará más tarde.")
            self.conn = None

    def add(self, record):
        """Añade un registro al buffer y comprueba si debe hacer flush."""
        self.buffer.append(record)
        if len(self.buffer) >= self.batch_size:
            logger.debug(f"Buffer lleno ({len(self.buffer)} registros). Ejecutando flush.")
            self.flush()

    def flush_if_needed(self):
        """Ejecuta un flush si ha pasado el tiempo máximo desde el último."""
        if self.buffer and (time.time() - self.last_flush_time > self.max_seconds):
            logger.debug(f"Tiempo máximo de {self.max_seconds}s excedido. Ejecutando flush con {len(self.buffer)} registros.")
            self.flush()

    def flush(self):
        """Inserta todos los registros del buffer en la base de datos."""
        if not self.buffer:
            return True # No hay nada que hacer

        # Comprobar la conexión antes de intentar escribir
        if not self.conn or self.conn.closed:
            logger.warning("Conexión a DB perdida. Intentando reconectar antes de flush...")
            self.connect()
            if not self.conn:
                logger.error("No se pudo reconectar a la DB. El flush fallará.")
                return False

        columns = self.buffer[0].keys()
        
        insert_query = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(
            table=self.qualified_table_name,
            cols=sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        
        # Convertir la lista de diccionarios a una lista de tuplas para execute_values
        values = [[record.get(col) for col in columns] for record in self.buffer]

        try:
            with self.conn.cursor() as cur:
                execute_values(cur, insert_query, values)
            self.conn.commit()
            logger.info(f"Lote de {len(self.buffer)} registros insertado exitosamente.")
            self.buffer.clear()
            self.last_flush_time = time.time()
            return True
        except psycopg2.Error as e:
            logger.error(f"Error en la inserción por lote: {e}")
            self.conn.rollback()
            return False

    def close(self):
        """Limpia el buffer final y cierra la conexión."""
        logger.info("Cerrando el insertador. Realizando un último flush...")
        self.flush()
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Conexión a PostgreSQL cerrada.")

# --- Lógica del Callback de RabbitMQ ---
def create_callback(inserter):
    """
    Crea y retorna la función callback, dándole acceso al objeto 'inserter'.
    Esto se conoce como una clausura (closure).
    """
    def callback(ch, method, properties, body):
        """
        Función que se ejecuta por cada mensaje. Su única tarea es procesar
        el mensaje y pasárselo al insertador.
        """

        def clean_json(raw_value):
            """
            Limpia y valida el campo de observación.
            Maneja valores nulos, vacíos, y JSON doblemente codificado.
            Retorna un string JSON válido o None.
            """
            if not raw_value:
                return None

            current_value = raw_value
            # Intenta decodificar repetidamente mientras el resultado sea un string
            # para manejar casos como "{\"key\":\"value\"}"
            while isinstance(current_value, str):
                try:
                    current_value = json.loads(current_value)
                except json.JSONDecodeError:
                    # Si no se puede decodificar, es un string simple o malformado.
                    # Como queremos guardar solo JSON válido o NULL, lo tratamos como inválido.
                    return None
            
            # Si después de decodificar tenemos un diccionario o una lista, es JSON válido.
            if isinstance(current_value, (dict, list)):
                # Lo re-codificamos a un string JSON limpio para la inserción.
                # Esto asegura un formato consistente en la BD.
                return json.dumps(current_value)
            
            # Si es cualquier otra cosa (un número, booleano suelto), lo consideramos inválido.
            return None 
        
        try:
            message = json.loads(body)
            data = message.get("data", {})  
            meta = message.get("meta", {})
  
            # --- Aquí va tu lógica de validación y parseo del mensaje ---
            if meta.get("origin") != TARGET_ORIGIN:
                logger.warning(f"Mensaje ignorado (origen no coincide): {meta.get('origin')}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            timezone = data.get("timezone")
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

            cleaned_extra_data = clean_json(data.get("campos_adicionales"))
            n1_value = data.get("n1")
            n2_value = data.get("n2")
            n3_value = data.get("n3")

            record_to_insert = {
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

            #print(record_to_insert)
            print(record_to_insert.get("client_uid"), "|", record_to_insert.get("project_uid"),"|", record_to_insert.get("document"), "|", record_to_insert.get("date"), "|", record_to_insert.get("management_id"),"|", record_to_insert.get("n3"))

            inserter.add(record_to_insert)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError:
            logger.error("Error: Mensaje no es un JSON válido. Descartando.")
            ch.basic_ack(delivery_tag=method.delivery_tag) # No se puede reprocesar
        except Exception as e:
            logger.critical(f"Error inesperado procesando mensaje: {e}", exc_info=True)
            # Nack para que RabbitMQ lo re-encole o lo mande a una Dead Letter Queue
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
    return callback
 
def main():
    """Inicia y gestiona el ciclo de vida del consumidor."""
    inserter = PostgresBatchInserter(
        dsn=POSTGRES_DSN,
        schema=DB_SCHEMA,
        table_name=DB_TABLE_NAME,
        batch_size=BATCH_SIZE,
        max_seconds=BATCH_MAX_SECONDS
    )
    connection = None
    
    try:
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.basic_qos(prefetch_count=BATCH_SIZE) # Esto lo veremos después 

        on_message_callback = create_callback(inserter)
        channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=on_message_callback, auto_ack=False)

        logger.info(f"Consumidor MibotAir listo. Escuchando en '{RABBITMQ_QUEUE}'.")
        print(f"\n✅ Consumidor MibotAir activo, listo para recibir mensajes.")
        print(f"   - Cola: '{RABBITMQ_QUEUE}'")
        print(f"   - Lotes de {BATCH_SIZE} registros o cada {BATCH_MAX_SECONDS} segundos.")
        print("   - Presiona CTRL+C para detener.\n")
        
        while True: 
            connection.process_data_events(time_limit=1)
            inserter.flush_if_needed()

    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"No se pudo conectar a RabbitMQ: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Detención manual solicitada.")
    finally:
        logger.info("Iniciando cierre limpio del consumidor...")
        if connection and connection.is_open:
            logger.info("Cerrando conexión a RabbitMQ...")
            connection.close()
        # El 'finally' en el 'inserter' se encargará de su propio cierre.
        inserter.close()
        logger.info("Consumidor detenido exitosamente.")
        print("\n🛑 Consumidor detenido.")

if __name__ == '__main__':
    main()