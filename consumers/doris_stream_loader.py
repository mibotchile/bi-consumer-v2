import base64
import gzip
import json
import os
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class DorisStreamLoader:
    def __init__(
        self,
        host,
        port,
        db,
        table,
        user,
        password,
        batch_size,
        max_seconds,
        encoder_cls,
        logger,
        label_prefix,
        stream_load_timeout,
        connect_timeout,
        read_timeout,
        retry_total,
        retry_backoff,
        retry_statuses,
        pool_maxsize,
        flush_workers,
        max_in_flight,
        inflight_wait_sec,
        enable_gzip,
        max_filter_ratio,
        pause_on_error_sec,
    ):
        self.load_url = f"http://{host}:{port}/api/{db}/{table}/_stream_load"
        self.user = user
        self.password = password
        self.batch_size = batch_size
        self.max_seconds = max_seconds
        self.encoder_cls = encoder_cls
        self.logger = logger
        self.label_prefix = label_prefix
        self.stream_load_timeout = stream_load_timeout
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.enable_gzip = enable_gzip
        self.max_filter_ratio = max_filter_ratio
        self.pause_on_error_sec = pause_on_error_sec
        self.hostname = socket.gethostname().replace(" ", "_")

        self.buffer = []
        self.last_flush_time = time.time()
        self._lock = threading.Lock()
        self._pause_until = 0

        self._executor = ThreadPoolExecutor(max_workers=flush_workers)
        self._inflight = threading.Semaphore(max_in_flight)
        self._inflight_wait_sec = inflight_wait_sec

        self.session = requests.Session()
        retries = Retry(
            total=retry_total,
            connect=retry_total,
            read=retry_total,
            status=retry_total,
            backoff_factor=retry_backoff,
            status_forcelist=retry_statuses,
            allowed_methods=frozenset(["PUT"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=pool_maxsize, pool_maxsize=pool_maxsize)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        auth_str = f"{self.user}:{self.password}"
        b64_auth = base64.b64encode(auth_str.encode()).decode()

        self.headers = {
            "format": "json",
            "strip_outer_array": "true",
            "Expect": "100-continue",
            "Authorization": f"Basic {b64_auth}",
            "enable_proxy": "true",
            "Content-Type": "application/json",
            "timeout": str(self.stream_load_timeout),
        }
        if self.max_filter_ratio is not None:
            self.headers["max_filter_ratio"] = str(self.max_filter_ratio)

    def add(self, record):
        with self._lock:
            self.buffer.append(record)
            if len(self.buffer) >= self.batch_size:
                self._flush_locked()

    def flush_if_needed(self):
        with self._lock:
            if self.buffer and (time.time() - self.last_flush_time > self.max_seconds):
                self._flush_locked()

    def flush(self):
        with self._lock:
            if self.buffer:
                self._flush_locked()

    def should_pause(self):
        return time.time() < self._pause_until

    def save_to_disk(self, json_payload, reason, file_prefix):
        try:
            filename = f"failed_loads/{file_prefix}_{int(time.time())}_{uuid.uuid4()}.json"
            os.makedirs("failed_loads", exist_ok=True)
            with open(filename, "w", encoding="utf-8") as f:
                f.write(json_payload)
            self.logger.error(
                f"Fallo Doris. Datos guardados en disco: {filename}. Razon: {reason}"
            )
            return True
        except Exception as e:
            self.logger.critical(f"Fallo total (Doris y disco): {e}")
            return False

    def _flush_locked(self):
        json_payload = json.dumps(self.buffer, cls=self.encoder_cls)
        batch_size = len(self.buffer)
        self.buffer.clear()
        self.last_flush_time = time.time()
        self._submit_payload(json_payload, batch_size)

    def _submit_payload(self, json_payload, batch_size):
        label = self._make_label(batch_size)
        if not self._inflight.acquire(timeout=self._inflight_wait_sec):
            self.logger.error("Limite de envios en vuelo; guardando lote en disco.")
            self.save_to_disk(json_payload, "inflight_limit", self.label_prefix)
            return
        future = self._executor.submit(self._send_payload, json_payload, label)
        future.add_done_callback(self._on_future_done)

    def _make_label(self, batch_size):
        suffix = uuid.uuid4().hex[:8]
        return f"{self.label_prefix}_{int(time.time() * 1000)}_{batch_size}_{self.hostname}_{suffix}"

    def _send_payload(self, json_payload, label):
        headers = self.headers.copy()
        headers["label"] = label
        data = json_payload

        if self.enable_gzip:
            data = gzip.compress(json_payload.encode("utf-8"))
            headers["Content-Encoding"] = "gzip"
            headers["compress_type"] = "gzip"

        try:
            response = self.session.put(
                self.load_url,
                data=data,
                headers=headers,
                allow_redirects=False,
                timeout=(self.connect_timeout, self.read_timeout),
            )

            final_response = response
            if response.status_code == 307:
                location = response.headers.get("Location")
                if not location:
                    self.save_to_disk(json_payload, "redirect_missing_location", self.label_prefix)
                    return False
                final_response = self.session.put(
                    location,
                    data=data,
                    headers=headers,
                    allow_redirects=True,
                    timeout=(self.connect_timeout, self.read_timeout),
                )

            resp_text = final_response.text or ""
            try:
                resp_dict = final_response.json()
            except ValueError:
                self.save_to_disk(
                    json_payload,
                    f"non_json_response status={final_response.status_code} body={resp_text[:500]}",
                    self.label_prefix,
                )
                return False

            if resp_dict.get("Status") == "Success":
                self.logger.info(
                    f"Doris Load OK. Label: {label}. Loaded: {resp_dict.get('NumberLoadedRows')}"
                )
                return True

            error_msg = resp_dict.get("Message", "Unknown Error")
            self.save_to_disk(json_payload, error_msg, self.label_prefix)
            return False
        except Exception as e:
            self.logger.error(f"Error de conexion/codigo: {e}")
            self.save_to_disk(json_payload, str(e), self.label_prefix)
            return False

    def _on_future_done(self, future):
        self._inflight.release()
        success = False
        try:
            success = future.result()
        except Exception as e:
            self.logger.error(f"Fallo en envio async: {e}")

        if not success and self.pause_on_error_sec > 0:
            self._pause_until = max(self._pause_until, time.time() + self.pause_on_error_sec)

    def close(self):
        self.flush()
        self._executor.shutdown(wait=True)
