# BI Consumer V2

## Desarrollado por:
- 
mibotchile - Jose Rivas

## Requisitos
- Python 3.10

## Documentacion

# Configuracion entorno virtual
1. python -m venv venv
2. source venv/bin/activate # Linux/Mac
3. .\venv\Scripts\activate # Windows
- 
# Instrucciones de instalacion
1. pip install -r requirements.txt

# Instrucciones de ejecucion
#consumidores v1 postgres
1. python consumer/bd_mibotair_consumer.py
2. python consumer/bd_voicebot_consumer.py

#consumidores v2 doris
1. python consumer/doris_mibotair_consumer.py
2. python consumer/doris_voicebot_consumer.py

#doris health check api
1. python api.py

#############################################
# Puesta en produccion como servicio

1. crear archivo del servicio
sudo nano /etc/systemd/system/bi-healthcheck.service

[Unit]
Description=API Healthcheck para Apache Doris
After=network.target 
[Service]
# Usuario que ejecutará la API (puede ser 'ubuntu', 'root' o 'www-data')
User=root 
# Directorio donde está tu código
WorkingDirectory=/ruta/del/repo 
# Comando de arranque (Usa la ruta completa de tu python o venv)
# Si usas entorno virtual (venv), la ruta suele ser /ruta/venv/bin/uvicorn
ExecStart=/ruta/del/repo/venv/Scripts/uvicorn api:app --host 0.0.0.0 --port 8000 --workers 4
# Cargar variables de entorno automáticamente
EnvironmentFile=/ruta/del/repo/.env
# Reiniciar automáticamente si falla
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target

2. activar el servicio
sudo systemctl daemon-reload 
# Habilitar el servicio para que inicie siempre al prender el servidor
sudo systemctl enable bi-healthcheck 
# Iniciar el servicio
sudo systemctl start bi-healthcheck 
# Detener el servicio
sudo systemctl stop bi-healthcheck 
# Ver el estado 
sudo systemctl status bi-healthcheck
# ver logs
sudo journalctl -u bi-healthcheck -f
