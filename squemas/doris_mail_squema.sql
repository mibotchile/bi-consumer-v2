
CREATE TABLE IF NOT EXISTS GENERAL.email_results (
    client_uid VARCHAR(24) COMMENT 'Obtenido via API AIM', 
    project_uid VARCHAR(24) COMMENT 'Viene en el mensaje',
    `date` DATETIME COMMENT 'Timestamp convertido', -- date es palabra reservada, requiere comillas
    uid VARCHAR(36) COMMENT 'UUID único por evento',
    campaign_id VARCHAR(50),
    document VARCHAR(20),
    email VARCHAR(150),
    event VARCHAR(50) COMMENT 'delivered, open, etc',
    weight TINYINT COMMENT 'Peso calculado segun evento',
    response STRING COMMENT 'Respuesta SMTP completa',
    ip VARCHAR(50),
    sg_event_id VARCHAR(100),
    sg_message_id VARCHAR(100),
    smtp_id VARCHAR(100),
    tls TINYINT,
    date_utc DATETIME,
    timezone VARCHAR(50),
    created_at DATETIME,
    
    INDEX idx_uid (uid) USING INVERTED,
    INDEX idx_doc (document) USING INVERTED,
    INDEX idx_email (email) USING INVERTED,
    INDEX idx_sg_msg (sg_message_id) USING INVERTED,
    INDEX idx_event (event) USING INVERTED,
    INDEX idx_project (project_uid) USING INVERTED,
    INDEX idx_client (client_uid) USING INVERTED
)
ENGINE=OLAP
DUPLICATE KEY(client_uid, project_uid, `date`)
PARTITION BY RANGE(`date`) () 
DISTRIBUTED BY HASH(project_uid) BUCKETS AUTO 
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-1000",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "20"
);