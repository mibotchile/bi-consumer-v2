DROP TABLE IF EXISTS GENERAL.voicebot_results;

CREATE TABLE IF NOT EXISTS GENERAL.voicebot_results ( 
    `client_uid` VARCHAR(24) COMMENT "ID Cliente (Sort Key 1)", 
    `project_uid` VARCHAR(24) COMMENT "ID Proyecto (Sort Key 2)",
    `date` DATETIME COMMENT "Fecha REAL de la gestión (Sort Key 3)",
     
    `uid` VARCHAR(36) COMMENT "UUID autogenerado",
    `campaign_id` VARCHAR(24),
    `campaign_name` VARCHAR(100),
    `document` VARCHAR(20) COMMENT "RUT/DNI",
    `phone` VARCHAR(15),
    `management` VARCHAR(100),
    `sub_management` VARCHAR(100),
    `weight` SMALLINT,
    `promise_date` DATE,
    `promise` VARCHAR(100),
    `interest` STRING,
    
    `duration` INT,
    `telephony_id` VARCHAR(24),
    `uniqueid` VARCHAR(20),
    `url` STRING,
    `id_record` VARCHAR(24),
    `bot_extension` VARCHAR(50),
    
    `observation` JSONB,
    `interactions` JSONB,
    `responses` JSONB,
    
    `date_utc` DATETIME,
    `timezone` VARCHAR(50),
    `created_at` DATETIME COMMENT "Fecha inserción sistema",

    INDEX idx_uid (uid) USING INVERTED COMMENT 'Vital para borrado puntual',
    INDEX idx_doc (document) USING INVERTED COMMENT 'Busqueda por RUT',
    INDEX idx_phone (phone) USING INVERTED,
    INDEX idx_mgmt (management) USING INVERTED,
    INDEX idx_project (project_uid) USING INVERTED,
    INDEX idx_client (client_uid) USING INVERTED,
    INDEX idx_date_utc (date_utc) USING INVERTED
)
ENGINE=OLAP
DUPLICATE KEY(`client_uid`, `project_uid`, `date`)
PARTITION BY RANGE(`date`) () 
DISTRIBUTED BY HASH(`project_uid`) BUCKETS AUTO 
PROPERTIES (
    "replication_num" = "2",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-1000",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10" 
);