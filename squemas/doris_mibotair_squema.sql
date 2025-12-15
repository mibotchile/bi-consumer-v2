
CREATE TABLE IF NOT EXISTS GENERAL.mibotair_results ( 
    `client_uid` VARCHAR(100) COMMENT "Cliente (Banco/Empresa)", 
    `project_uid` VARCHAR(100) COMMENT "Proyecto/Campaña macro",
    `date` DATETIME COMMENT "Fecha REAL de la gestión",
     
    `uid` VARCHAR(36) COMMENT "ID Único autogenerado por el consumidor", -- <--- NUEVA COLUMNA
    `campaign_id` BIGINT,
    `campaign_name` VARCHAR(255),
    `document` VARCHAR(50) COMMENT "RUT/DNI",
    `phone` VARCHAR(20),
    `management` VARCHAR(100),
    `sub_management` VARCHAR(100),
    `management_id` BIGINT,
    `weight` INT,
    `promise_date` DATE,
    `promise_amount` DECIMAL(12, 2),
    `observation` VARCHAR(500),
    `duration` INT,
    `telephony_id` BIGINT,
    `uniqueid` VARCHAR(100),
    `url` STRING,
    `id_record` BIGINT,
    `agent_name` VARCHAR(100),
    `agent_email` VARCHAR(150),
    `n1` VARCHAR(100),
    `n2` VARCHAR(100),
    `n3` VARCHAR(100),
    `extra_data` JSON, 
    `date_utc` DATETIME,
    `timezone` VARCHAR(50),
    `created_at` DATETIME COMMENT "Fecha inserción sistema",
 
    INDEX idx_uid (uid) USING INVERTED COMMENT 'Busqueda unica para borrado', -- <--- INDICE VITAL
    INDEX idx_doc (document) USING INVERTED COMMENT 'Busqueda rapida por RUT',
    INDEX idx_phone (phone) USING INVERTED COMMENT 'Busqueda rapida por telefono',
    INDEX idx_mgmt (management) USING INVERTED COMMENT 'Filtro por gestion',
    INDEX idx_submgmt (sub_management) USING INVERTED,
    INDEX idx_agent (agent_email) USING INVERTED,
    INDEX idx_n1 (n1) USING INVERTED,
    INDEX idx_n2 (n2) USING INVERTED,
    INDEX idx_n3 (n3) USING INVERTED,
    INDEX idx_project (project_uid) USING INVERTED, 
    INDEX idx_client (client_uid) USING INVERTED    
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