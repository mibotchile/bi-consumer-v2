CREATE TABLE public.mibotair_results (
    id bigserial NOT NULL, 
    campaign_id int NOT NULL, -- data['id']
    campaign_name varchar(100) NOT NULL, -- data['nombre']
    "document" varchar(20) NOT NULL, -- data['rut']
    phone varchar(15) NOT NULL, -- data['fono']
    "date" timestamp NOT NULL, -- data['fecha'] + data['hora'] (timezone)
    date_utc TIMESTAMPTZ NOT NULL, -- data['time'] (timezone UTC)
    "timezone" varchar(50) NOT NULL, -- data['timezone']
    management varchar(100) NOT NULL, -- data['gestion']
    sub_management varchar(100) NULL, -- data['subgestion']
    management_id int NOT NULL, -- data['id_gestion']
    weight int2 NOT NULL, -- data['peso']
    promise_date date NULL, -- data['fecha_compromiso']
    promise_amount float NULL, -- data['monto_compromiso']
    uid UUID NOT NULL DEFAULT gen_random_uuid(), 
    observation varchar(255) NULL, -- data['observacion']
    project_uid varchar(24) NOT NULL, -- data['idproyect']
    client_uid varchar(24) NOT NULL, -- data['idcliente']
    duration int2 NULL, -- data['duracion']
    telephony_id int NOT NULL, -- data['id_telefonia']
    extra_data jsonb NULL, -- data['campos_adicionales'] 
    uniqueid varchar(20) NULL, -- data['uniqueid']
    url varchar(160) NULL, -- data['url']
    id_record varchar(24) NULL, -- data['registro']
    n1 varchar(100) NULL, -- data['n1']
    n2 varchar(100) NULL, -- data['n2']
    n3 varchar(100) NULL, -- data['n3']
    agent_name varchar(100) NULL, -- data['nombre_agente']
    agent_email varchar(100) NULL, -- data['correo_agente']
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, "date")
) PARTITION BY RANGE ("date");

CREATE INDEX idx_gestiones_mibotair_project_date ON public.mibotair_results (project_uid, "date");
CREATE INDEX idx_gestiones_mibotair_document ON public.mibotair_results ("document");
CREATE INDEX idx_gestiones_mibotair_phone ON public.mibotair_results (phone);
CREATE INDEX idx_gestiones_mibotair_date_utc ON public.mibotair_results (date_utc);
CREATE INDEX idx_gestiones_mibotair_management ON public.mibotair_results (management);
CREATE INDEX idx_gestiones_mibotair_n1 ON public.mibotair_results (n1);
CREATE INDEX idx_gestiones_mibotair_n2 ON public.mibotair_results (n2);
CREATE INDEX idx_gestiones_mibotair_n3 ON public.mibotair_results (n3);
