CREATE TABLE public.voicebot_results (
    id bigserial NOT NULL, 
    campaign_id varchar(24) NOT NULL, -- data['id']
    campaign_name varchar(100) NOT NULL, -- data['nombre']
    "document" varchar(20) NOT NULL, -- data['rut']
    phone varchar(15) NOT NULL, -- data['fono']
    "date" timestamp NOT NULL, -- data['fecha'] + data['hora'] (timezone)
    date_utc TIMESTAMPTZ NOT NULL, -- data['time'] (timezone UTC)
    "timezone" varchar(50) NOT NULL, -- data['timezone']
    management varchar(100) NOT NULL, -- data['gestion']
    sub_management varchar(100) NULL, -- data['subgestion']
    weight int2 NOT NULL, -- data['peso']
    promise_date date NULL, -- data['fecha_compromiso']
    interest text NULL, -- data['interes']
    uid UUID NOT NULL DEFAULT gen_random_uuid(),
    promise varchar(100) NULL, -- data['compromiso']
    observation jsonb NULL, -- data['observacion']
    project_uid varchar(24) NOT NULL, -- data['idproyect']
    client_uid varchar(24) NOT NULL, -- data['idcliente']
    duration int2 NULL, -- data['duration']
    telephony_id varchar(24) NOT NULL, -- data['id_telefonia']
    interactions jsonb NULL, -- data['interactions']
    responses jsonb NULL, -- data['responses']
    uniqueid varchar(20) NULL, -- data['uniqueid']
    url varchar(160) NULL, -- data['url_record_bot']
    id_record varchar(24) NULL, -- data['registro']
    bot_extension varchar(50) NULL, -- data['idBot']
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, "date")
) PARTITION BY RANGE ("date");

CREATE INDEX idx_gestiones_project_date ON public.voicebot_results (project_uid, "date");
CREATE INDEX idx_gestiones_document ON public.voicebot_results ("document");
CREATE INDEX idx_gestiones_phone ON public.voicebot_results (phone);
CREATE INDEX idx_gestiones_date_utc ON public.voicebot_results (date_utc);
CREATE INDEX idx_gestiones_management ON public.voicebot_results (management);