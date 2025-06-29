CREATE TABLE IF NOT EXISTS public.jornada_paciente (
    id SERIAL PRIMARY KEY,
    paciente_id VARCHAR(50) NOT NULL,
    evento_tipo VARCHAR(100) NOT NULL,
    metadata JSONB,
    evento_timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS public.kpis_operacionais (
    id SERIAL PRIMARY KEY,
    paciente_id VARCHAR(50),
    kpi_nome VARCHAR(100),
    kpi_valor_segundos INT,
    calculado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.ocupacao_leitos_atual (
    ala VARCHAR(100) PRIMARY KEY,
    leitos_ocupados INT NOT NULL,
    ultima_atualizacao TIMESTAMP NOT NULL
);