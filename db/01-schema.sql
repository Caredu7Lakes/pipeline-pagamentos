-- ===========================================================================
-- 01-schema.sql · Modelo de dados (origem do CDC)
-- Idempotente: pode rodar novamente sem erro.
-- Alinhado a schemas/transacoes-v*.avsc e a flink/fraude.sql.
-- Rodar:  psql "$DATABASE_URL" -f db/01-schema.sql
-- ===========================================================================

-- Tabela de contas (dado "de referência" que enriquece a transação no Flink)
CREATE TABLE IF NOT EXISTS contas (
    id_conta      TEXT PRIMARY KEY,                 -- PK obrigatória para CDC
    titular       TEXT        NOT NULL,
    limite        NUMERIC(12,2) NOT NULL DEFAULT 0, -- usado por regras futuras (R2)
    status_conta  TEXT        NOT NULL DEFAULT 'ativa'
                   CHECK (status_conta IN ('ativa','bloqueada','encerrada')),
    atualizada_em TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Tabela de transações (o fluxo que o CDC captura evento a evento)
CREATE TABLE IF NOT EXISTS transacoes (
    id_transacao TEXT PRIMARY KEY,                  -- PK obrigatória para CDC
    id_cartao    TEXT        NOT NULL,              -- chave da janela de fraude
    id_conta     TEXT        NOT NULL REFERENCES contas(id_conta),
    valor        NUMERIC(12,2) NOT NULL CHECK (valor > 0),
    moeda        TEXT        NOT NULL DEFAULT 'BRL',
    canal        TEXT        NOT NULL DEFAULT 'pos' -- campo novo do schema v2
                   CHECK (canal IN ('pos','online','atm','recorrente')),
    criada_em    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Índice para consultas por cartão (análise/depuração; o Flink usa o stream)
CREATE INDEX IF NOT EXISTS idx_transacoes_cartao ON transacoes (id_cartao);