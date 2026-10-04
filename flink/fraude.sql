-- ===========================================================================
-- Camada 4 · Processamento (Flink SQL no Confluent Cloud)
-- Enriquece a transação com os dados da conta e aplica a regra de fraude:
--   3 transações no mesmo cartão em 60 segundos -> alerta.
-- Rode este arquivo dentro de: confluent flink shell ...
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1) Tabelas de origem
-- No Confluent Cloud, cada tópico Kafka já aparece como tabela Flink com o
-- schema do Schema Registry. Aqui declaramos APENAS os metadados que o Flink
-- não infere sozinho: a coluna de tempo do evento e a watermark.
-- ---------------------------------------------------------------------------

-- Transações vindas do CDC (tópico 'transacoes')
ALTER TABLE transacoes
  ADD WATERMARK FOR `$rowtime` AS `$rowtime` - INTERVAL '5' SECOND;
-- `$rowtime` = horário do evento no Kafka. A watermark tolera até 5s de atraso
-- antes de considerar a janela fechada.

-- ---------------------------------------------------------------------------
-- 2) Enriquecimento: transação  JOIN  conta
-- Usamos um JOIN temporal: para cada transação, pega a versão da conta válida
-- naquele instante (FOR SYSTEM_TIME AS OF). Evita casar com um estado futuro.
-- ---------------------------------------------------------------------------
CREATE VIEW transacoes_enriquecidas AS
SELECT
    t.id_transacao,
    t.id_cartao,
    t.id_conta,
    t.valor,
    t.`$rowtime` AS momento,
    c.titular,
    c.limite,
    c.status_conta
FROM transacoes AS t
JOIN contas FOR SYSTEM_TIME AS OF t.`$rowtime` AS c
  ON t.id_conta = c.id_conta;

-- ---------------------------------------------------------------------------
-- 3) Tópico de saída dos alertas
-- ---------------------------------------------------------------------------
CREATE TABLE alertas_fraude (
    id_cartao    STRING,
    titular      STRING,
    qtd_tx       BIGINT,
    valor_total  DECIMAL(12,2),
    janela_fim   TIMESTAMP_LTZ(3),
    regra        STRING
) WITH (
    'connector' = 'kafka',
    'topic'     = 'alertas-fraude'
    -- demais propriedades (bootstrap, chave reader) herdadas do ambiente
);

-- ---------------------------------------------------------------------------
-- 4) Regra de fraude · janela deslizante (HOP) de 60s
-- Conta transações por cartão em janelas de 60s, avançando de 10 em 10s.
-- Dispara quando houver 3 ou mais no mesmo cartão dentro da janela.
-- ---------------------------------------------------------------------------
INSERT INTO alertas_fraude
SELECT
    id_cartao,
    MAX(titular)                         AS titular,
    COUNT(*)                             AS qtd_tx,
    SUM(valor)                           AS valor_total,
    window_end                           AS janela_fim,
    'R1: 3+ tx mesmo cartao em 60s'      AS regra
FROM TABLE(
    HOP(
        TABLE transacoes_enriquecidas,
        DESCRIPTOR(momento),
        INTERVAL '10' SECOND,   -- passo da janela
        INTERVAL '60' SECOND    -- tamanho da janela
    )
)
GROUP BY id_cartao, window_start, window_end
HAVING COUNT(*) >= 3;

-- ===========================================================================
-- IDEIA DE EVOLUÇÃO (Regra 2): outra janela / outro critério. Exemplo:
--   valor somado > limite da conta em janela de 5 min -> alerta 'R2'.
-- Basta um segundo INSERT INTO alertas_fraude com HOP/TUMBLE diferente.
-- ===========================================================================
