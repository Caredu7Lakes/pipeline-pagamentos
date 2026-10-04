-- ===========================================================================
-- 02-seed.sql · Dados sintéticos de BASELINE (estado inicial normal)
-- Idempotente: ON CONFLICT DO NOTHING evita duplicar ao re-rodar.
-- O cenário de FRAUDE (3 tx/mesmo cartão/60s) NÃO vai aqui — ele precisa
-- acontecer em tempo real para a janela do Flink contar; virá de um gerador
-- de carga (etapa posterior), não de dados históricos.
-- Rodar:  psql "$DATABASE_URL" -f db/02-seed.sql
-- ===========================================================================

-- Contas
INSERT INTO contas (id_conta, titular, limite, status_conta) VALUES
    ('acc-001', 'Ana Souza',      5000.00, 'ativa'),
    ('acc-002', 'Bruno Lima',     2000.00, 'ativa'),
    ('acc-003', 'Carla Nunes',   12000.00, 'ativa'),
    ('acc-004', 'Diego Alves',    1500.00, 'bloqueada'),
    ('acc-005', 'Elisa Prado',    8000.00, 'ativa')
ON CONFLICT (id_conta) DO NOTHING;

-- Transações normais (espalhadas; nenhum cartão com 3 em 60s)
INSERT INTO transacoes (id_transacao, id_cartao, id_conta, valor, moeda, canal) VALUES
    ('tx-0001', 'card-ana',   'acc-001',  120.50, 'BRL', 'pos'),
    ('tx-0002', 'card-ana',   'acc-001',   89.90, 'BRL', 'online'),
    ('tx-0003', 'card-bruno', 'acc-002',  250.00, 'BRL', 'pos'),
    ('tx-0004', 'card-carla', 'acc-003', 1500.00, 'BRL', 'online'),
    ('tx-0005', 'card-carla', 'acc-003',   45.00, 'BRL', 'pos'),
    ('tx-0006', 'card-elisa', 'acc-005',  999.99, 'BRL', 'pos'),
    ('tx-0007', 'card-bruno', 'acc-002',   30.00, 'BRL', 'atm'),
    ('tx-0008', 'card-ana',   'acc-001',  210.00, 'BRL', 'pos')
ON CONFLICT (id_transacao) DO NOTHING;