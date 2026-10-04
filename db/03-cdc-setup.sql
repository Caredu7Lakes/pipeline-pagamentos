-- ===========================================================================
-- 03-cdc-setup.sql · Preparo do Postgres para Change Data Capture (CDC)
-- Idempotente. Roda DEPOIS de 01-schema.sql.
--
-- SEGURANÇA: a senha do cdc_user NÃO fica no arquivo. Ela é passada como
-- variável do psql no momento da execução, então nada sensível é versionado:
--   psql "$DATABASE_URL" -v cdc_password="$POSTGRES_CDC_PASSWORD" -f db/03-cdc-setup.sql
--
-- PRÉ-CONDIÇÃO (fora deste script): o parâmetro de replicação lógica precisa
-- estar ligado no servidor.
--   - Postgres puro:  wal_level = logical   (postgresql.conf, exige restart)
--   - RDS/Aurora:     parameter group rds.logical_replication = 1
--   - Neon/Supabase:  replicação lógica já habilitada por padrão
-- ===========================================================================

-- 1) Role dedicada que lê o WAL (least privilege: só replicação + leitura)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cdc_user') THEN
        -- :'cdc_password' = valor vindo do -v do psql (aspas = string literal)
        EXECUTE format('CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD %L', :'cdc_password');
    END IF;
END
$$;

GRANT CONNECT ON DATABASE CURRENT_CATALOG TO cdc_user;  -- acesso ao banco atual
GRANT USAGE  ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
-- tabelas criadas no futuro também ficam legíveis para o conector
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- 2) Publication: o conjunto de tabelas que o conector vai publicar.
--    Criar à mão é mais previsível que deixar o conector criar.
DROP PUBLICATION IF EXISTS dbz_pub;
CREATE PUBLICATION dbz_pub FOR TABLE transacoes, contas;

-- 3) REPLICA IDENTITY FULL: faz update/delete carregarem a imagem ANTIGA
--    completa da linha no evento (sem isso vem só a PK).
ALTER TABLE transacoes REPLICA IDENTITY FULL;
ALTER TABLE contas     REPLICA IDENTITY FULL;

-- 4) Verificações (devem retornar: logical / dbz_pub / t)
SELECT current_setting('wal_level')                                   AS wal_level;
SELECT pubname FROM pg_publication                                    WHERE pubname = 'dbz_pub';
SELECT rolreplication FROM pg_roles                                   WHERE rolname = 'cdc_user';