-- ===========================================================================
-- 03-cdc-setup.sql · Preparo do Postgres para Change Data Capture (CDC)
-- Idempotente. Roda DEPOIS de 01-schema.sql.
--
-- SEGURANÇA: a senha do cdc_user NÃO fica no arquivo. É passada como variável
-- do psql no momento da execução (nada sensível é versionado):
--   psql "$DATABASE_URL" -v cdc_password="SENHA_FORTE" -f db/03-cdc-setup.sql
--
-- PRÉ-CONDIÇÃO (fora deste script): replicação lógica LIGADA no servidor.
--   - Neon:  Console > Project > Settings > Logical Replication > Enable
--            (depois reconectar). Sem isso, wal_level fica 'replica'.
--   - RDS/Aurora:  parameter group rds.logical_replication = 1
--   - Postgres puro:  wal_level = logical (postgresql.conf, exige restart)
-- ===========================================================================

-- 1) Role dedicada que lê o WAL (least privilege: replicacao + leitura).
--    CREATE ROLE nao aceita variavel dentro de format(); montamos o comando
--    como texto com quote_literal e executamos com \gexec. IF NOT EXISTS via
--    WHERE NOT EXISTS garante idempotencia.
SELECT 'CREATE ROLE cdc_user LOGIN REPLICATION PASSWORD ' || quote_literal(:'cdc_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cdc_user')
\gexec

-- 2) Permissoes de leitura
GRANT USAGE  ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
-- tabelas criadas no futuro tambem ficam legiveis para o conector
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- 3) Publication: conjunto de tabelas que o conector publica.
DROP PUBLICATION IF EXISTS dbz_pub;
CREATE PUBLICATION dbz_pub FOR TABLE transacoes, contas;

-- 4) REPLICA IDENTITY FULL: update/delete carregam a imagem ANTIGA completa
--    da linha no evento (sem isso vem so a PK).
ALTER TABLE transacoes REPLICA IDENTITY FULL;
ALTER TABLE contas     REPLICA IDENTITY FULL;

-- 5) Verificacoes (esperado: logical / dbz_pub / t)
SELECT current_setting('wal_level')                AS wal_level;
SELECT pubname        FROM pg_publication          WHERE pubname  = 'dbz_pub';
SELECT rolreplication FROM pg_roles                WHERE rolname  = 'cdc_user';