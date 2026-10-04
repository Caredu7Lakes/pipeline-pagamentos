# Pipeline de Pagamentos — CDC → Flink SQL → Alerta de Fraude

Pipeline de ponta a ponta no **Confluent Cloud**: os dados nascem no **Postgres**,
entram por **CDC**, passam pelo **Flink SQL** (enriquecimento + regra de fraude) e
seguem para quem consome.

> **Arquitetura e justificativas:** veja [`docs/arquitetura.md`](docs/arquitetura.md).
> Leia esse documento primeiro. Aqui é o passo a passo para **subir e provar**.

**Regra de fraude (Camada 4):** 3 transações no mesmo cartão em 60 segundos → alerta.

---

## Pré-requisitos

```bash
# CLI do Confluent (https://docs.confluent.io/confluent-cli/current/install.html)
confluent version        # evidência: cole a versão abaixo
# <sua evidência aqui>

# Postgres com replicação lógica habilitada (ver Camada 3)
```

- Conta no Confluent Cloud.
- Postgres acessível pela rede do Confluent (endpoint público liberado ou PrivateLink).
- **Nunca** versione o `.env`. Copie o modelo:

```bash
cp .env.example .env      # preencha os valores reais; o .env está no .gitignore
```

---

## Camada 1 · Fundação

Cria ambiente, cluster e **duas identidades** (quem escreve ≠ quem lê).

```bash
confluent login --save

# cria e seleciona o ambiente
confluent environment create moshe-pipeline
confluent environment use env-xxxxx                 # id retornado acima

# cluster Basic (mais barato para aprender)
confluent kafka cluster create pagamentos \
  --cloud aws --region us-east-1 --type basic
confluent kafka cluster use lkc-xxxxx               # id do cluster

# duas service accounts: uma escreve (CDC), outra lê (Flink/consumidores)
confluent iam service-account create cdc-writer  --description "conector CDC (escrita)"
confluent iam service-account create app-reader  --description "Flink/consumidores (leitura)"

# uma chave de API por identidade (least privilege)
confluent api-key create --service-account sa-writer-id --resource lkc-xxxxx
confluent api-key create --service-account sa-reader-id --resource lkc-xxxxx
```

**Evidência (cluster UP + 2 contas):**
```text
# saída de: confluent kafka cluster list
# <sua evidência aqui>

# saída de: confluent iam service-account list
# <sua evidência aqui>
```

---

## Camada 2 · Contrato (Schema Registry)

Registra o formato das mensagens e testa se aguenta uma mudança de campo.

```bash
# habilita o Schema Registry no ambiente (escolha a região/cloud do cluster)
confluent schema-registry cluster enable --cloud aws --geo us

# define a política de compatibilidade do subject (BACKWARD = produtor pode
# evoluir sem quebrar consumidores existentes)
confluent schema-registry subject update transacoes-value --compatibility BACKWARD

# testa ANTES de registrar: a nova versão do schema é compatível?
confluent schema-registry schema validate \
  --subject transacoes-value \
  --schema schemas/transacoes-v2.avsc
```

**Evidência (teste de compatibilidade):**
```text
# resultado do validate (compatível / incompatível) ao adicionar/remover campo
# <sua evidência aqui>
```

---

## Camada 3 · Ingestão (CDC do Postgres)

### O que o Postgres precisa ANTES de ligar o conector

O conector lê o **WAL**, não as tabelas. Prepare o banco:

```sql
-- 1) replicação lógica (postgresql.conf). No RDS: rds.logical_replication = 1.
--    Exige RESTART do Postgres.
--    wal_level = logical
--    max_replication_slots >= 1
--    max_wal_senders       >= 1

-- 2) usuário dedicado de leitura do WAL
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD '***';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;

-- 3) publication (previsível criar à mão em vez de deixar o conector criar)
CREATE PUBLICATION dbz_pub FOR TABLE transacoes, contas;

-- 4) imagem completa da linha em update/delete (senão vem só a PK)
ALTER TABLE transacoes REPLICA IDENTITY FULL;
ALTER TABLE contas     REPLICA IDENTITY FULL;
```

Verifique antes de seguir:

```sql
SHOW wal_level;                                              -- deve ser: logical
SELECT * FROM pg_publication;                                -- deve listar dbz_pub
SELECT rolreplication FROM pg_roles WHERE rolname='cdc_user';-- deve ser: t
```

### Subir o conector CDC

O conector é definido em [`connectors/postgres-cdc.json`](connectors/postgres-cdc.json)
(valores vêm do `.env`; **nenhuma senha no JSON versionado**).

```bash
# cria o conector gerenciado (Debezium Postgres CDC Source)
confluent connect cluster create --config-file connectors/postgres-cdc.json

# acompanha até ficar RUNNING
confluent connect cluster list
```

**Evidência (insert, update e delete chegando como evento):**
```text
# consuma o tópico e provoque 1 insert, 1 update e 1 delete no Postgres
# confluent kafka topic consume transacoes --from-beginning --print-key
# <sua evidência aqui — os 3 eventos (op=c, op=u, op=d)>
```

> **DLQ:** o conector está configurado com `errors.tolerance=all` e
> `errors.deadletterqueue.topic.name=dlq-transacoes`, para que um evento com
> defeito vá para fila separada em vez de **parar o conector**.

---

## Camada 4 · Processamento (Flink SQL)

Enriquece a transação com a conta e aplica a regra de fraude. Script completo e
comentado em [`flink/fraude.sql`](flink/fraude.sql).

```sql
-- JOIN: enriquece cada transação com os dados da conta
-- WINDOW: conta transações por cartão em janelas de 60s
-- REGRA:  3+ no mesmo cartão em 60s -> grava em 'alertas-fraude'
-- (ver flink/fraude.sql para o código completo e comentado)
```

Executar:

```bash
# abre o shell SQL do Flink no Confluent Cloud
confluent flink shell --compute-pool lfcp-xxxxx --database lkc-xxxxx

# dentro do shell, rode o conteúdo de flink/fraude.sql
```

**Evidência (alerta gerado pela regra):**
```text
# linha(s) do tópico alertas-fraude após injetar 3 tx no mesmo cartão em <60s
# <sua evidência aqui>
```

---

## Camada 5 · Operação

```bash
# acessos (ACLs) por identidade — prova do least privilege
confluent kafka acl list

# métricas do cluster (throughput, lag) — via Metrics API ou console
# <sua evidência aqui: print do dashboard>
```

**Custo (obrigatório no final):**

```text
# estimativa/real do período em que o pipeline ficou no ar
# fonte: Confluent Cloud > Billing & payment
# <sua evidência aqui: valor em US$ e o que mais pesou>
```

---

## Como derrubar tudo (teardown)

> Faça isso ao final para **não acumular custo**.

```bash
confluent connect cluster delete lcc-xxxxx          # conector CDC
confluent flink compute-pool delete lfcp-xxxxx      # pool do Flink
confluent kafka cluster delete lkc-xxxxx            # cluster
confluent environment delete env-xxxxx              # ambiente (remove o resto)
```

No Postgres:

```sql
DROP PUBLICATION IF EXISTS dbz_pub;
-- o slot de replicação criado pelo conector precisa ser removido para liberar WAL:
SELECT pg_drop_replication_slot('nome_do_slot');    -- ver em pg_replication_slots
```

---

## Checklist antes de submeter

- [ ] Cada camada tem evidência e **não sobrou nenhum** `<sua evidência aqui>`.
- [ ] Outra pessoa sobe o pipeline seguindo só este README.
- [ ] Nenhuma senha/chave versionada; `.env.example` no lugar do `.env`.
- [ ] O link enviado é o do **repositório**, não o de um arquivo dentro dele.

## Melhorias (opcionais) — ramos de IA

Ver [`docs/arquitetura.md`](docs/arquitetura.md): scoring em tempo real (busca
vetorial) e RAG na investigação do alerta. Acoplam via `alertas-fraude` e não são
dependência da entrega base.
