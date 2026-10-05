# Pipeline de Pagamentos — CDC → Flink SQL → Alerta de Fraude

Pipeline de dados de ponta a ponta no **Confluent Cloud**. Os dados nascem num
**Postgres gerenciado (Neon)**, entram por **CDC (Debezium)**, passam pelo
**Flink SQL** — que junta a transação com a conta e marca as suspeitas — e o
alerta segue para quem consome.

> Leia antes: [`docs/conceitos.md`](docs/conceitos.md) (o que é cada peça, em
> linguagem simples) e [`docs/arquitetura.md`](docs/arquitetura.md) (diagrama e
> o porquê de cada escolha).

**Regra de fraude:** 3 transações no mesmo cartão em 60 segundos → alerta.

Recursos reais desta execução (região `us-east-2`):

| Recurso | ID |
|---|---|
| Ambiente | `env-9oqmq7` |
| Cluster Kafka (Basic) | `lkc-0x8y832` |
| Service account escritor | `sa-5mwdxw2` (cdc-writer) |
| Service account leitor | `sa-nywpmwz` (app-reader) |
| Conector CDC | `lcc-81qdkqr` (PostgresCdcSourceV2) |
| Compute pool Flink | `lfcp-12dy073` |

---

## Pré-requisitos

- Conta no **Confluent Cloud** + **CLI** (`confluent version` ≥ 4.60).
- Conta no **Neon** (Postgres gerenciado) com **replicação lógica habilitada**
  (Console → Settings → Logical Replication → Enable).
- **psql** instalado (cliente PostgreSQL 17).
- Nunca versione o `.env`:

```powershell
Copy-Item .env.example .env   # preencha com os valores reais
```

Variáveis do `.env` (modelo completo em `.env.example`): `DATABASE_URL`,
`POSTGRES_CDC_PASSWORD`, `CONFLUENT_ENVIRONMENT_ID`, `CONFLUENT_CLUSTER_ID`,
`WRITER_API_KEY/SECRET`, `READER_API_KEY/SECRET`.

---

## Camada 1 · Fundação

Cria ambiente, cluster e **duas identidades** (escritor ≠ leitor), cada uma com
sua API key, e aplica ACLs de menor privilégio.

```powershell
confluent login --save
confluent environment create pipeline-pagamentos
confluent environment use env-9oqmq7
confluent kafka cluster create pagamentos --cloud aws --region us-east-2 --type basic
confluent kafka cluster use lkc-0x8y832

# identidades e chaves
confluent iam service-account create cdc-writer --description "conector CDC (escrita)"
confluent iam service-account create app-reader --description "Flink/consumidores (leitura)"
confluent api-key create --service-account sa-5mwdxw2 --resource lkc-0x8y832
confluent api-key create --service-account sa-nywpmwz --resource lkc-0x8y832

# tópicos
confluent kafka topic create transacoes
confluent kafka topic create contas
confluent kafka topic create alertas-fraude
confluent kafka topic create dlq-transacoes

# ACLs por PREFIXO (o conector publica como pg.public.*)
confluent kafka acl create --allow --service-account sa-5mwdxw2 --operations WRITE,CREATE --topic "pg." --prefix
confluent kafka acl create --allow --service-account sa-nywpmwz --operations READ        --topic "pg." --prefix
confluent kafka acl create --allow --service-account sa-nywpmwz --operations READ        --consumer-group "*"
```

**Evidência — ACLs (menor privilégio):**
```text
  User:sa-5mwdxw2 | ALLOW | WRITE/CREATE | TOPIC | transacoes, contas, dlq-transacoes
  User:sa-nywpmwz | ALLOW | READ         | TOPIC | transacoes, contas, alertas-fraude
  User:sa-nywpmwz | ALLOW | READ         | GROUP | *
```
> Nota: as ACLs por prefixo `pg.` foram adicionadas depois de ver o conector
> publicar como `pg.public.*`. Escritor só escreve; leitor só lê.

---

## Camada 2 · Contrato (Schema Registry)

Os schemas Avro são registrados **automaticamente** pelo conector. A política é
**BACKWARD** (permite adicionar campo com default sem quebrar consumidores).

```powershell
confluent schema-registry subject update pg.public.transacoes-value --compatibility BACKWARD
```

**Teste real (aguenta mudança de campo):** adicionamos uma coluna no Postgres e
o Registry evoluiu o contrato sozinho.

```powershell
psql $env:DATABASE_URL -c "ALTER TABLE transacoes ADD COLUMN dispositivo TEXT NOT NULL DEFAULT 'desconhecido';"
psql $env:DATABASE_URL -c "INSERT INTO transacoes (id_transacao,id_cartao,id_conta,valor,moeda,canal,dispositivo) VALUES ('schema-test','card-ana','acc-001',50.00,'BRL','online','mobile');"
confluent schema-registry schema list
```

**Evidência — nova versão registrada:**
```text
  100002 | pg.public.transacoes-value | 1
  100009 | pg.public.transacoes-value | 2   <-- evoluiu, compatível
```

---

## Camada 3 · Ingestão (CDC do Postgres)

### Preparo do banco (uma vez)

```powershell
$env:DATABASE_URL = (Get-Content .env | Where-Object { $_ -match '^DATABASE_URL=' }) -replace '^DATABASE_URL=',''
psql $env:DATABASE_URL -f db/01-schema.sql
psql $env:DATABASE_URL -f db/02-seed.sql
psql $env:DATABASE_URL -v cdc_password="SUA_SENHA_FORTE" -f db/03-cdc-setup.sql
```

Verificação esperada: `wal_level = logical`, `dbz_pub`, `rolreplication = t`.

> **Atenção Neon:** o conector precisa do host **direto** (sem `-pooler`). O
> pooler (PgBouncer) não suporta replicação lógica. O script
> `connectors/create-connector.ps1` remove o `-pooler` automaticamente.

### Subir o conector

```powershell
.\connectors\create-connector.ps1
confluent connect cluster describe lcc-81qdkqr   # aguardar Status: RUNNING
```

**Evidência — insert, update e delete como evento** (tópico `pg.public.transacoes`):
```text
tx-0001 ... "op":"r"   (snapshot inicial — insert)
tx-0001 ... "op":"u"   before: valor 120.50  →  after: valor 777   (REPLICA IDENTITY FULL)
tx-0007 ... "op":"d"   before: linha completa, after: null
tx-0007 {}             (tombstone após o delete)
```

> **DLQ:** o conector está configurado com `errors.tolerance=all` e
> `errors.deadletterqueue.topic.name=dlq-transacoes` — evento com defeito vai
> para fila separada em vez de parar o conector. (Configurado; não forçamos um
> evento inválido nesta execução.)

---

## Camada 4 · Processamento (Flink SQL)

Script completo e comentado: [`flink/fraude.sql`](flink/fraude.sql).

**Descoberta de engenharia:** a fonte CDC chega em `changelog.mode = retract`
(emite update/delete), e janelas sobre tempo de evento **não aceitam** retract.
A solução correta e suportada é converter as fontes para `append` — no modo
append o Flink trata todo evento como INSERT, ideal para contar transações.

```sql
ALTER TABLE `pg.public.transacoes` SET ('changelog.mode' = 'append');
ALTER TABLE `pg.public.contas`     SET ('changelog.mode' = 'append');
```

A regra (janela TUMBLE de 60s + JOIN com a conta) grava em `alertas_fraude`.
Para testar, geramos 3 transações no mesmo cartão em <60s:

```powershell
$ts = Get-Date -Format "yyyyMMddHHmmss"
psql $env:DATABASE_URL -c "INSERT INTO transacoes (id_transacao,id_cartao,id_conta,valor,moeda,canal) VALUES ('fraude-$ts-1','card-fraude','acc-001',100,'BRL','online'),('fraude-$ts-2','card-fraude','acc-001',200,'BRL','online'),('fraude-$ts-3','card-fraude','acc-001',300,'BRL','online');"
```

**Evidência — alerta gerado:**
```text
id_cartao   | titular   | qtd_tx | valor_total | janela_inicio       | janela_fim
card-fraude | Ana Souza | 3      | 600.00      | 2026-10-04 20:26:00 | 20:27:00
```

> Nota operacional: a janela por tempo de evento só fecha quando a **watermark**
> avança — ou seja, quando chega um evento posterior ao fim da janela. Em
> produção, o fluxo contínuo de transações resolve isso naturalmente.

---

## Camada 5 · Operação

Ver [`docs/custo.md`](docs/custo.md) para o detalhamento de custo.

```powershell
confluent kafka acl list                                           # acessos (Camada 1)
confluent flink statement list --compute-pool lfcp-12dy073 --environment env-9oqmq7 | Select-String RUNNING
confluent billing cost list --start-date 2026-10-04 --end-date 2026-10-05
```

- **Acesso:** ACLs por identidade (menor privilégio) — ver Camada 1.
- **Métricas:** 1 job de detecção `RUNNING`, sem statements duplicados consumindo CFU.
- **Custo:** estimado em **< US$ 1** para toda a sessão (ver `docs/custo.md`).

---

## Melhoria · RAG de estimativa de custo (BD vetorial no Flink)

Um RAG que estima o custo de um job novo pela semelhança com jobs já executados.
Roda **inteiro no Flink**, com embedding **gerenciado** (custo zero de IA, sem
chave externa). Detalhes e SQL: [`docs/rag-custo.md`](docs/rag-custo.md) e
[`flink/rag-custo.sql`](flink/rag-custo.sql).

**Evidência:** consulta "janela de agregação por cartão para detectar fraude" →
3 vizinhos mais similares por cosseno → **custo estimado US$ 0,068**.

---

## Como derrubar tudo (teardown)

> Ao final, para não acumular custo.

```powershell
confluent connect cluster delete lcc-81qdkqr
confluent flink compute-pool delete lfcp-12dy073
confluent kafka cluster delete lkc-0x8y832
confluent environment delete env-9oqmq7
```

No Postgres (Neon):
```sql
DROP PUBLICATION IF EXISTS dbz_pub;
SELECT pg_drop_replication_slot('dbz_slot_pagamentos');
```

---

## Checklist de entrega

- [x] Cada camada tem evidência (prints/saídas reais neste README).
- [x] Outra pessoa sobe o pipeline seguindo só o que está escrito.
- [x] Nenhuma senha/chave versionada; `.env.example` no lugar do `.env`.
- [x] RAG com BD vetorial (similaridade) implementado como melhoria.

## Ideias para evoluir (não implementadas)

- Segunda regra de fraude (outra janela/critério).
- Forçar um evento inválido para provar a DLQ.
- Materializar tópicos como tabelas Iceberg com Tableflow.
- Scoring de risco em tempo real por similaridade vetorial (caminho quente).