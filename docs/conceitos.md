# Conceitos — o papel de cada peça do pipeline

> Para quem está chegando agora. Primeiro a analogia, depois o técnico.
> Para o desenho do fluxo e as justificativas de engenharia, ver
> [`arquitetura.md`](arquitetura.md).

## A analogia: um banco físico

Imagine o fluxo de dinheiro dentro de um banco.

- **Postgres** é o **cofre e os livros-razão**. Onde cada conta e cada transação
  ficam registradas oficialmente. É a fonte da verdade — o dado nasce ali.
- **Kafka** é a **esteira transportadora** que leva cada movimento, no instante
  em que acontece, do cofre até os setores que precisam saber. Não guarda a
  verdade; transporta os fatos em ordem, um atrás do outro.
- **Confluent** é a **empresa que opera essa esteira para você** — monta, mantém
  e cuida da segurança dela, para você não precisar construir e operar a esteira
  do zero.
- **CDC** é o **funcionário que fica olhando o livro-razão**. Toda vez que alguém
  escreve uma linha nova (ou altera/apaga), ele grita para a esteira: "entrou uma
  transação!". Não copia o livro inteiro de novo; avisa só o que mudou.
- **Flink** é o **analista de fraude na ponta da esteira**. Cada transação passa
  por ele, que junta com os dados da conta e aplica a regra. Se vê 3 compras no
  mesmo cartão em 60 segundos, carimba "suspeita" e manda para uma esteira
  separada de alertas.

## O técnico, peça por peça

**Postgres** — banco de dados relacional, a **origem**. Guarda as tabelas
`contas` e `transacoes`. Tudo começa aqui; é o único lugar onde o dado "existe de
verdade". Neste projeto roda gerenciado no Neon.

**Kafka** — plataforma de *streaming* de eventos. Em vez de ficar perguntando ao
banco "mudou algo?" o tempo todo (*polling*, caro e lento), cada mudança vira um
**evento** que entra numa fila ordenada chamada **tópico** (`transacoes`,
`contas`, `alertas-fraude`). Quem precisa do dado "assina" o tópico e recebe na
hora. Desacopla quem produz de quem consome.

**Confluent Cloud** — Kafka **gerenciado**. Operar Kafka sozinho (servidores,
réplicas, atualizações, segurança) é trabalhoso. A Confluent entrega isso pronto
e ainda agrega o Schema Registry (contrato dos dados), o Flink e os conectores.
Aluga-se a infraestrutura em vez de mantê-la.

**CDC (Change Data Capture)** — a técnica que liga Postgres → Kafka. O conector
(Debezium) lê o **WAL** do Postgres — o diário onde o banco anota toda alteração
antes de aplicá-la — e transforma cada `insert/update/delete` num evento Kafka.
Por isso configuramos `wal_level=logical`, a *publication* e o `cdc_user`: foi dar
ao funcionário permissão e acesso ao livro-razão.

**Flink SQL** — motor de **processamento de stream**. Lê os eventos conforme
chegam, faz o `JOIN` da transação com a conta (enriquecimento) e aplica a **regra
de fraude** com janela de tempo (3 tx / mesmo cartão / 60s). O resultado vira o
tópico `alertas-fraude`.

## O caminho completo, numa frase

O dado **nasce no Postgres**, o **CDC** percebe a mudança e a entrega à **esteira
do Kafka** (operada pela **Confluent**), o **Flink** analisa no caminho e marca o
que é suspeito, e o alerta segue para quem vai consumir.

## Por que duas identidades separadas

As ACLs criadas na Camada 1: `cdc-writer` só pode **colocar** coisas na esteira;
`app-reader` só pode **pegar**. Como no banco: o caixa que registra não é o mesmo
que audita. Se uma credencial vaza, o estrago fica limitado ao que ela podia
fazer. Isso é o princípio de **menor privilégio** (*least privilege*).

## Glossário rápido

| Termo | Em uma linha |
|---|---|
| **Evento** | Um fato que aconteceu (ex.: "transação tx-123 criada"). |
| **Tópico** | Fila ordenada de eventos do mesmo tipo. |
| **WAL** | Diário interno do Postgres com toda alteração feita no banco. |
| **Publication** | Lista de tabelas que o Postgres expõe para replicação. |
| **Schema Registry** | Guarda o "contrato" (formato) das mensagens. |
| **Janela (window)** | Recorte de tempo onde o Flink conta eventos (ex.: 60s). |
| **DLQ** | Fila separada para eventos com defeito, para não travar o fluxo. |
| **Least privilege** | Cada identidade só pode fazer o mínimo necessário. |