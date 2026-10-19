# Teste de carga

Este documento descreve o teste de carga do serviço de carteiras: o comando que o reproduz, o ambiente em que rodou, a
metodologia, os resultados e o que eles dizem. O enunciado pede comando reproduzível, ambiente, metodologia, throughput,
p50/p95/p99, erros, conflitos e atraso da outbox, e não fixa meta de RPS
([CHALLENGE §14](CHALLENGE.md#14-critérios-de-avaliação)). O gerador fica em [`test/load`](../test/load), atrás da build
tag `load`; as decisões técnicas do serviço estão em [`ARCHITECTURE.md`](../ARCHITECTURE.md) e as regras de negócio em
[`REGRAS_DE_NEGOCIO.md`](REGRAS_DE_NEGOCIO.md).

**Em resumo.** Pelo HTTP, o serviço sustentou 3.100–3.700 operações/s em carteiras distribuídas, com p99 de 17 ms (16
clientes) a 46 ms (64), e 2.000–2.200 operações/s martelando 5 carteiras, com p99 de 29 ms a 190 ms. Não houve nenhum
erro, nenhum retry de transação, nenhum `409` ou `503`, nenhuma mensagem na DLQ. Nas quatro fases, todas as carteiras
reconciliaram, os saldos bateram centavo a centavo com as operações e todo reenvio virou exatamente uma duplicata. O
caminho da fila e dos eventos é limitado pelo MiniStack, um emulador Python de um processo, e não pelo serviço. Os
eventos esperam minutos para sair, e a fila de entrada recebeu só 0,3–3,7% das operações.

## Sumário

1. [Comando](#comando)
2. [Ambiente](#ambiente)
3. [Metodologia](#metodologia)
4. [Resultados](#resultados)
5. [Análise](#análise)
6. [Limitações](#limitações)

## Comando

```sh
docker compose down -v
make load-test
docker compose down -v
```

O primeiro `down -v` garante um banco, um IdP e um broker vazios. `make load-test` sobe o compose (`make up`), constrói
a imagem do gerador (`test/load/Dockerfile`: o mesmo builder `golang:1.25.6-alpine` do serviço e um runtime `scratch`) e
o roda como serviço `load` de um compose sobreposto (`test/load/compose.yaml`), na mesma rede do compose:

```sh
docker compose -f docker-compose.yml -f test/load/compose.yaml run --rm --build load
```

Nada é copiado à mão. O gerador pede os tokens ao Keycloak (`client_credentials` dos clients de desenvolvimento do
realm, renovados um minuto antes de vencer) e lê as credenciais IAM dos provedores no volume `aws-shared`, montado só
para leitura, como as réplicas fazem; o leitor de eventos usa o root do emulador (ver [Metodologia](#metodologia)). O
resumo sai no stdout e o JSON completo em `tmp/load/load-<runId>.json` (o `tmp/` é ignorado pelo git). O código de saída
é 0 com a integridade aprovada em todas as fases e 1 caso contrário. A execução registrada abaixo levou 25 min 27 s, do
`make load-test` ao fim. Quase tudo foi espera pelo MiniStack escoar os eventos depois de cada janela; as quatro janelas
somam 40 s de carga.

Os parâmetros são flags do gerador, passadas por `ARGS`, por exemplo
`make load-test ARGS="-concurrency 8,32 -duration 1m"`:

| Flag | Padrão | O que é |
|---|---|---|
| `-concurrency` | `16,64` | níveis de concorrência; cada um passa por todas as fases |
| `-phases` | `mixed,contention` | fases, nesta ordem |
| `-warmup`, `-duration` | `5s`, `10s` | aquecimento, fora das medidas, e janela medida de cada fase |
| `-wallets`, `-hot-wallets` | `200`, `5` | carteiras da fase mista e da fase de contenção |
| `-sqs-share`, `-replay-share`, `-cross-share` | `20`, `5`, `5` | % das rodadas pela fila, com replay e com reenvio pelo outro canal |
| `-sqs-senders`, `-sqs-backlog` | `4`, `500` | goroutines que enviam as rodadas da fila e limite de mensagens enviadas e ainda não consumidas |
| `-initial-balance` | `10000000.00` | saldo inicial de cada carteira, em BRL |
| `-seed` | `1` | semente da mistura de operações |
| `-event-readers` | `8` | receptores concorrentes de `wallet-events.fifo` |
| `-sample-every` | `250ms` | intervalo de amostragem do `/metrics` das réplicas |
| `-drain-timeout` | `30m` | quanto esperar a fila e a outbox escoarem depois de cada janela |

## Ambiente

Uma execução, numa máquina só, com tudo no Docker Desktop. Cliente e servidor dividem as mesmas CPUs.

| Item | Valor |
|---|---|
| Máquina | Apple M5 (`Mac17,2`), 10 núcleos (4 de desempenho, 6 de eficiência), 16 GB de RAM |
| Sistema | macOS 26.6.2; na máquina rodavam só aplicativos de mesa (cerca de 0,7 núcleo de fundo) |
| Docker | Docker Desktop 4.53.0, Engine 29.0.1, Compose v2.40.3; a VM tem 10 CPUs e 7,65 GiB, kernel 6.12.54-linuxkit, logs `json-file` |
| Código medido | commit `8a78372` (serviço e gerador), migrations na versão 10 |
| Serviço | Go 1.25.6, imagem `distroless/static`; 3 réplicas iguais (`app-1..3`), cada uma com API, consumidor da fila, relay da outbox e resolvedor |
| Balanceador | `nginx:1.30.6-alpine`, round-robin, `keepalive 16` por worker para as réplicas, `worker_processes auto` |
| Banco | `postgres:18.6-alpine` com a configuração da imagem: `max_connections` 100, `shared_buffers` 128 MB, `synchronous_commit` on, `fsync` on |
| IdP | `quay.io/keycloak/keycloak:26.8.1` em `start-dev`, tokens de 5 minutos |
| SQS | `ministackorg/ministack:1.5.27` (Python 3.13, um processo) com `AUTH=true` |
| Gerador | Go 1.25.6, imagem `scratch`, no mesmo compose |
| Tracing | desligado: o compose base não define `OTEL_EXPORTER_OTLP_ENDPOINT` e o overlay `docker-compose.tracing.yml` não foi usado |

Configuração do serviço, toda nos padrões (o compose não muda nenhuma destas):

| Variável | Valor | Por que importa aqui |
|---|---|---|
| `DB_MAX_CONNS` | 20 por réplica (60 no total) | teto de transações simultâneas no banco |
| `DB_LOCK_TIMEOUT` × `DB_TX_MAX_ATTEMPTS` | 2s × 3 | espera máxima pelo lock de uma carteira antes do `503` |
| `HTTP_REQUEST_TIMEOUT` | 10s | prazo de uma requisição |
| `CONSUMER_MAX_GROUPS` | 8 por réplica | grupos FIFO (carteiras) processados em paralelo por réplica |
| `CONSUMER_WAIT_TIME`, `CONSUMER_VISIBILITY_TIMEOUT` | 20s, 30s | long polling e visibilidade da fila de entrada |
| `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE` | 500 ms, 50 | espera de um relay ocioso e eventos por passada |
| `LOG_LEVEL` | `info` | cada requisição e cada mensagem gera uma linha de log JSON |

## Metodologia

### Onde o gerador roda

O gerador é um container na rede do compose, como um provedor a mais. Ele fala com `http://nginx` (porta 80 da rede
interna, sem o encaminhamento de portas do Docker Desktop), lê `http://app-N:9090/metrics` das três réplicas, pede
tokens a `http://keycloak:8080` e usa o SQS do `http://ministack:4566`. Cliente e servidor estão no mesmo kernel da VM
do Docker Desktop. Por isso, as medidas que cruzam processos, como o `occurredAt` de um evento comparado com a hora do
envio, usam o mesmo relógio.

### Fases

Para cada nível de concorrência, nesta ordem, roda a fase **mista** e depois a de **contenção**. Cada fase:

1. Abre carteiras novas com 10.000.000,00 BRL: 200 na mista, 5 na de contenção. Saldos grandes garantem que nenhuma
   rejeição por saldo aconteça; a carga só manda sequências válidas.
2. Lê o `/metrics` das três réplicas e começa os workers.
3. Aquece por 5s, fora das medidas, e mede por 10s. Só entra nas medidas o que **começou** dentro da janela. A janela é
   curta de propósito. Cada fase termina esperando o MiniStack publicar os eventos que ela gerou, a umas poucas centenas
   por segundo, e a janela já dá dezenas de milhares de amostras HTTP. Com 10s de aquecimento e 30s de janela, a fase
   mista geraria uns 240 mil eventos e escoaria por mais de 15 minutos (ver [Análise](#análise)).
4. No fim da janela, os workers param. A operação em curso de cada um termina; nenhuma nova começa.
5. Espera escoar: toda mensagem enviada à fila consumida, toda operação com o evento de desfecho recebido e todo
   movimento com o seu `WalletBalanceChanged`.
6. Confere a integridade de todas as carteiras da fase e lê o `/metrics` de novo.

As fases não compartilham carteiras. O banco não é zerado entre elas: cresce ao longo da execução, como em produção.

### A carga

Cada nível de concorrência é o número de **workers HTTP** de laço fechado: um worker manda uma operação, espera a
resposta e só então manda a próxima. A cada rodada, o worker sorteia, com uma semente fixa por worker (o mesmo `-seed`
dá a mesma sequência de sorteios):

- a carteira, uniforme entre as da fase, e o provedor, `provider-a` ou `provider-b`;
- o formato da rodada, sempre uma sequência válida ([REGRAS §4 e §5](REGRAS_DE_NEGOCIO.md#4-operações)), com uma `BET`
  de 1,00 a 50,00:

| Formato | Peso | Operações |
|---|---|---|
| aposta e prêmio | 30% | `BET`, `WIN` da `BET` (0,01 até o dobro da aposta) |
| aposta perdida | 30% | `BET`, `LOSS` (0,00) |
| devolução | 10% | `BET`, `REFUND` da `BET` |
| aposta desfeita | 10% | `BET`, `ROLLBACK` da `BET` |
| prêmio desfeito | 10% | `BET`, `WIN` da `BET`, `ROLLBACK` do `WIN` |
| devolução desfeita | 10% | `BET`, `REFUND` da `BET`, `ROLLBACK` do `REFUND` |

- o canal: 20% das rodadas vão pela fila. Uma rodada pela fila é entregue a um de **4 senders**, goroutines à parte que
  mandam as mensagens dela em sequência, todas com `MessageGroupId` = `walletId`. A ordem do FIFO garante que uma
  referência chega antes da operação que a cita, então nenhuma operação espera referência. Se os senders estão ocupados,
  ou se já há 500 mensagens enviadas e ainda não consumidas (soma de `sqs_messages_total` das réplicas fora `retry`,
  amostrada a cada 250 ms), a rodada vai por HTTP. Assim o worker HTTP nunca espera o emulador, e a vazão HTTP mede o
  serviço. O relatório dá a fração que de fato foi pela fila.

Uma rodada HTTP é interrompida na primeira operação que não termina `PROCESSED`, para não mandar uma referência a algo
que não existe.

**Reenvios.** 5% das rodadas reenviam uma operação com a mesma chave pelo mesmo canal: por HTTP, logo depois da
resposta, esperando `200` com `idempotentReplay: true` e o mesmo `transactionId`; pela fila, como outra mensagem, com
outro `messageId`. Outros 5% reenviam pelo outro canal. Numa rodada HTTP, uma operação já respondida volta pela fila.
Numa rodada pela fila, a `BET` vai também por HTTP, depois de um atraso sorteado de 0 a 400 ms, e corre com o
consumidor; os dois desfechos acontecem. Todo reenvio que chega precisa virar exatamente uma duplicata contada pelo
serviço, e nunca uma transação nova.

### O que cada número mede

Todo o cálculo é inteiro, como no serviço: durações em microssegundos, percentis por posto mais próximo sobre todas as
amostras ordenadas, e taxas em décimos por segundo (`contagem × 10 × 10⁹ / janela em ns`). A guarda de float do
`internal/archtest` cobre o gerador.

| Medida | Como |
|---|---|
| vazão HTTP | operações HTTP que começaram na janela e tiveram resposta, ÷ 10s; por tipo também |
| vazão SQS enviada | operações pela fila cujo `SendMessage` começou na janela e deu certo, ÷ 10s |
| vazão SQS decidida | operações pela fila decididas pelo consumidor com `occurredAt` dentro da janela, ÷ 10s |
| latência HTTP | do `http.Client.Do` até ler o corpo inteiro, no cliente; inclui o nginx. Só entram as respostas (`2xx` e `422`); as falhas ficam na contagem de erros. Replays e corridas com a fila têm linhas próprias |
| latência SQS | `SendMessage`; envio → decisão (`occurredAt` do evento de desfecho, gravado pelo serviço com milissegundos truncados, menos a hora do envio); envio → evento de desfecho recebido pelo gerador |
| atraso do evento | para todo evento das carteiras da fase com `occurredAt` na janela: recebido − `occurredAt`, que é o `processedAt` da transação. Ele se divide em publicação (`SentTimestamp` do SQS − `occurredAt`: a outbox e o relay, mais a ingestão do emulador) e entrega (recebido − `SentTimestamp`: a fila do emulador e o leitor do gerador) |
| conflitos e erros | deltas das três réplicas entre o início e o fim da janela: `db_tx_retries_total` por motivo, `wagering_duplicates_total` por canal e origem, `http_requests_total` 409 e 5xx, `sqs_messages_total`, `sqs_dlq_total`, `outbox_publish_total`; no cliente, cada status e `code` de problem+json, e se veio `Retry-After` |
| lag da outbox | `outbox_pending_events` e `outbox_oldest_pending_age_seconds` das três réplicas, amostrados a cada 250 ms (p95 e máximo, pelo maior valor entre as réplicas em cada amostra). Cada réplica recalcula esses gauges a cada 5s, então amostrar mais rápido não dá mais resolução: os números são uma escada de 5 em 5 segundos |
| escoamento | tempo do fim da janela até tudo escoar, e eventos publicados nesse tempo |
| médias do servidor | `wagering_processing_seconds` e `http_request_seconds` (soma ÷ contagem na janela) |

O leitor de eventos tem oito receptores com long polling de 1s, até 10 mensagens por chamada e um `DeleteMessageBatch`
por lote. Ele usa o root do emulador, e não o usuário `events-reader`. O MiniStack recusa `DeleteMessageBatch` a esse
usuário, porque avalia a ação de lote ao pé da letra, enquanto a AWS a autoriza por `sqs:DeleteMessage`. Apagar uma
mensagem por chamada custava ao emulador uma passada pela fila inteira a cada evento, e o leitor ficava para trás: numa
execução anterior, a fase mista não escoou em 30 minutos. O atraso de entrega ainda inclui o próprio leitor, então é um
limite superior para um consumidor dedicado.

### Integridade

Ao fim de cada fase, depois do escoamento:

- Toda operação sem desfecho conhecido (erro de transporte, resposta de falha, mensagem sem evento) é consultada por
  `GET /providers/{p}/wagering/transactions/{externalTransactionId}`. Uma requisição recusada que aparece gravada
  reprova.
- Toda carteira reconcilia consistente. Nenhum saldo é negativo, nem na carteira nem em nenhum lançamento.
- O saldo gravado é igual ao saldo inicial mais o efeito de cada operação `PROCESSED`, calculado pelo gerador.
- O ledger tem exatamente um lançamento por movimento processado, mais a abertura. O conjunto de `transactionId` dos
  lançamentos é exatamente o das operações que moveram dinheiro, sem repetição.
- Cada operação decidida tem exatamente um evento de desfecho (`eventId` distinto). Cada movimento tem um
  `WalletBalanceChanged`, e cada carteira, uma abertura.
- O total de decisões contado pelas réplicas (`wagering_transactions_total`) é o número de operações decididas. As
  duplicatas por chave (`wagering_duplicates_total{source="idempotency"}`) são exatamente os reenvios que chegaram; as
  da inbox são reportadas à parte, porque uma reentrega legítima do SQS também conta ali.
- Nenhuma operação é rejeitada (a carga só tem sequências válidas), nenhuma mensagem vai para a DLQ e nenhuma
  reconciliação diverge.

## Resultados

Execução `fe8a2996a4ff`, em 2026-10-19, das 13:31 às 13:56 UTC. O JSON completo é o `tmp/load/load-<runId>.json` de uma
execução; os números abaixo saem dele. Latências em milissegundos, com duas casas.

### Vazão

Operações que começaram dentro da janela de 10s, por segundo:

| Fase | Concorrência | Carteiras | Total | HTTP | SQS enviadas | SQS decididas | Fração pela fila | Rodadas na janela |
|---|---|---|---|---|---|---|---|---|
| mista | 16 | 200 | 3.204,7 | 3.101,6 | 103,1 | 102,0 | 3,2% | 14.549 |
| contenção | 16 | 5 | 2.101,7 | 2.022,9 | 78,8 | 60,5 | 3,7% | 9.542 |
| mista | 64 | 200 | 3.744,1 | 3.734,5 | 9,6 | 9,8 | 0,3% | 17.010 |
| contenção | 64 | 5 | 2.255,2 | 2.213,6 | 41,6 | 57,2 | 1,8% | 10.249 |

Por tipo, na fase mista com 16: `BET` 1.408,4/s, `WIN` 568,7/s, `LOSS` 421,5/s, `REFUND` 282,4/s e `ROLLBACK` 420,6/s,
as proporções da mistura. As rodadas sorteadas para a fila que foram por HTTP, porque os senders ou a fila de entrada
estavam cheios, somam 2.457, 1.487, 3.358 e 1.826, na ordem da tabela. Na contenção com 64, o consumidor decidiu na
janela mais mensagens do que foram enviadas nela, porque ainda processava as do aquecimento.

### Latência HTTP

No cliente, de todas as respostas, mais as médias do lado do servidor (`http_request_seconds` e
`wagering_processing_seconds`, o caso de uso):

| Fase | Concorrência | n | p50 | p95 | p99 | máx | média | média no servidor | média do caso de uso |
|---|---|---|---|---|---|---|---|---|---|
| mista | 16 | 31.016 | 4,46 | 9,68 | 16,73 | 52,93 | 5,12 | 4,57 | 4,54 |
| contenção | 16 | 20.229 | 5,96 | 19,87 | 29,32 | 124,17 | 7,88 | 7,43 | 7,50 |
| mista | 64 | 37.345 | 15,61 | 32,58 | 46,34 | 152,33 | 17,05 | 16,07 | 16,26 |
| contenção | 64 | 22.136 | 17,47 | 91,75 | 189,76 | 599,63 | 28,82 | 27,96 | 28,44 |

Por tipo (p50 / p99):

| Fase | Concorrência | `BET` | `WIN` | `LOSS` | `REFUND` | `ROLLBACK` |
|---|---|---|---|---|---|---|
| mista | 16 | 4,42 / 16,37 | 4,83 / 18,12 | 3,48 / 13,76 | 4,86 / 21,30 | 4,86 / 16,76 |
| contenção | 16 | 5,50 / 27,65 | 6,70 / 34,83 | 5,93 / 31,32 | 6,37 / 29,34 | 6,59 / 29,96 |
| mista | 64 | 15,28 / 44,60 | 16,79 / 49,11 | 12,94 / 43,73 | 16,67 / 48,85 | 16,96 / 48,47 |
| contenção | 64 | 16,52 / 186,56 | 18,25 / 190,07 | 17,82 / 201,39 | 18,74 / 191,13 | 18,12 / 196,54 |

O replay da mesma chave, respondido pela leitura da transação gravada sem o lock da carteira, ficou em p50/p99 de
0,86/4,00 (mista 16), 0,63/2,20 (contenção 16), 2,42/12,33 (mista 64) e 1,39/13,05 (contenção 64), com 447 a 876
amostras por fase.

### Fila (SQS)

| Fase | Concorrência | n | `SendMessage` p50 / p95 / p99 | envio → decisão p50 / p95 / p99 / máx |
|---|---|---|---|---|
| mista | 16 | 1.020 | 29,61 / 66,50 / 104,29 | 62,00 / 139,71 / 218,81 / 257,92 |
| contenção | 16 | 776 | 53,85 / 81,31 / 113,63 | 3.381 / 5.005 / 5.191 / 5.357 |
| mista | 64 | 94 | 334,07 / 403,00 / 439,49 | 599 / 1.002 / 1.399 / 1.399 |
| contenção | 64 | 408 | 5,15 / 57,86 / 89,14 | 5.098 / 7.650 / 7.897 / 8.000 |

O envio → decisão não muda com o tipo da operação. Na fase mista com 16, o p50 vai de 47 ms (`BET`) a 86 ms
(`ROLLBACK`); na contenção com 16, de 3.360 a 3.486 ms.

### Conflitos, erros e duplicatas

| Fase | Concorrência | Erros no cliente | `db_tx_retries_total` | `409` / `503` | DLQ | Duplicatas por chave (esperadas = contadas) | Da inbox | Corridas HTTP × fila (HTTP ganhou / fila ganhou) |
|---|---|---|---|---|---|---|---|---|
| mista | 16 | 0 | 0 | 0 / 0 | 0 | 1.266 | 0 | 0 / 34 |
| contenção | 16 | 0 | 0 | 0 / 0 | 0 | 842 | 0 | 19 / 0 |
| mista | 64 | 0 | 0 | 0 / 0 | 0 | 1.275 | 0 | 0 / 2 |
| contenção | 64 | 0 | 0 | 0 / 0 | 0 | 1.275 | 0 | 23 / 0 |

Nenhuma requisição falhou, nenhuma primeira tentativa voltou como replay (o nginx não precisou repetir nenhuma), e
nenhuma transação de banco foi refeita por conflito.

### Outbox e atraso dos eventos

| Fase | Concorrência | Publicados na janela | Pendentes no fim da janela | Idade do mais antigo (máx., gauge) | Escoamento | Publicados no escoamento |
|---|---|---|---|---|---|---|
| mista | 16 | 7.150 (715/s) | 54.101 | 10,3 s | 4 min 2 s | 68.301 (281,7/s) |
| contenção | 16 | 789 (79/s) | 42.433 | 11,5 s | 4 min 55 s | 55.641 (188,8/s) |
| mista | 64 | 850 (85/s) | 97.297 | 14,6 s | 8 min 30 s | 100.426 (197,0/s) |
| contenção | 64 | 855 (86/s) | 50.084 | 12,0 s | 6 min 37 s | 62.878 (158,3/s) |

O atraso de cada evento com `occurredAt` na janela, em segundos, dividido no envio ao SQS:

| Fase | Concorrência | Eventos | Publicação p50 / p95 / p99 | Entrega p50 / p95 / p99 (ms) | Total p50 / p99 / máx |
|---|---|---|---|---|---|
| mista | 16 | 59.756 | 102,3 / 223,0 / 238,3 | 31,9 / 93,7 / 129,2 | 102,3 / 238,4 / 242,5 |
| contenção | 16 | 38.852 | 200,1 / 280,3 / 289,5 | 10,0 / 28,3 / 35,2 | 200,1 / 289,6 / 292,5 |
| mista | 64 | 69.796 | 338,9 / 491,6 / 505,5 | 47,9 / 128,4 / 170,2 | 339,0 / 505,6 / 509,9 |
| contenção | 64 | 42.346 | 253,9 / 380,4 / 390,5 | 12,5 / 48,1 / 83,3 | 253,9 / 390,5 / 394,8 |

Quase todo o atraso está na publicação: o evento espera na outbox até o relay conseguir mandá-lo ao emulador. Depois de
publicado, o leitor o recebe em dezenas de milissegundos.

### Integridade

Aprovada nas quatro fases:

| Fase | Concorrência | Carteiras consistentes | Saldo negativo | Saldo ≠ operações | Lançamentos = movimentos + aberturas | Decididas (contadas pelas réplicas) | Rejeitadas | Decisões duplicadas | Eventos faltando |
|---|---|---|---|---|---|---|---|---|---|
| mista | 16 | 200/200 | 0 | 0 | 38.090 = 37.890 + 200 | 43.948 (43.948) | 0 | 0 | 0 |
| contenção | 16 | 5/5 | 0 | 0 | 26.275 = 26.270 + 5 | 30.495 (30.495) | 0 | 0 | 0 |
| mista | 64 | 200/200 | 0 | 0 | 47.089 = 46.889 + 200 | 54.271 (54.271) | 0 | 0 | 0 |
| contenção | 64 | 5/5 | 0 | 0 | 29.709 = 29.704 + 5 | 34.419 (34.419) | 0 | 0 | 0 |

O gerador recebeu 304.706 eventos distintos, sem nenhum reentregue nem malformado. Nenhuma operação ficou sem desfecho
conhecido, então nenhuma consulta foi necessária. As réplicas não registraram nenhuma linha `WARN` ou `ERROR` durante a
execução. No fim, a outbox estava vazia.

## Análise

### Onde vai o tempo de uma requisição HTTP

A média no cliente (5,12 ms na mista com 16) fica a meio milissegundo da média do servidor (4,57 ms), que é quase toda o
caso de uso (4,54 ms). O nginx, a rede da VM e o cliente somam cerca de 0,5 ms. O resto é a transação no PostgreSQL:
travar a carteira, gravar a transação, o lançamento, o saldo, os eventos da outbox e o commit com `fsync`. O `LOSS`, que
não gera lançamento nem `WalletBalanceChanged`, é o tipo mais rápido. `WIN`, `REFUND` e `ROLLBACK`, que buscam a
referência, são os mais lentos.

Durante as janelas, o PostgreSQL usa 350–400% de CPU, cada réplica uns 67%, o nginx 20% e o gerador 30%. O gargalo é o
banco. De 16 para 64 clientes, a vazão HTTP sobe só 20% (3.102 → 3.735/s), enquanto a latência média triplica (5,1 →
17,1 ms): o sistema já estava perto da saturação com 16. As 64 requisições simultâneas disputam as 60 conexões do pool
(20 por réplica) e a CPU do banco, e a diferença vira fila. A média do caso de uso (16,26 ms) inclui a espera por uma
conexão do pool.

### Contenção: o lock serializa e nada se perde

Com 5 carteiras, cada uma recebe cerca de 400 operações por segundo, uma depois da outra: o `SELECT … FOR NO KEY UPDATE`
da carteira põe os escritores em fila. Com 16 clientes, a fila por carteira é curta, e o p99 sobe de 17 para 29 ms. Com
64, umas 13 requisições esperam por carteira, e a cauda cresce: p95 de 92 ms, p99 de 190 ms, máximo de 600 ms. Mesmo o
pior caso ficou longe do `DB_LOCK_TIMEOUT` de 2s. Por isso não houve nenhum retry de transação nem nenhum `503`, e a
vazão total (2.000–2.200/s) é a de 5 filas serializadas, cada uma com uma transação de ~2,5 ms por vez.

A disputa é segura por construção. Todo movimento de uma carteira acontece com o lock dela, o `UPDATE` do saldo confere
a versão lida, e o banco recusa saldo negativo e lançamento que não encadeia no anterior
([ARCHITECTURE §7](../ARCHITECTURE.md#7-concorrência)). Os números confirmam: 26.270 e 29.704 movimentos sobre 5
carteiras, com o saldo de cada uma igual, centavo a centavo, ao saldo inicial mais o efeito calculado pelo gerador, e o
ledger com exatamente um lançamento por movimento. Os reenvios no meio da disputa, inclusive a mesma `BET` correndo por
HTTP e pela fila, viraram exatamente uma duplicata cada.

### A outbox, antes e depois da correção do claim

Nas execuções curtas de validação, ainda com o claim antigo, a fase mista com 16 clientes acumulou 46 mil eventos
pendentes. O `EXPLAIN ANALYZE` do claim levou 9,7 s: o planejador, com estatísticas de uma tabela que acabara de
crescer, sondava o índice errado para cada candidato e descartava 42 milhões de linhas. Os três relays repetiam essa
consulta sem parar. O PostgreSQL ficou em 437% de CPU com as réplicas em 45%, e a outbox publicou uns 20 eventos por
segundo durante a carga (300 numa janela de 15s). O claim foi reescrito no commit `4707f23` para pular de carteira em
carteira pelo índice `(aggregate_id, seq)`. O benchmark daquele commit mede 1–3 ms com 50 mil pendentes, contra
90–1.479 ms antes.

Nesta execução, a fase mista com 16 publicou 715 eventos por segundo durante a janela, com o banco ocupado servindo
3.100 requisições por segundo. No escoamento, com até 97 mil pendentes, o PostgreSQL ficou em ~14% de CPU e as réplicas
em ~2%. A publicação parou de depender do banco e passou a esperar só o emulador.

### Características do desenho, não defeitos

**Um evento por carteira em cada passada do relay.** O claim pega só a cabeça de cada carteira, o evento pendente mais
antigo dela, para que um consumidor receba os eventos de uma carteira na ordem do `walletVersion`
([ARCHITECTURE §12](../ARCHITECTURE.md#12-outbox-e-eventos)). Com muitas carteiras, uma passada leva até 50 eventos; com
5 carteiras quentes, leva no máximo 5. Por isso as fases de contenção publicaram ~80 eventos/s durante a janela, contra
715/s da fase mista com 16, e é por isso que uma carteira que gera mais eventos por segundo do que o relay faz passadas
acumula atraso. É o preço da ordem por carteira, e ele foi aceito de propósito. Para subir o teto sem perder a ordem, o
claim poderia levar uma sequência de eventos consecutivos de cada carteira e publicá-los no mesmo lote, já que o FIFO
preserva a ordem dentro do grupo.

Na fase mista com 64, a publicação na janela também caiu para 85/s, mas por outro motivo. O relay usa o mesmo pool de 20
conexões por réplica que as requisições. Com 64 clientes, o pool fica ocupado, e cada claim e cada settle esperam na
mesma fila das requisições. Sob carga máxima, a outbox perde essa disputa para o caminho síncrono e só recupera o
atraso quando a carga cai.

**O consumidor FIFO processa uma mensagem por grupo de cada vez.** O `MessageGroupId` é a carteira, então as mensagens
de uma carteira são decididas em série, na ordem de envio. Com 5 carteiras, no máximo 5 mensagens estão em processamento
em todo o sistema, cada uma esperando o lock da carteira atrás das requisições HTTP. Isso dá ~60 decisões por segundo
pela fila (60,5 e 57,2/s nas duas contenções) e um envio → decisão de 3,4 s e 5,1 s no p50: o tempo que uma mensagem
passa na fila atrás das outras da mesma carteira, com o gerador mantendo até 500 mensagens pendentes. Com 200 carteiras,
o mesmo caminho decide em 62 ms no p50.

### O MiniStack é o teto do caminho da fila e dos eventos

O container do MiniStack fica em 100% de CPU, um núcleo inteiro, da primeira à última fase. É um processo Python, e o
SQS dele (`ministack/services/sqs.py`, versão 1.5.27) tem custo linear por chamada:

- todo `SendMessage` numa fila FIFO reconstrói o dicionário de deduplicação inteiro (`_prune_dedup`), com os envios dos
  últimos 5 minutos. A centenas de envios por segundo, cada envio percorre dezenas de milhares de entradas, e o custo
  por segundo cresce com o quadrado da taxa;
- `DeleteMessage`, e cada entrada de `DeleteMessageBatch`, reconstroem a lista de mensagens da fila; o `ReceiveMessage`
  numa fila FIFO a percorre duas vezes.

O teto que sobra é de algumas centenas de mensagens por segundo, divididas entre o relay, o consumidor da fila de
entrada, os senders e o leitor do gerador. O serviço produz bem mais do que isso: cada operação HTTP gera um ou dois
eventos, e a fase mista com 16 gerou uns 6.000 eventos por segundo. Por isso:

- a outbox acumula durante a janela e só esvazia minutos depois, a 160–280 eventos por segundo, e o atraso de publicação
  vai a minutos no p50;
- os senders esperam 30–330 ms por `SendMessage`, e a fração pela fila fica em 0,3–3,7% em vez dos 20% pedidos;
- uma primeira versão do leitor, que apagava um evento por chamada, ficava para trás do relay. O atraso que ela deixava
  tornava cada chamada mais cara, e a fase mista não escoou em 30 minutos.

Nada disso perde ou duplica nada: a integridade conferiu cada evento e cada decisão. Os tempos absolutos da fila e dos
eventos, porém, são do emulador. No SQS da AWS, uma chamada não custa proporcionalmente ao tamanho da fila, e os lotes
de 10 que o relay já manda não ficariam mais caros à medida que a fila cresce.

### A decisão de 30,1 s das execuções de validação

Numa execução curta de validação, ainda com o claim antigo, uma única mensagem levou 30,1 s do envio à decisão. As
outras 4.306 daquela fase levaram no máximo 235 ms no p99. O valor coincide com o `VisibilityTimeout` de 30s: tudo
indica que a mensagem foi recebida e não foi nem apagada nem devolvida, e reapareceu quando a visibilidade venceu. A
explicação mais provável é uma resposta de `ReceiveMessage` que se perdeu com o emulador saturado. Os logs daquela
execução não foram guardados, então a causa não está confirmada. Seja qual for, a reentrega é segura: a inbox a
reconhece como a mesma mensagem, e a idempotência impede uma segunda decisão. O caso não se repetiu nesta execução,
em que as réplicas não registraram nenhum `WARN`. Os máximos de envio → decisão foram 0,26 s, 5,4 s, 1,4 s e 8,0 s,
todos explicados pela fila por carteira descrita acima.

## Limitações

- **Um host só.** O gerador, o nginx, as três réplicas, o Postgres, o Keycloak e o MiniStack dividem as 10 CPUs e os
  7,65 GiB da VM do Docker Desktop. A vazão é a de uma máquina de desenvolvimento, não a de um cluster. A latência
  inclui o escalonamento entre containers que disputam CPU. Não há rede de verdade entre eles.
- **MiniStack no lugar do SQS.** Todo o caminho da fila e dos eventos (`SendMessage`, consumo, publicação da outbox e
  entrega dos eventos) mede o emulador, um processo Python com custo linear no tamanho da fila e da janela de
  deduplicação por chamada. No SQS de verdade, esses números seriam outros. O que vale levar daqui é a correção sob
  carga e a separação entre o que é do serviço e o que é do emulador, não os tempos absolutos da fila.
- **Uma execução por configuração.** Não há repetições nem intervalo de confiança; a variação entre execuções não foi
  medida.
- **Gerador de laço fechado.** Cada worker só manda a próxima operação depois da resposta, então a vazão é a que N
  clientes conseguem. Como todo gerador de laço fechado, ele sofre de *coordinated omission*: quando o serviço
  desacelera, o gerador também manda menos, e a cauda medida fica menor do que a de uma carga de taxa fixa. Uma taxa
  fixa acima da capacidade mostraria filas maiores do que as daqui.
- **A fila não chega a 20%.** Os senders esperam o emulador, e as rodadas que os encontram ocupados vão por HTTP. A
  fração pela fila de cada fase está nos resultados.
- **O atraso de entrega dos eventos é um limite superior.** Ele inclui o leitor do gerador, que divide o emulador com o
  relay e o consumidor. O leitor usa o root do MiniStack para apagar em lote (ver [Metodologia](#metodologia)).
- **Janelas de 10s.** Bastam para os percentis, com dezenas de milhares de amostras, mas não mostram o comportamento de
  minutos de carga contínua, como autovacuum, checkpoints e crescimento de índices.
- **Só sequências válidas.** Rejeições de negócio, referências pendentes e o resolvedor não entram na carga; a correção
  deles é provada pelos testes de integração e e2e.
- **Tracing desligado e logs em `info`.** Com o tracing ligado, o custo dos spans e do exportador entraria nos números.
  Cada requisição e cada mensagem gera uma linha de log JSON, que o Docker grava em disco.
- **Banco com a configuração da imagem.** Não houve tuning do PostgreSQL. O banco cresce ao longo da execução, porque
  cada fase abre carteiras novas e nada é apagado.
