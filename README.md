# backend-challenge-go

Serviço de carteiras e apostas em Go. Ele movimenta saldos de jogadores por HTTP e por SQS com as mesmas garantias:
dinheiro em inteiros (nunca float), idempotência persistida no banco, ledger append-only e invariantes financeiras
impostas pelo próprio PostgreSQL. A correção não depende de nenhuma instância: três réplicas idênticas se coordenam só
pelo banco (lock por carteira, índices únicos e leases).

Cada réplica tem quatro componentes, ligáveis por variável de ambiente: a API HTTP, o consumidor da fila
`wager-transactions.fifo`, o publicador da outbox (eventos em `wallet-events.fifo`) e o resolvedor de referências
pendentes. A autenticação é OAuth 2.0 `client_credentials` contra um Keycloak; o SQS roda localmente no MiniStack.

- Regras de negócio, com o resultado observável de cada uma e os testes que a provam:
  [`docs/REGRAS_DE_NEGOCIO.md`](docs/REGRAS_DE_NEGOCIO.md)
- Decisões técnicas, limitações e interpretações: [`ARCHITECTURE.md`](ARCHITECTURE.md)
- Enunciado original: [`docs/CHALLENGE.md`](docs/CHALLENGE.md)

## Sumário

1. [Pré-requisitos](#pré-requisitos)
2. [Subir o ambiente](#subir-o-ambiente)
3. [Variáveis de ambiente](#variáveis-de-ambiente)
4. [Filas e provisionamento](#filas-e-provisionamento)
5. [Migrations](#migrations)
6. [Autenticação](#autenticação)
7. [Exemplos de chamadas](#exemplos-de-chamadas)
8. [Contrato de erros](#contrato-de-erros)
9. [Testes](#testes)
10. [Observabilidade](#observabilidade)

## Pré-requisitos

| Ferramenta | Versão | Para quê |
|---|---|---|
| Docker Desktop (ou Docker Engine) com Compose v2 | recente | ambiente local e testes de integração e e2e |
| Go | 1.25.6, a do `go.mod` e do `Dockerfile` (uma mais nova também compila) | rodar os testes e as ferramentas fora do Docker |
| gcc (só no Linux) | qualquer | race detector dos alvos com `-race` |
| make, curl | qualquer | atalhos do `Makefile` e exemplos |
| jq, uuidgen | qualquer | só para os exemplos de chamadas |
| golangci-lint | v2.15.0 (opcional) | `make lint` |

Não é preciso ter o AWS CLI: o serviço `aws` do compose traz um, dentro da rede do compose e com as credenciais
geradas pelo provisionamento (ver [Filas e provisionamento](#filas-e-provisionamento)). O Docker precisa estar
rodando para o compose, para os testes de integração e para o e2e.

Os blocos de comando abaixo foram executados nesta ordem, num clone limpo, e as saídas mostradas são as reais (tokens
encurtados com `…`). Eles funcionam em bash e em zsh e não têm comentários, porque o zsh interativo não aceita `#`
sem `setopt interactivecomments`.

## Subir o ambiente

```sh
docker compose up --build
```

Esse comando prende o terminal nos logs (Ctrl+C para tudo). Para subir sem prender o terminal, use
`docker compose up --build -d`, que é o que `make up` faz. Na primeira vez o compose baixa as imagens e compila o
binário; com o cache do Docker, as subidas seguintes levam segundos.

| Serviço | Imagem | Porta no host | O que é |
|---|---|---|---|
| `postgres` | `postgres:18.6-alpine` | `127.0.0.1:5432` | banco; o script `deploy/postgres/initdb/00-roles.sql` cria os papéis `wallet_owner` e `wallet_app` |
| `keycloak` | `quay.io/keycloak/keycloak:26.8.1` | `127.0.0.1:8080` | IdP, com o realm `wagering` importado de `deploy/keycloak/realm-wagering.json` |
| `ministack` | `ministackorg/ministack:1.5.27` | `127.0.0.1:4566` | SQS e IAM locais, com `AUTH=true` |
| `migrate` | `backend-challenge-go/wallet:local` | — | one-shot: aplica as migrations como `wallet_owner` e termina |
| `aws-provision` | `backend-challenge-go/wallet:local` | — | one-shot: cria filas, redrive e usuários IAM e grava o arquivo de credenciais |
| `app-1`, `app-2`, `app-3` | `backend-challenge-go/wallet:local` | `127.0.0.1:8081`–`8083` (API) e `127.0.0.1:9091`–`9093` (`/metrics`) | as três réplicas do serviço |
| `nginx` | `nginx:1.30.6-alpine` | `127.0.0.1:8000` | balanceador round-robin na frente das réplicas; é a porta dos exemplos |
| `aws` | `amazon/aws-cli:2.38.1` | — | perfil `tools`: não sobe com o `up`, só com `docker compose run --rm aws …` |

A imagem do serviço é construída uma vez, pelo `migrate`, e usada por todos os containers que rodam um comando dele.
As réplicas só sobem depois que `postgres`, `keycloak` e `ministack` ficam saudáveis e os dois one-shots terminam com
sucesso.

**Conferir que subiu.** `postgres`, `keycloak`, `ministack` e `nginx` ficam `healthy`. As réplicas ficam `Up`, sem
estado de saúde: a imagem distroless não tem shell nem cliente HTTP para um healthcheck, e o nginx as checa
passivamente. Os one-shots ficam `Exited (0)`. Os nomes dos containers levam como prefixo o nome do diretório do
clone (aqui, `wallet-r1`).

```sh
docker compose ps -a
```

```text
NAME                        IMAGE                               COMMAND                  SERVICE         CREATED          STATUS                      PORTS
wallet-r1-app-1-1           backend-challenge-go/wallet:local   "/usr/local/bin/wall…"   app-1           23 seconds ago   Up 6 seconds                127.0.0.1:8081->8080/tcp, 127.0.0.1:9091->9090/tcp
wallet-r1-app-2-1           backend-challenge-go/wallet:local   "/usr/local/bin/wall…"   app-2           23 seconds ago   Up 6 seconds                127.0.0.1:8082->8080/tcp, 127.0.0.1:9092->9090/tcp
wallet-r1-app-3-1           backend-challenge-go/wallet:local   "/usr/local/bin/wall…"   app-3           23 seconds ago   Up 6 seconds                127.0.0.1:8083->8080/tcp, 127.0.0.1:9093->9090/tcp
wallet-r1-aws-provision-1   backend-challenge-go/wallet:local   "/usr/local/bin/prov…"   aws-provision   23 seconds ago   Exited (0) 16 seconds ago   
wallet-r1-keycloak-1        quay.io/keycloak/keycloak:26.8.1    "/opt/keycloak/bin/k…"   keycloak        23 seconds ago   Up 22 seconds (healthy)     127.0.0.1:8080->8080/tcp
wallet-r1-migrate-1         backend-challenge-go/wallet:local   "/usr/local/bin/migr…"   migrate         23 seconds ago   Exited (0) 16 seconds ago   
wallet-r1-ministack-1       ministackorg/ministack:1.5.27       "python -m ministack"    ministack       23 seconds ago   Up 22 seconds (healthy)     127.0.0.1:4566->4566/tcp
wallet-r1-nginx-1           nginx:1.30.6-alpine                 "/docker-entrypoint.…"   nginx           23 seconds ago   Up 6 seconds (healthy)      127.0.0.1:8000->80/tcp
wallet-r1-postgres-1        postgres:18.6-alpine                "docker-entrypoint.s…"   postgres        23 seconds ago   Up 22 seconds (healthy)     127.0.0.1:5432->5432/tcp
```

A readiness de cada réplica confere o banco e o SQS; pelo nginx, responde uma réplica qualquer:

```sh
curl -s localhost:8000/health/ready
```

```json
{"status":"ok","checks":{"postgres":"ok","sqs":"ok"}}
```

**Derrubar.**

```sh
docker compose down -v
```

O `-v` apaga os volumes, e é ele que garante um recomeço limpo. O script de papéis do Postgres só roda num volume
vazio, e um volume antigo guarda o schema e os dados de antes (inclusive a carteira dos exemplos, que então responde
`409`). O volume `aws-shared` guarda o arquivo de credenciais, que o provisionamento refaz a cada `up` de qualquer
forma, porque o MiniStack guarda filas, usuários e chaves só em memória. O Keycloak não tem volume: o realm vive no
container e é importado de novo sempre que o container é recriado, o que o `down` faz. Por isso, depois de mudar o
realm, um `up` que reaproveite o container não basta.

## Variáveis de ambiente

A configuração é lida do ambiente e validada inteira na partida. Todos os problemas aparecem numa única mensagem, e o
processo sai com código 2 sem subir nada. Uma variável vazia vale o mesmo que ausente. Durações usam o formato do Go
(`500ms`, `2s`, `15m`) e precisam ser positivas; booleanos aceitam o que `strconv.ParseBool` aceita (`true`/`false`,
`1`/`0`, `t`/`f`); inteiros têm mínimo 1. O [`.env.example`](.env.example) traz todas, com os valores do compose.

| Variável | Padrão | Descrição |
|---|---|---|
| `APP_INSTANCE_ID` | hostname | identifica a réplica nos logs e no `application_name` do Postgres |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` ou `error`, sem diferenciar maiúsculas |
| `SHUTDOWN_DRAIN_DELAY` | `2s` | pausa entre a readiness ir a 503 e os servidores pararem; sem a API, não há pausa |
| `SHUTDOWN_TIMEOUT` | `60s` | prazo do shutdown inteiro; precisa cobrir a soma dos passos (nota 1) |
| `HTTP_ENABLED` | `true` | liga a API HTTP, que também serve a readiness |
| `HTTP_ADDR` | `:8080` | endereço da API |
| `HTTP_READ_HEADER_TIMEOUT` | `5s` | prazo para ler os headers |
| `HTTP_READ_TIMEOUT` | `10s` | prazo para ler a requisição |
| `HTTP_WRITE_TIMEOUT` | `15s` | prazo para escrever a resposta |
| `HTTP_IDLE_TIMEOUT` | `60s` | prazo de conexão ociosa |
| `HTTP_SHUTDOWN_TIMEOUT` | `10s` | espera pelas requisições em voo no shutdown |
| `HTTP_REQUEST_TIMEOUT` | `10s` | prazo do trabalho de uma requisição; esgotado, a resposta é `503` + `Retry-After` (nota 2) |
| `HTTP_MAX_BODY_BYTES` | `65536` | tamanho máximo do corpo; acima, `413` |
| `HTTP_MAX_HEADER_BYTES` | `16384` | tamanho máximo dos headers |
| `ADMIN_ADDR` | `:9090` | servidor admin, só com `GET /metrics` |
| `DATABASE_URL` | **obrigatória** | DSN do PostgreSQL, como `wallet_app` |
| `DB_MAX_CONNS` | `20` | tamanho do pool |
| `DB_LOCK_TIMEOUT` | `2s` | `lock_timeout` de cada conexão: quanto um escritor espera pelo lock de uma carteira; menor que `DB_STATEMENT_TIMEOUT` (nota 3) |
| `DB_STATEMENT_TIMEOUT` | `5s` | `statement_timeout` de cada conexão; maior que `DB_LOCK_TIMEOUT` |
| `DB_TX_MAX_ATTEMPTS` | `3` | tentativas de uma transação em conflito (serialização, deadlock, lock timeout, versão, corrida de índice único) |
| `OIDC_ISSUER` | **obrigatória** | `iss` exigido nos tokens |
| `OIDC_JWKS_URL` | **obrigatória** | URL do JWKS do realm |
| `OIDC_AUDIENCE` | `wagering-api` | `aud` exigido |
| `OIDC_CLOCK_SKEW` | `30s` | tolerância de relógio para `exp` e `nbf` |
| `SQS_INPUT_QUEUE` | `wager-transactions.fifo` | fila de entrada |
| `SQS_INPUT_DLQ` | `wager-transactions-dlq.fifo` | DLQ da entrada, para onde o consumidor move o que não pode processar |
| `SQS_EVENTS_QUEUE` | `wallet-events.fifo` | fila dos eventos de saída |
| `CONSUMER_ENABLED` | `true` | liga o consumidor SQS |
| `CONSUMER_NAME` | `wager-transactions` | nome do consumidor na inbox |
| `CONSUMER_MAX_GROUPS` | `8` | grupos (carteiras) processados em paralelo |
| `CONSUMER_VISIBILITY_TIMEOUT` | `30s` | visibilidade das mensagens recebidas |
| `CONSUMER_PROCESSING_TIMEOUT` | `15s` | prazo por mensagem; menor que a visibilidade e, com o consumidor ligado, maior que 5s (nota 4) |
| `CONSUMER_DRAIN_TIMEOUT` | `20s` | espera pelas mensagens em andamento no shutdown |
| `CONSUMER_WAIT_TIME` | `20s` | long polling, de `1s` a `20s` |
| `CONSUMER_MAX_RECEIVES` | `10` | recebimento a partir do qual uma falha transitória move a mensagem para a DLQ como `RETRIES_EXHAUSTED` |
| `OUTBOX_RELAY_ENABLED` | `true` | liga o publicador da outbox |
| `OUTBOX_POLL_INTERVAL` | `500ms` | intervalo de polling sem trabalho |
| `OUTBOX_BATCH_SIZE` | `50` | eventos por claim, de 1 a 500 |
| `OUTBOX_LEASE` | `30s` | posse de um lote reivindicado |
| `OUTBOX_BACKOFF_MAX` | `5m` | teto do backoff de um evento que falha; não há limite de tentativas |
| `RESOLVER_ENABLED` | `true` | liga o resolvedor de referências pendentes |
| `RESOLVER_POLL_INTERVAL` | `1s` | intervalo de polling sem trabalho |
| `RESOLVER_BATCH_SIZE` | `50` | transações por claim, de 1 a 500 |
| `RESOLVER_LEASE` | `30s` | posse de uma transação reivindicada |
| `REFERENCE_MAX_ATTEMPTS` | `10` | buscas pela referência antes de `REFERENCE_NOT_FOUND` |
| `REFERENCE_TTL` | `15m` | prazo da espera, fixado ao entrar em `PENDING_REFERENCE` e nunca estendido |
| `REFERENCE_BACKOFF_BASE` | `1s` | primeiro intervalo entre buscas; no máximo `REFERENCE_BACKOFF_MAX` (nota 5) |
| `REFERENCE_BACKOFF_MAX` | `60s` | teto do intervalo entre buscas; no mínimo `REFERENCE_BACKOFF_BASE` |
| `PERMANENT_FAILURE_LIMIT` | `3` | falhas permanentes de infraestrutura numa pendência antes de `FAILED` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | vazio | URL base `http`/`https` do coletor OTLP/HTTP, como `http://jaeger:4318`; os spans vão para `<URL>/v1/traces`. Vazio desliga o tracing (ver [Observabilidade](#observabilidade)) |
| `OTEL_SERVICE_NAME` | `wallet` | `service.name` dos spans |

1. **`SHUTDOWN_TIMEOUT`.** Os componentes param um depois do outro, então o prazo precisa cobrir o dreno
   (`SHUTDOWN_DRAIN_DELAY`, só com a API ligada), o worker mais lento entre os ligados (`CONSUMER_DRAIN_TIMEOUT` + 5s
   para devolver à fila o que o dreno cancelou, `OUTBOX_LEASE` + 2s para soltar os leases do relay, `RESOLVER_LEASE`),
   `HTTP_SHUTDOWN_TIMEOUT` (só com a API ligada), 5s para exportar os spans na fila (só com tracing ligado) e 5s de
   folga. Com os padrões, 2 + 32 + 10 + 5 = 49s, ou 54s com tracing. Um valor menor é recusado na partida. O
   `stop_grace_period` das réplicas no compose é 65s, acima do padrão.
2. **`HTTP_REQUEST_TIMEOUT`.** É o que transforma um banco que parou de responder, sem fechar as conexões, num `503` a
   tempo, em vez de uma conexão cortada pelo `HTTP_WRITE_TIMEOUT`. A configuração exige que o prazo, somado aos 5s que
   o rollback do trabalho cortado pode levar, caiba em `HTTP_WRITE_TIMEOUT` (10 + 5 = 15s nos padrões) e, com a API
   ligada, que seja maior que os 5s que o verificador de tokens pode levar para buscar o JWKS. Convém, sem que seja
   validado, que passe de `DB_LOCK_TIMEOUT` × `DB_TX_MAX_ATTEMPTS` mais as esperas curtas entre as tentativas (6s com
   os padrões), para que uma requisição presa a uma carteira travada esgote as tentativas antes do prazo.
3. **`DB_LOCK_TIMEOUT`.** Menor que `DB_STATEMENT_TIMEOUT`, para que a espera por uma carteira disputada termine como
   lock timeout (`55P03`), que o serviço repete até `DB_TX_MAX_ATTEMPTS`, e não como timeout de comando (`57014`), que
   é tratado como indisponibilidade, sem nova tentativa (`503` no HTTP). Um valor igual ou maior é recusado na partida.
4. **`CONSUMER_PROCESSING_TIMEOUT`.** Menor que `CONSUMER_VISIBILITY_TIMEOUT`, para que o SQS não entregue a outra
   réplica uma mensagem ainda em processamento, e, com o consumidor ligado, maior que os mesmos 5s da busca do JWKS,
   que o prazo da mensagem não pode cortar.
5. **`REFERENCE_BACKOFF_BASE`.** No máximo `REFERENCE_BACKOFF_MAX`: o resolvedor limita cada espera ao teto, então uma
   base acima dele seria trocada pelo teto sem aviso. Um valor maior é recusado na partida.

Fora da configuração do serviço:

| Variável | Quem lê | Uso |
|---|---|---|
| `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_SHARED_CREDENTIALS_FILE`, `AWS_PROFILE` | o SDK da AWS | endpoint do MiniStack e o perfil `wallet-app` do arquivo gerado pelo provisionamento; no compose, `http://ministack:4566`, `us-east-1`, `/run/aws/credentials` e `wallet-app` |
| `MIGRATIONS_DATABASE_URL` | `cmd/migrate` | DSN do dono do schema (`wallet_owner`) |
| `FAILPOINTS` | o binário compilado com `-tags failpoints` | pontos de crash dos testes e2e (ver [Testes](#testes)) |
| `OTEL_EXPORTER_OTLP_{HEADERS,TIMEOUT,COMPRESSION,CERTIFICATE,CLIENT_CERTIFICATE,CLIENT_KEY}` e `OTEL_EXPORTER_OTLP_PROTOCOL=http/json`, cada uma também na variante `OTEL_EXPORTER_OTLP_TRACES_*` | o exportador OTLP/HTTP, só com tracing ligado | cabeçalhos, prazo e compressão de cada envio; o TLS de um endpoint `https`; `http/json` manda os spans em JSON em vez de protobuf, pelo mesmo transporte HTTP |
| `OTEL_BSP_*`, `OTEL_SPAN_*_LIMIT`, `OTEL_ATTRIBUTE_*_LIMIT`, `OTEL_EVENT_ATTRIBUTE_COUNT_LIMIT`, `OTEL_LINK_ATTRIBUTE_COUNT_LIMIT` | o SDK do OpenTelemetry, só com tracing ligado | o lote de spans e os limites de atributos, eventos e links de cada span; os padrões servem ao compose |
| `OTEL_RESOURCE_ATTRIBUTES` | o SDK do OpenTelemetry, só com tracing ligado | atributos a mais no resource dos spans, como `deployment.environment.name=dev`; `service.name` e `service.instance.id` vêm sempre de `OTEL_SERVICE_NAME` e `APP_INSTANCE_ID` e prevalecem sobre os desta variável |

Outras variáveis padrão do OpenTelemetry não valem: `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (o endpoint vem só de
`OTEL_EXPORTER_OTLP_ENDPOINT`), `OTEL_EXPORTER_OTLP_INSECURE` e `OTEL_EXPORTER_OTLP_TRACES_INSECURE` (o esquema da URL
decide: `http` sem TLS, `https` com), `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` (o transporte é sempre OTLP/HTTP),
`OTEL_TRACES_EXPORTER` (o exportador é sempre o OTLP/HTTP), `OTEL_TRACES_SAMPLER` e `OTEL_TRACES_SAMPLER_ARG` (a
amostragem é sempre `ParentBased(AlwaysSample)`, sem proporção), `OTEL_PROPAGATORS` (a propagação é sempre W3C trace
context e baggage) e `OTEL_SDK_DISABLED` (só um `OTEL_EXPORTER_OTLP_ENDPOINT` vazio desliga o tracing).

O compose não repassa o `.env.example`, nem um `.env`, às réplicas: elas recebem só o que o `environment:` do
`docker-compose.yml` fixa e usam o padrão para o resto. Para mudar uma variável no compose, crie um
`docker-compose.override.yml`, que o compose aplica sozinho, com uma entrada por réplica:

```yaml
services:
  app-1:
    environment:
      LOG_LEVEL: debug
```

**Rodar o binário no host** (opcional), contra as dependências do compose, como uma quarta instância. O arquivo de
credenciais é copiado do volume, com as chaves que o MiniStack já aceita, e as variáveis do `.env.example` são
carregadas com os hosts trocados por `localhost`. A API vai para a porta 8090, porque a 8080 é do Keycloak:

```sh
mkdir -p tmp/aws
docker compose cp aws-provision:/shared/aws/credentials tmp/aws/credentials
set -a; . ./.env.example; set +a
DATABASE_URL='postgres://wallet_app:wallet_app@localhost:5432/wallet?sslmode=disable' \
OIDC_JWKS_URL=http://localhost:8080/realms/wagering/protocol/openid-connect/certs \
AWS_ENDPOINT_URL=http://localhost:4566 AWS_SHARED_CREDENTIALS_FILE=tmp/aws/credentials \
APP_INSTANCE_ID=host-1 HTTP_ADDR=:8090 go run ./cmd/wallet
```

`tmp/` está no `.gitignore`. A instância do host divide o banco e as filas com as réplicas do compose e para com
Ctrl+C, na mesma ordem de shutdown das outras.

## Filas e provisionamento

O serviço `aws-provision` roda `cmd/provision` contra o MiniStack, como root, a cada `up`. Ele só usa chamadas padrão
de SQS e IAM e é idempotente: filas e usuários são reaproveitados, atributos e políticas reescritos, e uma chave de
acesso só é criada quando o arquivo de credenciais não tem uma que o IAM ainda aceite.

| Recurso | Detalhe |
|---|---|
| `wager-transactions.fifo` | entrada; redrive para `wager-transactions-dlq.fifo` com `maxReceiveCount` 30 |
| `wallet-events.fifo` | eventos de saída; redrive para `wallet-events-dlq.fifo` com `maxReceiveCount` 30 |
| atributos das 4 filas | FIFO, `ContentBasedDeduplication=false`, `VisibilityTimeout` 30s, retenção de 14 dias |
| usuário `provider-a-sqs`, `provider-b-sqs` | `SendMessage` e `GetQueueUrl` na fila de entrada, e nada mais |
| usuário `wallet-app` | consome a entrada (`ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility`, também em lote, `GetQueueAttributes`, `GetQueueUrl`); envia para a DLQ de entrada e, também em lote, para `wallet-events.fifo` |
| usuário `events-reader` | consome `wallet-events.fifo` |
| credenciais | `/shared/aws/credentials` no volume `aws-shared`, perfis `[events-reader]`, `[provider-a]`, `[provider-b]` e `[wallet-app]`, permissão 0600, do mesmo usuário não root dos containers do serviço |

**Tentativas e DLQ.** Quem move uma mensagem para a DLQ é o consumidor, com o motivo no atributo `FailureReason`: na
hora, quando a reentrega daria o mesmo resultado, ou como `RETRIES_EXHAUSTED`, depois de `CONSUMER_MAX_RECEIVES` (10)
recebimentos com falha transitória. O `maxReceiveCount` 30 do redrive é só uma rede de segurança acima desse limite. O
que acontece com cada mensagem está em [REGRAS §10](docs/REGRAS_DE_NEGOCIO.md#10-mensageria).

Rodar o provisionamento de novo usa o mesmo arquivo do volume e mantém as chaves que o IAM ainda aceita. Ele imprime
as URLs das filas e o id de cada chave, que sozinho não é segredo:

```sh
docker compose run --rm aws-provision
```

```text
 Container wallet-r1-ministack-1  Running
queue   wager-transactions-dlq.fifo  http://ministack:4566/000000000000/wager-transactions-dlq.fifo
queue   wager-transactions.fifo      http://ministack:4566/000000000000/wager-transactions.fifo
queue   wallet-events-dlq.fifo       http://ministack:4566/000000000000/wallet-events-dlq.fifo
queue   wallet-events.fifo           http://ministack:4566/000000000000/wallet-events.fifo
profile events-reader                AKIACFC681401BF440E6
profile provider-a                   AKIA200C22BEF26E4624
profile provider-b                   AKIA191FBF32A4F24DE0
profile wallet-app                   AKIAE5162C23AAB24386
wrote /shared/aws/credentials
```

O IAM do MiniStack confere a assinatura das chaves de usuário e avalia as políticas. Um provedor, por exemplo, não lê
a fila em que escreve:

```sh
docker compose run --rm aws --profile provider-a sqs receive-message \
  --queue-url http://ministack:4566/000000000000/wager-transactions.fifo
```

```text
aws: [ERROR]: An error occurred (AccessDeniedException) when calling the ReceiveMessage operation: User: arn:aws:iam::000000000000:user/provider-a-sqs is not authorized to perform: sqs:ReceiveMessage because no identity-based policy allows the sqs:ReceiveMessage action
```

O envio de uma mensagem como provedor, a leitura dos eventos e a inspeção da DLQ estão em
[Pela fila](#pela-fila-sqs), porque usam a carteira dos exemplos.

## Migrations

As migrations ficam em `migrations/` (`NNNNNN_nome.up.sql` e `.down.sql`, embutidas no binário com `go:embed`) e são
aplicadas pelo comando `migrate`, sempre como `wallet_owner`, o dono do schema. O serviço conecta como `wallet_app`, que
não cria objetos, não desliga triggers e não apaga histórico. Os dois papéis são criados por
`deploy/postgres/initdb/00-roles.sql`, que o entrypoint do Postgres executa na primeira inicialização de um volume
vazio. O compose aplica as migrations a cada `up`, antes das réplicas, que se recusam a subir num schema diferente do
que o binário embute.

O comando aceita `up`, `down N`, `version` e `force V` (grava V como versão e limpa a marca de migração interrompida,
só para recuperar uma migração que falhou no meio). Cada um imprime a versão resultante; o código de saída é 0 em
sucesso, 1 em falha do banco e 2 em erro de uso.

```sh
docker compose run --rm migrate version
```

```text
 Container wallet-r1-postgres-1  Running
version 10
```

Reverter a última migration e aplicar de novo. Os alvos do `make` passam `--build`, porque as migrations vão dentro
da imagem; o log do build foi omitido abaixo:

```sh
make migrate-down N=1
```

```text
docker compose run --rm --build migrate down 1
 Container wallet-r1-postgres-1  Running
…
version 9
```

```sh
make migrate-up
```

```text
docker compose run --rm --build migrate up
 Container wallet-r1-postgres-1  Running
…
version 10
```

O serviço só confere a versão na partida, e as réplicas no ar continuam contando com tudo o que as migrations criaram:
reverter a 000009 com elas no ar faz falhar toda escrita que grava evento, porque o `INSERT` da outbox nomeia a coluna
`traceparent`, até a migration ser aplicada de novo; reverter mais que isso tira do banco tabelas e guardas de que elas
dependem. Fora do Docker, com Go instalado:

```sh
MIGRATIONS_DATABASE_URL='postgres://wallet_owner:wallet_owner@localhost:5432/wallet?sslmode=disable' \
go run ./cmd/migrate version
```

```text
version 10
```

## Autenticação

O realm `wagering` é importado automaticamente na subida do Keycloak. Todos os clients são confidenciais e só têm o
fluxo `client_credentials`. Os segredos abaixo são **de desenvolvimento**: estão versionados no realm e, para os alvos
`token-*`, no `Makefile`; o gerador de carga os repete em `test/load/auth.go`. O `.env.example` não tem nenhum.

| Client | Segredo (dev) | Escopos | Claims |
|---|---|---|---|
| `provider-a` | `provider-a-secret` | `wagering:write`, `wagering:read` | `provider_id=provider-a` |
| `provider-b` | `provider-b-secret` | `wagering:write`, `wagering:read` | `provider_id=provider-b` |
| `wallet-service` | `wallet-service-secret` | `wallets:manage` | nenhum `provider_id`: é o serviço interno |
| `provider-a-short` | `provider-a-short-secret` | iguais aos do `provider-a` | `provider_id=provider-a`, com token de 2 segundos, para os testes de expiração |

Todos os tokens têm audiência `wagering-api` e valem 5 minutos (os do `provider-a-short`, 2 segundos). Os alvos do
`make` imprimem só o access token:

```sh
TOKEN_A=$(make -s token-provider-a)
TOKEN_B=$(make -s token-provider-b)
TOKEN_INT=$(make -s token-internal)
```

O mesmo pedido com curl:

```sh
curl -s -u provider-a:provider-a-secret -d grant_type=client_credentials \
  http://localhost:8080/realms/wagering/protocol/openid-connect/token
```

```json
{"access_token":"eyJhbGciOiJSUzI1NiIsInR5…","expires_in":300,"refresh_expires_in":0,"token_type":"Bearer","not-before-policy":0,"scope":"wagering:read wagering:write"}
```

O console de administração do Keycloak fica em `http://localhost:8080`, com usuário e senha de desenvolvimento
`admin`/`admin`.

**Quem pode o quê.** Um provedor envia transações com `wagering:write` e lê as suas com `wagering:read`; só o serviço
interno, com `wallets:manage`, administra carteiras (abre, lê, pagina o ledger e reconcilia) e lê qualquer transação. O
`providerId` autorizado vem sempre do token: um provedor que manda outro no corpo ou no path recebe
`403 PROVIDER_MISMATCH`. O health é público. A matriz por rota está em
[REGRAS §9](docs/REGRAS_DE_NEGOCIO.md#9-autorização), e a justificativa, em
[ARCHITECTURE.md](ARCHITECTURE.md#13-autenticação-e-autorização).

## Exemplos de chamadas

Os exemplos usam o nginx (`localhost:8000`) e continuam do bloco dos tokens, no mesmo shell. Rodados num volume novo,
na ordem, dão as mesmas respostas, com outros ids e horários. Os tokens valem 5 minutos: se uma chamada responder
`401`, rode de novo o bloco dos tokens. O header opcional `X-Correlation-Id` é devolvido na resposta e propagado para
logs e eventos; sem ele, ou com um valor fora de `^[A-Za-z0-9._:-]{1,128}$`, o serviço gera um.

### Carteira

Abrir a carteira do jogador do enunciado, com 1000.00 BRL (`201`), guardando o id:

```sh
WALLET_ID=$(curl -s -X POST localhost:8000/wallets \
  -H "Authorization: Bearer $TOKEN_INT" -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}' \
  | tee /dev/stderr | jq -r .id)
```

```json
{"id":"01a1471b-3a58-70a0-8ce2-ee46aeba9ac3","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","balance":{"amount":"1000.00","currency":"BRL"},"version":1}
```

A abertura com saldo positivo grava, no mesmo commit, a carteira na versão 1, a transação `OPENING` já `PROCESSED`, o
lançamento de crédito e os eventos `WagerTransactionProcessed` e `WalletBalanceChanged`. Uma segunda carteira para o
mesmo jogador e a mesma moeda é recusada. `POST /wallets` não tem `Idempotency-Key`: a unicidade de (jogador, moeda)
é o que impede uma carteira duplicada. O `409` traz o id da carteira que já existe no membro extra `walletId` do
problem+json, então quem perdeu a resposta `201` o recupera na repetição (a rota é interna, `wallets:manage`, e nada
disso chega aos provedores).

```sh
curl -s -X POST localhost:8000/wallets -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_INT" -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"50.00","currency":"BRL"}}'
```

```text
{"type":"about:blank","title":"Wallet already exists","status":409,"code":"WALLET_ALREADY_EXISTS","detail":"the player already has a wallet in this currency","correlationId":"01a1471b-3a66-7649-86b4-635695d77e8b","walletId":"01a1471b-3a58-70a0-8ce2-ee46aeba9ac3"}
HTTP 409
```

```sh
curl -s localhost:8000/wallets/$WALLET_ID -H "Authorization: Bearer $TOKEN_INT" | jq
```

```json
{
  "id": "01a1471b-3a58-70a0-8ce2-ee46aeba9ac3",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "balance": {
    "amount": "1000.00",
    "currency": "BRL"
  },
  "version": 1,
  "createdAt": "2026-10-16T23:45:29.176Z",
  "updatedAt": "2026-10-16T23:45:29.176Z"
}
```

### Transações

A aposta do enunciado (`201`; o `-w` imprime o status na última linha):

```sh
curl -s -X POST localhost:8000/wagering/transactions -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_A" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123",
       "playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"'"$WALLET_ID"'",
       "roundId":"round-987","gameId":"fortune-chimp","kind":"BET",
       "money":{"amount":"25.00","currency":"BRL"}}'
```

```text
{"transactionId":"01a1471b-3a77-718a-90b2-0e7b65f06992","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
HTTP 201
```

Para encurtar o resto, uma função que envia uma operação de `provider-a` na mesma carteira e rodada:
`tx <Idempotency-Key> <externalTransactionId> <kind> <valor> [referenceExternalTransactionId]`.

```sh
tx() {
  jq -nc --arg ext "$2" --arg wallet "$WALLET_ID" --arg kind "$3" --arg amount "$4" --arg ref "${5:-}" '{
      providerId: "provider-a", externalTransactionId: $ext,
      playerId: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", walletId: $wallet,
      roundId: "round-987", gameId: "fortune-chimp", kind: $kind,
      money: {amount: $amount, currency: "BRL"}}
    + (if $ref == "" then {} else {referenceExternalTransactionId: $ref} end)' |
  curl -s -X POST localhost:8000/wagering/transactions -w 'HTTP %{http_code}\n' \
    -H "Authorization: Bearer $TOKEN_A" -H 'Content-Type: application/json' -H "Idempotency-Key: $1" -d @-
}
```

Replay: a mesma chave com o mesmo conteúdo devolve `200`, o resultado gravado e `idempotentReplay: true`, com o saldo
do processamento original:

```sh
tx provider-a:transaction-123 transaction-123 BET 25.00
```

```text
{"transactionId":"01a1471b-3a77-718a-90b2-0e7b65f06992","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":true}
HTTP 200
```

A mesma chave com outro conteúdo, e o mesmo `externalTransactionId` com outra chave, são conflitos (`409`), e nada é
gravado:

```sh
tx provider-a:transaction-123 transaction-123 BET 30.00
```

```text
{"type":"about:blank","title":"Idempotency key reused","status":409,"code":"IDEMPOTENCY_KEY_REUSED","detail":"key already used with a different payload","correlationId":"01a1471b-3a87-759e-9862-0228d8fd00e7"}
HTTP 409
```

```sh
tx provider-a:other-key transaction-123 BET 25.00
```

```text
{"type":"about:blank","title":"Duplicate external transaction","status":409,"code":"DUPLICATE_EXTERNAL_TRANSACTION","detail":"externalTransactionId already submitted under another idempotency key","correlationId":"01a1471b-3a8e-740c-a7c3-f55ee95648a6"}
HTTP 409
```

Uma rejeição de negócio é definitiva: a transação fica gravada como `REJECTED`, com `failureCode`, e a resposta é
`422` com o corpo da transação (também no replay):

```sh
tx provider-a:transaction-124 transaction-124 BET 5000.00
```

```text
{"transactionId":"01a1471b-3a95-7cc1-b4cb-71a3b01fd705","status":"REJECTED","failureCode":"INSUFFICIENT_FUNDS","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
HTTP 422
```

Um `WIN` com referência à aposta, o `REFUND` da aposta e o `ROLLBACK` do `WIN`, que debita:

```sh
tx provider-a:transaction-125 transaction-125 WIN 50.00 transaction-123
```

```text
{"transactionId":"01a1471b-3aa5-7dd1-a69c-a01c01c70fff","status":"PROCESSED","balance":{"amount":"1025.00","currency":"BRL"},"idempotentReplay":false}
HTTP 201
```

```sh
tx provider-a:transaction-126 transaction-126 REFUND 25.00 transaction-123
```

```text
{"transactionId":"01a1471b-3ab1-71a9-8987-4a33cca338ec","status":"PROCESSED","balance":{"amount":"1050.00","currency":"BRL"},"idempotentReplay":false}
HTTP 201
```

```sh
tx provider-a:transaction-127 transaction-127 ROLLBACK 50.00 transaction-125
```

```text
{"transactionId":"01a1471b-3acb-7442-a3af-1a6f1349bcf5","status":"PROCESSED","balance":{"amount":"1000.00","currency":"BRL"},"idempotentReplay":false}
HTTP 201
```

Uma referência aceita no máximo uma reversão bem-sucedida, seja `REFUND` ou `ROLLBACK`. A aposta já foi devolvida
pelo `REFUND`, então um `ROLLBACK` dela é rejeitado:

```sh
tx provider-a:transaction-128 transaction-128 ROLLBACK 25.00 transaction-123
```

```text
{"transactionId":"01a1471b-3ad4-76fa-9aeb-e802c46b1d19","status":"REJECTED","failureCode":"REFERENCE_ALREADY_REVERSED","balance":{"amount":"1000.00","currency":"BRL"},"idempotentReplay":false}
HTTP 422
```

**Referência que ainda não chegou.** Um `REFUND` de uma aposta que o serviço ainda não recebeu fica em
`PENDING_REFERENCE`, e a resposta é `202` com `Location`:

```sh
curl -s -i -X POST localhost:8000/wagering/transactions \
  -H "Authorization: Bearer $TOKEN_A" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-201' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-201",
       "playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"'"$WALLET_ID"'",
       "roundId":"round-987","gameId":"fortune-chimp","kind":"REFUND",
       "money":{"amount":"10.00","currency":"BRL"},"referenceExternalTransactionId":"transaction-200"}'
```

```text
HTTP/1.1 202 Accepted
Server: nginx
Date: Fri, 16 Oct 2026 23:45:29 GMT
Content-Type: application/json
Content-Length: 111
Connection: keep-alive
Cache-Control: no-store
Location: /wagering/transactions/01a1471b-3adb-74d3-806c-46547cdca448
X-Correlation-Id: 01a1471b-3ada-780f-8336-53bd45a227c3

{"transactionId":"01a1471b-3adb-74d3-806c-46547cdca448","status":"PENDING_REFERENCE","idempotentReplay":false}
```

```sh
curl -s localhost:8000/providers/provider-a/wagering/transactions/transaction-201 \
  -H "Authorization: Bearer $TOKEN_A" | jq
```

```json
{
  "transactionId": "01a1471b-3adb-74d3-806c-46547cdca448",
  "status": "PENDING_REFERENCE",
  "kind": "REFUND",
  "providerId": "provider-a",
  "externalTransactionId": "transaction-201",
  "walletId": "01a1471b-3a58-70a0-8ce2-ee46aeba9ac3",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "roundId": "round-987",
  "gameId": "fortune-chimp",
  "money": {
    "amount": "10.00",
    "currency": "BRL"
  },
  "referenceExternalTransactionId": "transaction-200",
  "attempts": 0,
  "nextAttemptAt": "2026-10-16T23:45:30.129Z",
  "expiresAt": "2026-10-17T00:00:29.307Z",
  "createdAt": "2026-10-16T23:45:29.307Z",
  "updatedAt": "2026-10-16T23:45:29.307Z"
}
```

A aposta chega:

```sh
tx provider-a:transaction-200 transaction-200 BET 10.00
```

```text
{"transactionId":"01a1471b-3aec-7193-acbb-e7cd4b257830","status":"PROCESSED","balance":{"amount":"990.00","currency":"BRL"},"idempotentReplay":false}
HTTP 201
```

O resolvedor de qualquer réplica reavalia a pendência no `nextAttemptAt`, com backoff exponencial e jitter (até 1s,
2s, 4s… e no máximo 60s entre as buscas), e a resolve. Alguns segundos depois:

```sh
curl -s localhost:8000/providers/provider-a/wagering/transactions/transaction-201 \
  -H "Authorization: Bearer $TOKEN_A" | jq -c '{status, balance, attempts, processedAt}'
```

```json
{"status":"PROCESSED","balance":{"amount":"1000.00","currency":"BRL"},"attempts":0,"processedAt":"2026-10-16T23:45:30.584Z"}
```

O `balance` é o saldo depois do crédito do `REFUND`. `attempts` conta as reavaliações que não acharam a referência; aqui
a primeira já a encontrou. Uma referência que não chega em `REFERENCE_MAX_ATTEMPTS` buscas ou em `REFERENCE_TTL` termina
`REJECTED` com `REFERENCE_NOT_FOUND` (as regras da espera estão em
[REGRAS §6](docs/REGRAS_DE_NEGOCIO.md#6-referências-pendentes)).

### Consultas

Por id interno, como o provedor dono:

```sh
TX_ID=$(curl -s localhost:8000/providers/provider-a/wagering/transactions/transaction-123 \
  -H "Authorization: Bearer $TOKEN_A" | jq -r .transactionId)
curl -s localhost:8000/wagering/transactions/$TX_ID -H "Authorization: Bearer $TOKEN_A" | jq
```

```json
{
  "transactionId": "01a1471b-3a77-718a-90b2-0e7b65f06992",
  "status": "PROCESSED",
  "kind": "BET",
  "providerId": "provider-a",
  "externalTransactionId": "transaction-123",
  "walletId": "01a1471b-3a58-70a0-8ce2-ee46aeba9ac3",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "roundId": "round-987",
  "gameId": "fortune-chimp",
  "money": {
    "amount": "25.00",
    "currency": "BRL"
  },
  "balance": {
    "amount": "975.00",
    "currency": "BRL"
  },
  "walletVersion": 2,
  "attempts": 0,
  "createdAt": "2026-10-16T23:45:29.207Z",
  "updatedAt": "2026-10-16T23:45:29.207Z",
  "processedAt": "2026-10-16T23:45:29.207Z"
}
```

O serviço interno lê qualquer transação, pelos dois caminhos:

```sh
curl -s localhost:8000/wagering/transactions/$TX_ID \
  -H "Authorization: Bearer $TOKEN_INT" | jq -c '{transactionId, status, providerId}'
curl -s localhost:8000/providers/provider-a/wagering/transactions/transaction-123 \
  -H "Authorization: Bearer $TOKEN_INT" | jq -c '{transactionId, status, providerId}'
```

```json
{"transactionId":"01a1471b-3a77-718a-90b2-0e7b65f06992","status":"PROCESSED","providerId":"provider-a"}
{"transactionId":"01a1471b-3a77-718a-90b2-0e7b65f06992","status":"PROCESSED","providerId":"provider-a"}
```

Outro provedor não vê a transação: pelo id interno, `404`, que não revela que ela existe; pelo path com outro
`providerId`, `403`:

```sh
curl -s localhost:8000/wagering/transactions/$TX_ID -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_B"
curl -s localhost:8000/providers/provider-a/wagering/transactions/transaction-123 -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_B"
```

```text
{"type":"about:blank","title":"Transaction not found","status":404,"code":"TRANSACTION_NOT_FOUND","correlationId":"01a1471b-46ec-76ed-b0d1-94646b3ea3bc"}
HTTP 404
{"type":"about:blank","title":"Provider mismatch","status":403,"code":"PROVIDER_MISMATCH","detail":"providerId does not match the access token","correlationId":"01a1471b-46f3-77bc-8da9-eb99f45de594"}
HTTP 403
```

### Ledger e reconciliação

O ledger pagina por cursor opaco, em ordem estável de inserção. `limit` vai de 1 a 200 (padrão 50), e o `nextCursor`
de uma página é o `cursor` da seguinte:

```sh
PAGE=$(curl -s "localhost:8000/wallets/$WALLET_ID/ledger?limit=3" -H "Authorization: Bearer $TOKEN_INT")
echo "$PAGE" | jq
```

```json
{
  "items": [
    {
      "id": "01a1471b-3a58-70a2-a967-0d05f5fae542",
      "transactionId": "01a1471b-3a58-70a1-882e-454f99412c83",
      "direction": "CREDIT",
      "amount": {
        "amount": "1000.00",
        "currency": "BRL"
      },
      "balanceBefore": {
        "amount": "0.00",
        "currency": "BRL"
      },
      "balanceAfter": {
        "amount": "1000.00",
        "currency": "BRL"
      },
      "createdAt": "2026-10-16T23:45:29.176Z"
    },
    {
      "id": "01a1471b-3a77-71ab-8c7b-5b2042c4beeb",
      "transactionId": "01a1471b-3a77-718a-90b2-0e7b65f06992",
      "direction": "DEBIT",
      "amount": {
        "amount": "25.00",
        "currency": "BRL"
      },
      "balanceBefore": {
        "amount": "1000.00",
        "currency": "BRL"
      },
      "balanceAfter": {
        "amount": "975.00",
        "currency": "BRL"
      },
      "createdAt": "2026-10-16T23:45:29.207Z"
    },
    {
      "id": "01a1471b-3aa5-7e0c-9f93-3f0a84b820fb",
      "transactionId": "01a1471b-3aa5-7dd1-a69c-a01c01c70fff",
      "direction": "CREDIT",
      "amount": {
        "amount": "50.00",
        "currency": "BRL"
      },
      "balanceBefore": {
        "amount": "975.00",
        "currency": "BRL"
      },
      "balanceAfter": {
        "amount": "1025.00",
        "currency": "BRL"
      },
      "createdAt": "2026-10-16T23:45:29.253Z"
    }
  ],
  "nextCursor": "eyJ2IjoxLCJzZXEiOjN9"
}
```

```sh
curl -s "localhost:8000/wallets/$WALLET_ID/ledger?limit=3&cursor=$(echo "$PAGE" | jq -r .nextCursor)" \
  -H "Authorization: Bearer $TOKEN_INT" | jq
```

```json
{
  "items": [
    {
      "id": "01a1471b-3ab1-71c2-aa01-c598c352b729",
      "transactionId": "01a1471b-3ab1-71a9-8987-4a33cca338ec",
      "direction": "CREDIT",
      "amount": {
        "amount": "25.00",
        "currency": "BRL"
      },
      "balanceBefore": {
        "amount": "1025.00",
        "currency": "BRL"
      },
      "balanceAfter": {
        "amount": "1050.00",
        "currency": "BRL"
      },
      "createdAt": "2026-10-16T23:45:29.265Z"
    },
    {
      "id": "01a1471b-3acb-7456-9a24-100d37c21c26",
      "transactionId": "01a1471b-3acb-7442-a3af-1a6f1349bcf5",
      "direction": "DEBIT",
      "amount": {
        "amount": "50.00",
        "currency": "BRL"
      },
      "balanceBefore": {
        "amount": "1050.00",
        "currency": "BRL"
      },
      "balanceAfter": {
        "amount": "1000.00",
        "currency": "BRL"
      },
      "createdAt": "2026-10-16T23:45:29.291Z"
    },
    {
      "id": "01a1471b-3aec-71ac-95ab-e13192b2af06",
      "transactionId": "01a1471b-3aec-7193-acbb-e7cd4b257830",
      "direction": "DEBIT",
      "amount": {
        "amount": "10.00",
        "currency": "BRL"
      },
      "balanceBefore": {
        "amount": "1000.00",
        "currency": "BRL"
      },
      "balanceAfter": {
        "amount": "990.00",
        "currency": "BRL"
      },
      "createdAt": "2026-10-16T23:45:29.324Z"
    }
  ],
  "nextCursor": "eyJ2IjoxLCJzZXEiOjZ9"
}
```

A última página vem com `"nextCursor": null`. A reconciliação reconstrói o saldo pelo ledger, abertura incluída, numa
leitura consistente, e não altera nada:

```sh
curl -s -X POST localhost:8000/wallets/$WALLET_ID/reconciliation -H "Authorization: Bearer $TOKEN_INT" | jq
```

```json
{
  "walletId": "01a1471b-3a58-70a0-8ce2-ee46aeba9ac3",
  "storedBalance": {
    "amount": "1000.00",
    "currency": "BRL"
  },
  "calculatedBalance": {
    "amount": "1000.00",
    "currency": "BRL"
  },
  "difference": {
    "amount": "0.00",
    "currency": "BRL"
  },
  "consistent": true,
  "checkedEntries": 7
}
```

### Pela fila (SQS)

Uma aposta enviada à fila de entrada como `provider-a`, com as credenciais IAM dele. O corpo segue o enunciado; o
access token do Keycloak do próprio provedor vai no atributo `ProviderToken`, porque é ele que identifica o produtor;
`correlationId` é opcional. `MessageGroupId` é o `walletId` (ordem por carteira, paralelismo entre carteiras) e
`MessageDeduplicationId` é o `messageId`, que precisa ser único entre todos os provedores, por isso um UUID:

```sh
MSG_ID=$(uuidgen | tr 'A-Z' 'a-z')
BODY=$(jq -nc --arg id "$MSG_ID" --arg at "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)" --arg wallet "$WALLET_ID" '{
    messageId: $id, type: "WagerTransactionRequested", occurredAt: $at,
    data: {providerId: "provider-a", externalTransactionId: "transaction-300",
           idempotencyKey: "provider-a:transaction-300",
           playerId: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", walletId: $wallet,
           roundId: "round-987", gameId: "fortune-chimp", kind: "BET",
           money: {amount: "10.00", currency: "BRL"}}}')
ATTRS=$(jq -nc --arg token "$TOKEN_A" '{
    ProviderToken: {DataType: "String", StringValue: $token},
    correlationId: {DataType: "String", StringValue: "readme-sqs-1"}}')
docker compose run --rm aws --profile provider-a sqs send-message \
  --queue-url http://ministack:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$WALLET_ID" --message-deduplication-id "$MSG_ID" \
  --message-body "$BODY" --message-attributes "$ATTRS"
```

```json
{
    "MD5OfMessageBody": "1c60fb195d9292cf25a27327ebddf3af",
    "MD5OfMessageAttributes": "1d2bd5d4c53be5ce96765f52caadd280",
    "MessageId": "3ce546b6-da57-49e2-bb36-14a9cb42ca7d",
    "SequenceNumber": "00000000000000000001"
}
```

Um consumidor de alguma réplica processa a mensagem pelo mesmo caso de uso do HTTP, com a inbox na mesma transação,
e só então a apaga. Um ou dois segundos depois:

```sh
curl -s localhost:8000/providers/provider-a/wagering/transactions/transaction-300 \
  -H "Authorization: Bearer $TOKEN_A" | jq -c '{status, kind, balance}'
```

```json
{"status":"PROCESSED","kind":"BET","balance":{"amount":"990.00","currency":"BRL"}}
```

**Eventos de saída.** O relay publica a outbox em `wallet-events.fifo`, com `MessageGroupId` = `walletId`,
`MessageDeduplicationId` = `eventId` e os atributos `eventType`, `eventVersion` e `correlationId`. Os dois primeiros
eventos da carteira, lidos como `events-reader`, são os da abertura. O corpo sai como o PostgreSQL devolve o JSONB da
outbox, com as chaves em outra ordem:

```sh
docker compose run --rm aws --profile events-reader sqs receive-message \
  --queue-url http://ministack:4566/000000000000/wallet-events.fifo \
  --max-number-of-messages 2 --message-attribute-names All --message-system-attribute-names MessageGroupId
```

```json
{
    "Messages": [
        {
            "MessageId": "d4b26d6e-5ce9-4ab6-8ec9-dd607ae6516a",
            "ReceiptHandle": "44624ae3-5883-444b-8c72-2b50e5a3259d",
            "MD5OfBody": "94a1428b5c40d8ff5ee69500606f7000",
            "Body": "{\"data\": {\"kind\": \"OPENING\", \"money\": {\"amount\": \"1000.00\", \"currency\": \"BRL\"}, \"playerId\": \"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1\", \"walletId\": \"01a1471b-3a58-70a0-8ce2-ee46aeba9ac3\", \"balanceAfter\": {\"amount\": \"1000.00\", \"currency\": \"BRL\"}, \"transactionId\": \"01a1471b-3a58-70a1-882e-454f99412c83\", \"walletVersion\": 1}, \"eventId\": \"01a1471b-3a5c-7bff-9cc2-4b28eef52aed\", \"version\": 1, \"eventType\": \"WagerTransactionProcessed\", \"occurredAt\": \"2026-10-16T23:45:29.176Z\", \"aggregateId\": \"01a1471b-3a58-70a0-8ce2-ee46aeba9ac3\", \"correlationId\": \"01a1471b-3a57-7691-bb24-eeffd78db3db\"}",
            "Attributes": {
                "MessageGroupId": "01a1471b-3a58-70a0-8ce2-ee46aeba9ac3"
            },
            "MD5OfMessageAttributes": "fa227d97b347b27915e5d9df7ae1fbad",
            "MessageAttributes": {
                "eventType": {
                    "StringValue": "WagerTransactionProcessed",
                    "DataType": "String"
                },
                "eventVersion": {
                    "StringValue": "1",
                    "DataType": "Number"
                },
                "correlationId": {
                    "StringValue": "01a1471b-3a57-7691-bb24-eeffd78db3db",
                    "DataType": "String"
                }
            }
        },
        {
            "MessageId": "cc8901d0-2fdd-44af-8f8a-5c1176d619be",
            "ReceiptHandle": "fdc5b0a0-b168-46e8-aab3-5032ab40b4b3",
            "MD5OfBody": "11b70e83c68df090c5b31fc741ec5dc9",
            "Body": "{\"data\": {\"money\": {\"amount\": \"1000.00\", \"currency\": \"BRL\"}, \"walletId\": \"01a1471b-3a58-70a0-8ce2-ee46aeba9ac3\", \"direction\": \"CREDIT\", \"balanceAfter\": {\"amount\": \"1000.00\", \"currency\": \"BRL\"}, \"balanceBefore\": {\"amount\": \"0.00\", \"currency\": \"BRL\"}, \"transactionId\": \"01a1471b-3a58-70a1-882e-454f99412c83\", \"walletVersion\": 1}, \"eventId\": \"01a1471b-3a5c-7c0a-a344-fce28cb3ad77\", \"version\": 1, \"eventType\": \"WalletBalanceChanged\", \"occurredAt\": \"2026-10-16T23:45:29.176Z\", \"aggregateId\": \"01a1471b-3a58-70a0-8ce2-ee46aeba9ac3\", \"correlationId\": \"01a1471b-3a57-7691-bb24-eeffd78db3db\"}",
            "Attributes": {
                "MessageGroupId": "01a1471b-3a58-70a0-8ce2-ee46aeba9ac3"
            },
            "MD5OfMessageAttributes": "e7372f2f2441ba3fa91ba4d9dd0a215f",
            "MessageAttributes": {
                "eventType": {
                    "StringValue": "WalletBalanceChanged",
                    "DataType": "String"
                },
                "eventVersion": {
                    "StringValue": "1",
                    "DataType": "Number"
                },
                "correlationId": {
                    "StringValue": "01a1471b-3a57-7691-bb24-eeffd78db3db",
                    "DataType": "String"
                }
            }
        }
    ]
}
```

**DLQ.** A mesma mensagem sem o `ProviderToken`, com outro id de deduplicação, não tem quem a assine e vai para a DLQ
na hora:

```sh
docker compose run --rm aws --profile provider-a sqs send-message \
  --queue-url http://ministack:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$WALLET_ID" --message-deduplication-id "no-token-$MSG_ID" \
  --message-body "$BODY"
```

```json
{
    "MD5OfMessageBody": "1c60fb195d9292cf25a27327ebddf3af",
    "MessageId": "36512435-20db-44d0-ab54-b4d815ed56b4",
    "SequenceNumber": "00000000000000000002"
}
```

Só o root do MiniStack lê a DLQ (nenhum usuário IAM tem permissão de leitura nela). A cópia leva os atributos
`FailureReason` e `SourceMessageId`, o `FailureCode` quando o motivo tem um código (`UNAUTHENTICATED`, `FORBIDDEN` e
`INVALID_MESSAGE`, como aqui), mais o `correlationId` do produtor quando houver, e nunca o `ProviderToken`:

```sh
docker compose run --rm -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test aws sqs receive-message \
  --queue-url http://ministack:4566/000000000000/wager-transactions-dlq.fifo \
  --message-attribute-names All --message-system-attribute-names All --max-number-of-messages 10
```

```json
{
    "Messages": [
        {
            "MessageId": "106d809e-5f90-48bd-9bd7-22ddc4a91a04",
            "ReceiptHandle": "bdfd9b64-d2e3-46df-be75-9291c0447203",
            "MD5OfBody": "1c60fb195d9292cf25a27327ebddf3af",
            "Body": "{\"messageId\":\"8dc9244a-a89f-4db2-bc8f-40cdd96ed764\",\"type\":\"WagerTransactionRequested\",\"occurredAt\":\"2026-10-16T23:45:32.000Z\",\"data\":{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-300\",\"idempotencyKey\":\"provider-a:transaction-300\",\"playerId\":\"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1\",\"walletId\":\"01a1471b-3a58-70a0-8ce2-ee46aeba9ac3\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",\"money\":{\"amount\":\"10.00\",\"currency\":\"BRL\"}}}",
            "Attributes": {
                "SenderId": "000000000000",
                "SentTimestamp": "1792194337133",
                "ApproximateReceiveCount": "1",
                "ApproximateFirstReceiveTimestamp": "1792194340714",
                "SequenceNumber": "00000000000000000001",
                "MessageDeduplicationId": "36512435-20db-44d0-ab54-b4d815ed56b4",
                "MessageGroupId": "01a1471b-3a58-70a0-8ce2-ee46aeba9ac3"
            },
            "MD5OfMessageAttributes": "c028ee0e7e1d431bf97ef6a1e270b6c9",
            "MessageAttributes": {
                "SourceMessageId": {
                    "StringValue": "36512435-20db-44d0-ab54-b4d815ed56b4",
                    "DataType": "String"
                },
                "FailureReason": {
                    "StringValue": "UNAUTHENTICATED",
                    "DataType": "String"
                },
                "FailureCode": {
                    "StringValue": "MISSING_PROVIDER_TOKEN",
                    "DataType": "String"
                }
            }
        }
    ]
}
```

O redrive da DLQ é manual e passa pelo provedor: sem o token, uma cópia devolvida à entrada seria recusada de novo, e
quem reprocessa é o provedor, reenviando a mensagem com um token novo (ver
[ARCHITECTURE.md](ARCHITECTURE.md#18-limitações-interpretações-e-trabalho-não-concluído)).

## Contrato de erros

Erros saem em `application/problem+json` (RFC 9457), com um `code` estável, além de `type`, `title`, `status`,
`detail` (quando há), `correlationId` e, quando o erro é de campo, `errors[]` com `field` e `code`. O cliente decide
pelo `code`, nunca pelo texto. A exceção é o resultado definitivo de uma transação (`422` para `REJECTED`, `500` para
`FAILED`): ele vem no corpo normal da transação, com `status` e `failureCode`, como nos exemplos acima.

O status diz a classe do resultado:

| HTTP | O que significa |
|---|---|
| `200`, `201`, `202` | sucesso; no envio de transação, `201` é a processada, `200` o replay de uma processada e `202` a pendente, com `Location` |
| `400`, `413`, `415` | entrada inválida, nada gravado; o `code` e os `errors[]` dizem o que corrigir, e a chave fica livre para o reenvio corrigido |
| `401`, `403` | token ausente, inválido ou expirado (com `WWW-Authenticate: Bearer`); token sem permissão para a rota ou para o `providerId` |
| `404`, `405` | carteira, transação ou rota inexistente; método que a rota não aceita (com `Allow`) |
| `409` | conflito de idempotência, ou carteira que já existe (com o `walletId` dela no corpo) |
| `422` | rejeição de negócio, gravada como `REJECTED`, com o `failureCode` no corpo da transação |
| `500` | erro inesperado (`INTERNAL_ERROR`, nada gravado) ou, no replay, uma transação `FAILED` |
| `503` | indisponibilidade temporária, com `Retry-After: 2`; repetir é seguro |

Cada `code`, cada `failureCode`, os códigos por campo de `errors[]` e o que cada um vira quando a operação chega pela
fila estão em [REGRAS §8](docs/REGRAS_DE_NEGOCIO.md#8-rejeições-e-códigos-de-falha).

Exemplos reais, na sequência da sessão acima. Sem token, com o desafio no header:

```sh
curl -s -i localhost:8000/wallets/$WALLET_ID
```

```text
HTTP/1.1 401 Unauthorized
Server: nginx
Date: Fri, 16 Oct 2026 23:45:40 GMT
Content-Type: application/problem+json
Content-Length: 193
Connection: keep-alive
Cache-Control: no-store
Www-Authenticate: Bearer realm="wagering"
X-Correlation-Id: 01a1471b-6802-71fc-9d78-75e2433b9c35

{"type":"about:blank","title":"Unauthenticated","status":401,"code":"UNAUTHENTICATED","detail":"a valid bearer access token is required","correlationId":"01a1471b-6802-71fc-9d78-75e2433b9c35"}
```

Provedor numa rota de carteira, e o serviço interno enviando uma transação:

```sh
curl -s localhost:8000/wallets/$WALLET_ID -w 'HTTP %{http_code}\n' -H "Authorization: Bearer $TOKEN_A"
curl -s -X POST localhost:8000/wagering/transactions -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_INT" -H 'Content-Type: application/json' -H 'Idempotency-Key: k-1' -d '{}'
```

```text
{"type":"about:blank","title":"Forbidden","status":403,"code":"FORBIDDEN","detail":"the access token does not allow this operation","correlationId":"01a1471b-680b-78e9-b79a-8d03a1f74797"}
HTTP 403
{"type":"about:blank","title":"Forbidden","status":403,"code":"FORBIDDEN","detail":"the access token does not allow this operation","correlationId":"01a1471b-6812-772e-81ca-77591800a0a7"}
HTTP 403
```

`provider-b` tentando operar em nome de `provider-a`:

```sh
curl -s -X POST localhost:8000/wagering/transactions -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_B" -H 'Content-Type: application/json' -H 'Idempotency-Key: provider-a:transaction-900' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-900",
       "playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"'"$WALLET_ID"'",
       "roundId":"round-987","gameId":"fortune-chimp","kind":"BET",
       "money":{"amount":"1.00","currency":"BRL"}}'
```

```text
{"type":"about:blank","title":"Provider mismatch","status":403,"code":"PROVIDER_MISMATCH","detail":"providerId does not match the access token","correlationId":"01a1471b-6819-7c43-bf6a-b33313ef3831"}
HTTP 403
```

Vários campos errados de uma vez: UUID em maiúsculas, `roundId` ausente, `amount` vazio e moeda em minúsculas:

```sh
curl -s -X POST localhost:8000/wagering/transactions -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_A" -H 'Content-Type: application/json' -H 'Idempotency-Key: provider-a:transaction-901' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-901",
       "playerId":"0192F28F-5DC0-7D58-BDB2-814AD6A0F4A1","walletId":"'"$WALLET_ID"'",
       "gameId":"fortune-chimp","kind":"BET","money":{"amount":"","currency":"brl"}}'
```

```text
{"type":"about:blank","title":"Invalid money","status":400,"code":"INVALID_MONEY","correlationId":"01a1471b-6820-7ee8-854d-6d766fa58b4f","errors":[{"field":"playerId","code":"INVALID_FORMAT"},{"field":"roundId","code":"REQUIRED"},{"field":"money.amount","code":"EMPTY"},{"field":"money.currency","code":"UNSUPPORTED"}]}
HTTP 400
```

O valor como número JSON:

```sh
curl -s -X POST localhost:8000/wagering/transactions -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_A" -H 'Content-Type: application/json' -H 'Idempotency-Key: provider-a:transaction-902' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-902",
       "playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"'"$WALLET_ID"'",
       "roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":25.00,"currency":"BRL"}}'
```

```text
{"type":"about:blank","title":"Invalid money","status":400,"code":"INVALID_MONEY","detail":"amount must be a decimal string","correlationId":"01a1471b-6827-7c28-8951-edb920be8746","errors":[{"field":"money.amount","code":"NOT_A_STRING"}]}
HTTP 400
```

Escala de uma casa, `LOSS` com valor e `REFUND` sem referência:

```sh
tx provider-a:transaction-903 transaction-903 BET 25.5
tx provider-a:transaction-904 transaction-904 LOSS 1.00
tx provider-a:transaction-905 transaction-905 REFUND 1.00
```

```text
{"type":"about:blank","title":"Invalid money","status":400,"code":"INVALID_MONEY","correlationId":"01a1471b-6830-76d0-8a80-603aedb94ccb","errors":[{"field":"money.amount","code":"SCALE_TOO_SMALL"}]}
HTTP 400
{"type":"about:blank","title":"Invalid amount for kind","status":400,"code":"INVALID_AMOUNT_FOR_KIND","correlationId":"01a1471b-6838-730d-93f2-8674fa4e90b6","errors":[{"field":"money.amount","code":"MUST_BE_ZERO"}]}
HTTP 400
{"type":"about:blank","title":"Reference required","status":400,"code":"REFERENCE_REQUIRED","correlationId":"01a1471b-6840-7107-a5ef-7da7caf8fbdc","errors":[{"field":"referenceExternalTransactionId","code":"REQUIRED"}]}
HTTP 400
```

Sem `Idempotency-Key`:

```sh
curl -s -X POST localhost:8000/wagering/transactions -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_A" -H 'Content-Type: application/json' -d '{}'
```

```text
{"type":"about:blank","title":"Idempotency key required","status":400,"code":"IDEMPOTENCY_KEY_REQUIRED","detail":"the Idempotency-Key header is required","correlationId":"01a1471b-6846-7b73-aca3-12658d5d1d9d"}
HTTP 400
```

Id do path que não é UUID, cursor adulterado e carteira inexistente:

```sh
curl -s localhost:8000/wagering/transactions/not-a-uuid -w 'HTTP %{http_code}\n' -H "Authorization: Bearer $TOKEN_A"
curl -s "localhost:8000/wallets/$WALLET_ID/ledger?cursor=abc" -w 'HTTP %{http_code}\n' -H "Authorization: Bearer $TOKEN_INT"
curl -s localhost:8000/wallets/0192f291-27dd-7d3f-8071-5f8685deef37 -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_INT"
```

```text
{"type":"about:blank","title":"Invalid request","status":400,"code":"INVALID_REQUEST","detail":"transactionId must be a canonical UUID","correlationId":"01a1471b-684d-7ecd-a771-fddcc14ed37f","errors":[{"field":"transactionId","code":"INVALID_FORMAT"}]}
HTTP 400
{"type":"about:blank","title":"Invalid cursor","status":400,"code":"INVALID_CURSOR","detail":"cursor must be one this API returned and limit an integer from 1 to 200","correlationId":"01a1471b-6854-75ef-a960-629d810964e8","errors":[{"field":"cursor","code":"INVALID_FORMAT"}]}
HTTP 400
{"type":"about:blank","title":"Wallet not found","status":404,"code":"WALLET_NOT_FOUND","correlationId":"01a1471b-685b-719c-ac45-bd9f8b5fd0ee"}
HTTP 404
```

`Content-Type` errado, campo desconhecido e corpo acima do limite (este, pelo nginx, que recusa antes de repassar e
responde com o mesmo contrato da réplica):

```sh
curl -s -X POST localhost:8000/wallets -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_INT" -H 'Content-Type: text/plain' -d 'hello'
curl -s -X POST localhost:8000/wallets -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_INT" -H 'Content-Type: application/json' -d '{"playerId":"x","extra":1}'
head -c 70000 /dev/zero | tr '\0' ' ' | curl -s -X POST localhost:8000/wallets -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $TOKEN_INT" -H 'Content-Type: application/json' --data-binary @-
```

```text
{"type":"about:blank","title":"Unsupported Media Type","status":415,"code":"UNSUPPORTED_MEDIA_TYPE","detail":"Content-Type must be application/json","correlationId":"01a1471b-6862-713d-bee8-dbcc669334f5"}
HTTP 415
{"type":"about:blank","title":"Bad Request","status":400,"code":"INVALID_REQUEST","detail":"unknown field extra","correlationId":"01a1471b-6869-7a32-a11f-b7f86bc8ea60"}
HTTP 400
{"type":"about:blank","title":"Request Entity Too Large","status":413,"code":"PAYLOAD_TOO_LARGE","detail":"body exceeds 65536 bytes","correlationId":"1be29761025e3f9d83ac512025335ade"}
HTTP 413
```

**Indisponibilidade.** Com o Postgres pausado, a readiness vai a `503` e uma requisição que precisa do banco recebe
`503 TEMPORARILY_UNAVAILABLE` com `Retry-After: 2` quando o `HTTP_REQUEST_TIMEOUT` (10s) vence, em vez de ficar
pendurada:

```sh
docker compose pause postgres
curl -s localhost:8000/health/ready -w 'HTTP %{http_code}\n'
curl -s -i localhost:8000/wallets/$WALLET_ID -H "Authorization: Bearer $TOKEN_INT"
docker compose unpause postgres
```

```text
 Container wallet-r1-postgres-1  Paused
{"status":"unavailable","checks":{"postgres":"fail","sqs":"ok"}}
HTTP 503
HTTP/1.1 503 Service Unavailable
Server: nginx
Date: Fri, 16 Oct 2026 23:45:53 GMT
Content-Type: application/problem+json
Content-Length: 158
Connection: keep-alive
Cache-Control: no-store
Retry-After: 2
X-Correlation-Id: 01a1471b-70a8-7c83-96e0-56908c9afa9b

{"type":"about:blank","title":"Temporarily unavailable","status":503,"code":"TEMPORARILY_UNAVAILABLE","correlationId":"01a1471b-70a8-7c83-96e0-56908c9afa9b"}
 Container wallet-r1-postgres-1  Unpaused
```

Depois do `unpause`, a readiness volta a `200` na hora; o healthcheck do container `postgres` leva alguns segundos
para voltar a `healthy`, e até lá um `docker compose run` que dependa dele recusa subir.

```sh
curl -s localhost:8000/health/ready -w 'HTTP %{http_code}\n'
```

```text
{"status":"ok","checks":{"postgres":"ok","sqs":"ok"}}
HTTP 200
```

## Testes

| Comando | O que roda | Precisa de | Duração medida |
|---|---|---|---|
| `make test` | `go test -tags=load ./...`: unitários, testes de arquitetura e os testes das partes puras do gerador de carga | nada além do Go | 14s |
| `make test-race` | `go test -tags=load -race ./...`: os mesmos, com race detector | Go (no Linux, também um compilador C, como o `gcc`) | 16s |
| `make test-failpoints` | `go test -tags=failpoints -race ./...`: os mesmos, com os failpoints reais compilados no lugar dos inertes, mais os testes do pacote dos failpoints que só existem com a tag | Go (no Linux, também um compilador C, como o `gcc`) | 17s |
| `go vet ./...` (`make vet`) | análise estática | nada além do Go | 1s |
| `make lint` | `golangci-lint run --build-tags=integration,e2e,load ./...` e, de novo, com `integration,e2e,failpoints`: a tag `failpoints` troca a versão inerte dos failpoints pela real, e cada passada vê uma delas | golangci-lint v2.15.0 | 4s |
| `gofmt -l .` | lista arquivos fora do `gofmt` (sai vazio) | nada além do Go | menos de 1s |
| `make test-integration` | `go test -tags=integration -race ./...`: unitários e integração, com containers reais | Docker | 2 min 34s |
| `make test-e2e` | `go test -tags=e2e -race -count=1 ./test/e2e/...`: o binário real em várias instâncias, com falhas simuladas | Docker | 2 min 47s |
| `make load-test` | sobe o compose e roda o gerador de carga num container na rede dele: duas concorrências, duas fases cada, com integridade conferida no fim; método e resultados em [`docs/TESTE_DE_CARGA.md`](docs/TESTE_DE_CARGA.md) | Docker | 25 min 27s |
| `go test -tags=load ./test/load/...` | só os testes das partes puras do gerador de carga (também em `make test`) | nada além do Go | 1s |

As durações são de uma execução num Apple M5 (10 núcleos) com Docker Desktop (8 GB), com as imagens já baixadas e o
cache de build do Go quente; numa máquina fria, a primeira execução também compila as dependências e baixa as imagens
dos containers.

**Build tags.** `integration` liga os testes que sobem containers; `e2e` liga o pacote `test/e2e`; `failpoints`
compila os pontos de crash no binário; `load` liga o gerador de carga de `test/load`. Sem nenhuma tag, `go test ./...`
não precisa de Docker. O pacote dos failpoints tem testes próprios com a tag, que `make test-failpoints` roda junto
com o resto dos unitários; só o pacote:

```sh
go test -tags failpoints ./internal/platform/failpoint/
```

**Integração.** Os testes com a tag `integration` sobem containers reais com
[testcontainers-go](https://golang.testcontainers.org/) (`test/testenv`): PostgreSQL 18.6, com o mesmo script de
papéis e todas as migrations; Keycloak 26.8.1, com o mesmo realm; e MiniStack 1.5.27 com `AUTH=true`, provisionado
pelo mesmo `provision.Run`. Nada da infraestrutura é mock. Cada pacote sobe, no seu `TestMain`, só os containers de que
precisa e os derruba no fim; basta o Docker estar rodando, sem o compose. Um pacote só:

```sh
go test -tags=integration -race ./internal/adapter/postgres/...
```

**Múltiplas instâncias e simulação de falhas (e2e).** O `TestMain` de `test/e2e` sobe PostgreSQL, Keycloak e MiniStack
uma vez e compila `cmd/wallet` com `-tags failpoints` (e com `-race`, quando os testes rodam com ele). Cada cenário roda
processos separados desse binário no host, cada um com suas conexões e sua memória, contra as mesmas dependências, e
termina conferindo pela reconciliação que o saldo de cada carteira bate com o ledger. Num binário com a tag,
`FAILPOINTS=nome=exit` faz o processo sair com código 137, como um `kill -9`, exatamente naquele ponto; um nome
desconhecido encerra com código 2. Os pontos são `submit.before_commit`, `submit.after_commit`,
`consumer.after_commit_before_delete`, `outbox.after_claim`, `outbox.after_publish_before_ack` e
`resolver.after_claim`. As pausas de banco e broker usam `docker pause` nos containers do teste. Um cenário só:

```sh
go test -tags=e2e -race -count=1 -run 'TestE2E_PostgresAndSQSOutage/postgres_paused' ./test/e2e/
```

**Os cenários obrigatórios do enunciado e onde estão.**

| Cenário (§13 do enunciado) | Teste e2e (3 instâncias ou processos separados) | Também na integração |
|---|---|---|
| a mesma aposta 50 vezes em paralelo, um débito | `TestE2E_SameBet50Times` (25 por HTTP nas 3 instâncias, 25 pela fila; 49 duplicatas contadas nas métricas) | `TestSubmit_SameBet50Concurrent` |
| 2 × 80.00 sobre 100.00 | `TestE2E_TwoBetsOf80_HTTPandSQS` (uma por canal, em instâncias diferentes) | `TestSubmit_TwoBetsOf80On100` |
| carteiras distintas em paralelo | `TestE2E_ParallelWallets/every_wallet_completes`, `TestE2E_ParallelWallets/a_held_wallet_delays_only_its_own_bets` | `TestSubmit_ParallelWallets` |
| consumidor interrompido depois do commit e antes do delete | `TestE2E_ConsumerKilledAfterCommit` | `TestConsumer_CrashBetweenCommitAndDelete` |
| dois publishers na mesma outbox, recuperação | `TestE2E_OutboxCrashAfterClaim`, `TestE2E_OutboxCrashAfterPublishBeforeAck` | `TestRelay_TwoRelaysNoDoublePublishOnHappyPath`, `TestRelay_RecoversAbandonedLease`, `TestRelay_RepublishKeepsEventID` |
| `REFUND`/`ROLLBACK` antes da referência: resolução e expiração | `TestE2E_PendingReferenceResolvedAndExpired` | `TestResolver_ResolvesPendingRefund`, `TestResolver_ExpiresPending`, `TestResume_ResolvesAfterReferenceArrives`, `TestResume_RejectsAfterTTL` |
| reinício preservando idempotência, pendências e saldo | `TestE2E_RestartPreservesEverything` | — |
| pendência comitada retomada por outra instância | `TestE2E_ResolverKilledAfterClaim` | `TestResolver_TwoResolversNoDoubleProcessing` |
| a mesma operação por HTTP e por SQS | `TestE2E_CrossChannelSameOperation/HTTP_then_SQS`, `TestE2E_CrossChannelSameOperation/SQS_then_HTTP` | — |
| processo morto antes e depois do commit de um envio | `TestE2E_SubmitKilledBeforeCommit`, `TestE2E_SubmitKilledAfterCommit`, `TestE2E_ConsumerKilledBeforeCommit` | — |
| indisponibilidade temporária do PostgreSQL e do SQS | `TestE2E_PostgresAndSQSOutage/postgres_paused`, `TestE2E_PostgresAndSQSOutage/MiniStack_paused` | `TestConsumer_TransientRetry`, `TestApp_ReadinessFollowsTheBroker` (readiness `503` com `"sqs":"fail"` e a volta a `200`) |
| `SIGTERM` com carga, atrás do nginx do compose | `TestE2E_SIGTERMUnderLoadThroughNginx` | `TestApp_DrainKeepsServingWhileNotReady`, `TestConsumer_StopReleasesUnstarted` |
| autenticação real, isolamento entre provedores, sem efeito em acesso negado | — | `test/integration`: `TestAuth_MissingInvalidExpiredToken`, `TestAuthz_ProviderIsolation`, `TestAuthz_CrossProviderReversal`, `TestAuthz_WalletEndpointsInternalOnly`, `TestAuthz_NoSideEffects` |
| composição Fx, partida e encerramento sem vazamento | — | `TestApp_StartStop_NoLeaks`, `TestApp_FailsFastOnBadDeps`, `TestApp_StopOrder`; sem Docker, `TestModule_Validates` |

**O nginx no e2e.** O cenário do `SIGTERM` roda o nginx do compose num container, com a mesma configuração, na frente
de instâncias que rodam no host e que ele alcança como `host.docker.internal`, fixado no primeiro endereço IPv4 do
nome. No Docker Desktop, o nome leva à loopback do host. Num Docker Engine no Linux, onde o nome não existe, o
container o recebe como `host-gateway`, o endereço da bridge no host, e as instâncias do cenário escutam em `0.0.0.0`,
porque esse endereço não é loopback.

**No Linux.** Além do Docker Desktop (macOS), as suítes passaram num Docker Engine 29.8.2 de Linux (arm64):
`go test ./...`, `make test-race`, `make test-failpoints`, `make test-integration`, `make test-e2e` com todos os
cenários (o do nginx pelo `host-gateway`) e o compose (`docker compose up --build -d`, a readiness pelo nginx e uma
aposta). O daemon rodou num container `docker:29.8.2-dind-alpine3.24` com o Go 1.25.6, e os testes, no mesmo
container, como usuário comum do grupo `docker`: daemon e testes dividem a rede, como numa máquina Linux. No Linux, o
race detector precisa de cgo, então os alvos com `-race` (`make test-race`, `make test-failpoints`,
`make test-integration` e `make test-e2e`) pedem um compilador C, como o `gcc`, no `PATH`; sem ele, o Go para com
`-race requires cgo`.

## Observabilidade

**Logs.** JSON em stdout, uma linha por evento, com `instance` e os ids disponíveis (`correlationId`, `messageId`,
`transactionId`, `walletId`, `providerId` e, com tracing ligado, `traceId` e `spanId`), em chaves camelCase. Tokens,
`Authorization`, corpos e valores monetários nunca vão para o log, sem exceção: a divergência da reconciliação registra
o `walletId`, a moeda e quantos lançamentos conferiu, e os valores ficam na resposta; o erro de uma invariante que o
banco recusa leva só o código e o nome da trigger, sem a mensagem do servidor, que cita os valores (detalhes em
[ARCHITECTURE.md](ARCHITECTURE.md#15-observabilidade)). Uma latência medida sai em milissegundos inteiros
(`durationMs`); as outras durações, como texto (`"retryIn":"2s"`). A linha de acesso de cada requisição registra a rota
casada, nunca o path cru:

```sh
docker compose logs --no-log-prefix app-1 app-2 app-3 | grep '"route":"POST /wagering/transactions"' | tail -n 1
```

```json
{"time":"2026-10-16T23:45:40.934829172Z","level":"INFO","msg":"http request","instance":"app-1","method":"POST","route":"POST /wagering/transactions","status":400,"durationMs":0,"providerId":"provider-a","correlationId":"01a1471b-6846-7b73-aca3-12658d5d1d9d"}
```

**Métricas.** Prometheus em `GET /metrics` na porta admin (`ADMIN_ADDR`, `:9090`; no compose, `9091`, `9092` e `9093`,
uma por réplica): transações por status e código, duplicatas, novas tentativas de transação, mensagens e DLQ, atraso da
outbox, resoluções de referência, divergências de reconciliação e latência de processamento e HTTP. Cada réplica conta o
que ela mesma fez, então o total é a soma das três:

```sh
for port in 9091 9092 9093; do
  echo "== $port"
  curl -s localhost:$port/metrics | grep -E '^(wagering_transactions_total|wagering_duplicates_total|sqs_dlq_total)'
done
```

```text
== 9091
wagering_transactions_total{channel="http",failure_code="",kind="BET",status="PROCESSED"} 1
wagering_transactions_total{channel="http",failure_code="",kind="WIN",status="PROCESSED"} 1
wagering_transactions_total{channel="http",failure_code="REFERENCE_ALREADY_REVERSED",kind="ROLLBACK",status="REJECTED"} 1
== 9092
sqs_dlq_total{reason="UNAUTHENTICATED"} 1
wagering_transactions_total{channel="http",failure_code="",kind="BET",status="PROCESSED"} 1
wagering_transactions_total{channel="http",failure_code="",kind="REFUND",status="PENDING_REFERENCE"} 1
wagering_transactions_total{channel="http",failure_code="",kind="REFUND",status="PROCESSED"} 1
wagering_transactions_total{channel="resolver",failure_code="",kind="REFUND",status="PROCESSED"} 1
wagering_transactions_total{channel="sqs",failure_code="",kind="BET",status="PROCESSED"} 1
== 9093
wagering_duplicates_total{channel="http",source="idempotency"} 1
wagering_transactions_total{channel="http",failure_code="",kind="ROLLBACK",status="PROCESSED"} 1
wagering_transactions_total{channel="http",failure_code="INSUFFICIENT_FUNDS",kind="BET",status="REJECTED"} 1
```

**Health.** `GET /health/live` (o processo atende) e `GET /health/ready` (banco, SQS e shutdown; `503` quando não
pronta), públicos, na porta da API, como nos exemplos acima. O corpo mostra só `ok` ou `fail` por checagem; o motivo
vai para o log.

**Tracing.** OpenTelemetry, desligado por padrão. O overlay `docker-compose.tracing.yml` acrescenta um Jaeger
(`jaegertracing/jaeger:2.20.0`, all-in-one, em memória) e liga as três réplicas a ele com
`OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`; o `docker-compose.yml` não muda:

```sh
docker compose -f docker-compose.yml -f docker-compose.tracing.yml up --build -d
```

A UI fica em <http://127.0.0.1:16686> (serviço `wallet`). Os spans são exportados em lote, então um trace aparece em
até uns 5s. Depois da aposta e da mensagem da fila dos exemplos, a API JSON do Jaeger lista os traces, com o span que
os começou e as réplicas por onde passaram (abaixo, um trecho; cada passada do relay que publicou algo é um trace
`outbox.relay`):

```sh
curl -s '127.0.0.1:16686/api/traces?service=wallet&limit=20' | jq -r '.data[] | [.traceID, (.spans | length),
  (.spans | min_by(.startTime) | .operationName),
  ([.processes[].tags[] | select(.key == "service.instance.id") | .value] | unique | join(","))] | @tsv'
```

```text
8b7b9973d40bf8d0148b93b6f5777fd8	18	POST /wagering/transactions	app-1,app-2
4bf92f3577b34da6a3ce929d0e0e4736	16	process wager-transactions.fifo	app-3
c46ec8d605f904f59ffc1751135eddf2	10	POST /wallets	app-1
4ec04a6e89e39f8ea2c0e24b38344376	5	outbox.relay	app-1
```

O trace da aposta passou por duas réplicas: a `app-2` atendeu a requisição, e o relay da `app-1` publicou os eventos no
mesmo trace. Os spans dele, na ordem em que começaram:

```sh
curl -s 127.0.0.1:16686/api/traces/8b7b9973d40bf8d0148b93b6f5777fd8 \
  | jq -r '.data[0].spans | sort_by(.startTime)[] | .operationName'
```

```text
POST /wagering/transactions
wagering.submit
uow.snapshot
BEGIN
SELECT wager_transactions
COMMIT
uow.do
BEGIN
SELECT wallets
SELECT wager_transactions
SELECT wager_transactions
INSERT wager_transactions
INSERT wallet_ledger_entries
UPDATE wallets
BATCH INSERT outbox_events
COMMIT
send wallet-events.fifo
send wallet-events.fifo
```

Toda linha de log gravada sob um span leva `traceId` e `spanId`, então do log se chega ao trace
(`http://127.0.0.1:16686/trace/<traceId>`):

```json
{"time":"2026-10-19T11:15:56.393789466Z","level":"INFO","msg":"http request","instance":"app-2","method":"POST","route":"POST /wagering/transactions","status":201,"durationMs":3,"providerId":"provider-a","correlationId":"01a153e0-1365-7e78-8839-d8b58dd415b0","traceId":"8b7b9973d40bf8d0148b93b6f5777fd8","spanId":"fb76775ffbc0e440"}
```

Uma chamada que chega com contexto W3C continua o trace de quem chamou: o header `traceparent` no HTTP, ou o atributo
`traceparent` na mensagem da fila. No exemplo do SQS, acrescentar ao `ATTRS`
`traceparent: {DataType: "String", StringValue: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}` faz a
mensagem ser processada no trace `4bf92f35…`, o segundo da lista acima. Os eventos publicados em `wallet-events.fifo`
levam o atributo `traceparent` do span que os enviou, para que um consumidor deles continue o trace. Uma instância no
host (ver [Variáveis de ambiente](#variáveis-de-ambiente)) entra no mesmo Jaeger com
`OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318`. Valores, saldos, tokens e corpos nunca vão para um span. Para
derrubar, `docker compose -f docker-compose.yml -f docker-compose.tracing.yml down -v`. O que é rastreado, as regras de
cardinalidade e o custo com o tracing desligado: [ARCHITECTURE.md](ARCHITECTURE.md#tracing).

Detalhes das métricas e dos rótulos: [ARCHITECTURE.md](ARCHITECTURE.md#15-observabilidade).
