# Arquitetura

Este documento registra as decisões técnicas do serviço de carteiras: o que foi escolhido, por quê, e o que ficou de
fora. As regras de negócio (o que cada operação faz, quando é recusada, o que fica gravado e publicado, e os testes que
provam cada uma) estão em [`docs/REGRAS_DE_NEGOCIO.md`](docs/REGRAS_DE_NEGOCIO.md); aqui fica o como e o porquê
técnico. O enunciado está em [`docs/CHALLENGE.md`](docs/CHALLENGE.md); como subir e testar está no
[`README.md`](README.md).

Convenções: identificadores e comentários de código em inglês; este documento e o README em português. "Provedor" é o
cliente externo que envia apostas (provider-a, provider-b); "serviço interno" é o cliente que administra carteiras
(wallet-service).

## Sumário

1. [Visão geral e topologia](#1-visão-geral-e-topologia)
2. [Organização de pacotes](#2-organização-de-pacotes)
3. [Dinheiro](#3-dinheiro)
4. [Modelo de domínio](#4-modelo-de-domínio)
5. [Máquina de estados e classes de falha](#5-máquina-de-estados-e-classes-de-falha)
6. [Persistência](#6-persistência)
7. [Concorrência](#7-concorrência)
8. [Idempotência](#8-idempotência)
9. [Operações e reversões](#9-operações-e-reversões)
10. [Referências pendentes](#10-referências-pendentes)
11. [SQS de entrada](#11-sqs-de-entrada)
12. [Outbox e eventos](#12-outbox-e-eventos)
13. [Autenticação e autorização](#13-autenticação-e-autorização)
14. [Fx, ciclo de vida e shutdown](#14-fx-ciclo-de-vida-e-shutdown)
15. [Observabilidade](#15-observabilidade)
    - [Tracing](#tracing)
16. [Contrato de erros HTTP](#16-contrato-de-erros-http)
17. [Testes e simulação de falhas](#17-testes-e-simulação-de-falhas)
18. [Limitações, interpretações e trabalho não concluído](#18-limitações-interpretações-e-trabalho-não-concluído)

---

## 1. Visão geral e topologia

Um único binário, `wallet`, roda em três réplicas idênticas. Cada réplica tem quatro componentes, ligados ou desligados
por variável de ambiente e todos ligados por padrão:

| Componente | Variável | Função |
|---|---|---|
| API HTTP | `HTTP_ENABLED` | carteiras (serviço interno) e transações (provedores) |
| Consumidor SQS | `CONSUMER_ENABLED` | lê `wager-transactions.fifo` e chama o mesmo caso de uso do HTTP |
| Publicador da outbox | `OUTBOX_RELAY_ENABLED` | publica os eventos comitados em `wallet-events.fifo` |
| Resolvedor de referências | `RESOLVER_ENABLED` | retoma transações que esperam a referência |

O PostgreSQL é a única fonte da verdade. Saldo, ledger, transação, inbox e outbox são gravados no mesmo commit. As
réplicas não guardam em memória nada de que a correção dependa. Elas se coordenam só pelo banco, por lock da linha da
carteira, índices únicos e leases com prazo.

```
  provedor A/B ──client_credentials──► Keycloak (realm wagering) ◄── JWKS ──┐
       │                                                                    │
       ├── HTTP ──► nginx :8000 (round-robin) ──► app-1 / app-2 / app-3 ────┤
       │                                            │  API HTTP + OIDC      │
       └── SQS (usuário IAM do provedor) ──┐        │  consumidor SQS       │
                                           ▼        │  relay da outbox      │
                          MiniStack  wager-transactions.fifo ─► DLQ         │
                          (SQS+IAM)  wallet-events.fifo ──────► DLQ         │
                                           ▲        │                       │
                                           └────────┤  resolvedor           │
                                                    ▼                       │
                                             PostgreSQL 18 ◄────────────────┘
  serviço interno ── HTTP (escopo wallets:manage) ──► nginx
```

No compose: `postgres`, `keycloak`, `ministack`, os one-shots `migrate` e `aws-provision`, `app-1..3` (portas 8081–8083,
admin 9091–9093), `nginx` (porta 8000) e, só sob demanda (`docker compose run --rm aws …`), um AWS CLI na rede do
compose. As portas de desenvolvimento são publicadas só em `127.0.0.1`.

O nginx open source não tem health check ativo, e a imagem distroless das réplicas não tem com que rodar um
healthcheck. O nginx usa checagem passiva (`max_fails=1 fail_timeout=5s`), resolve os nomes das réplicas de novo a
cada 5s no DNS do Docker e reenvia para outra réplica, até 3 tentativas, a requisição cuja réplica recusou a
conexão, a resetou ou não respondeu a tempo (`proxy_next_upstream error timeout non_idempotent`), inclusive POST. Uma
resposta que a réplica chegou a enviar, mesmo de erro, volta como está. O reenvio de um POST é seguro por motivos
diferentes em cada rota: `POST /wagering/transactions` carrega `Idempotency-Key`, e a repetição vira replay;
`POST /wallets` não tem chave, mas só existe uma carteira por jogador e moeda, então a repetição de uma abertura que
já foi comitada responde `409 WALLET_ALREADY_EXISTS` e nunca cria outra; a reconciliação só lê. Quando não sobra
réplica, o nginx responde ele mesmo o `503 TEMPORARILY_UNAVAILABLE` com `Retry-After: 2` do serviço, e o `413` acima
de 64 KiB também sai no contrato do serviço.

Os tempos do nginx seguem os das réplicas. As conexões ociosas reaproveitadas fecham em 50s, antes dos 60s de
`HTTP_IDLE_TIMEOUT`, para que a réplica não feche do outro lado uma conexão em que o nginx está escrevendo. Os 20s de
`proxy_read_timeout` e `proxy_send_timeout` ficam acima dos 10s e 15s de leitura e escrita das réplicas: o nginx só
desiste de uma requisição, e a reenvia, quando a réplica já não poderia respondê-la. Os 2s de `proxy_connect_timeout`
bastam numa rede Docker local, onde uma conexão mais lenta é com uma réplica que sumiu. O limite de corpo aparece em
três lugares que mudam juntos: o padrão de `HTTP_MAX_BODY_BYTES`, o `client_max_body_size` e o `detail` do `413` do
nginx. As respostas que o próprio nginx dá levam o `X-Correlation-Id` do cliente quando ele segue a regra dos ids de
texto, ou um gerado. O log de acesso dele é uma linha JSON por requisição, com horário, método, a URI crua
(`$request_uri`, query incluída, então o `cursor` e o `limit` do ledger aparecem nela), status, duração, as réplicas
tentadas e o status de cada uma, o `X-Request-Id` que o cliente mandou e o `X-Correlation-Id` da resposta; nenhum outro
header e nenhum corpo. O log do serviço, ao contrário, registra a rota casada, e não o path. O healthcheck do container
do nginx usa um endpoint próprio, só na loopback, que não é publicado nem gera tráfego nas réplicas; o compose sobe o
nginx depois das réplicas e o para antes delas.

Uma única imagem serve a todos os comandos (`wallet`, `migrate`, `provision`), então os one-shots rodam exatamente o
build do serviço. Só o `migrate` a constrói, porque builds paralelos da mesma tag no compose podem falhar ao marcá-la, e
ela nunca é baixada (`pull_policy: never`): um registry poderia servir outra imagem com o mesmo nome. As bases de build
(`golang:1.25.6-alpine`) e de execução (distroless `static-debian12:nonroot`) estão fixadas por digest, para que uma tag
republicada não troque o toolchain nem a base, e `GOTOOLCHAIN=local` faz o build falhar em vez de baixar outro
toolchain. O diretório `/shared/aws` nasce na imagem com dono `nonroot`: um volume nomeado é inicializado a partir da
imagem, dono incluído, e é isso que deixa o provisionamento, que roda como `nonroot`, escrever nele. O healthcheck do
Postgres força TCP (`-h 127.0.0.1`), porque na primeira inicialização o servidor temporário do entrypoint só escuta no
socket, e uma sonda pelo socket diria pronto antes de os scripts de init rodarem; o volume fica um nível acima de
`PGDATA`, porque a imagem 18 guarda o cluster num subdiretório por versão.

## 2. Organização de pacotes

A regra de dependência aponta para dentro: o domínio não conhece ninguém, a aplicação conhece o domínio, e os
adaptadores conhecem os dois. As interfaces ficam no pacote que as consome, como é idiomático em Go, e não num pacote
`interfaces` genérico.

```
cmd/
  wallet/          main: lê a configuração e roda fx.New(bootstrap.Module(cfg))
  migrate/         up | down N | version | force V
  provision/       cria filas, redrive e usuários IAM e grava o arquivo de credenciais
internal/
  domain/          Go puro: stdlib sem I/O + google/uuid, nada de Fx, HTTP, SQS ou pgx
    money/         Money, Currency, Parse, aritmética com overflow, JSON
    wallet/        Wallet (agregado), LedgerEntry, Direction
    wager/         Command, Transaction, Kind, Status, FailureCode, Decide, hash canônico
    event/         Envelope e os quatro eventos concretos
  app/             portas (UnitOfWork, repositórios, Clock, IDGenerator), erros de infraestrutura, registro da outbox
    wallets/       abrir, ler, paginar o ledger e reconciliar carteiras
    wagering/      Submit (HTTP e SQS), consultas, ClaimDue e Resume das pendências
  adapter/
    postgres/      pool, UnitOfWork, repositórios, classificação de SQLSTATE, migrations, conferência do schema
    httpapi/       servidor, middlewares, problem+json, decoder estrito, cursor, rotas
    oidc/          verificador JWT/JWKS, Principal
    sqsin/         consumidor, envelope estrito, identidade do produtor, DLQ, extensor de visibilidade
    sqsout/        publicador dos eventos em lote
    awsclient/     configuração AWS e resolução das URLs das filas
  worker/          Runner genérico dos laços de fundo (start/stop/done)
    outboxrelay/   relay da outbox e medição do atraso
    resolver/      resolvedor das referências pendentes
  bootstrap/       módulos Fx, hooks de ciclo de vida, ordem e orçamento do shutdown
  platform/        config, logging, metrics, tracing, health, clock, idgen, retry, failpoint
  tools/provision/ o provisionamento, usado por cmd/provision e pelos testes
  archtest/        testes de arquitetura (dependências do domínio, proibição de float, SDK do tracing)
migrations/        000001…000010 .up.sql / .down.sql, embutidas com go:embed
deploy/            realm do Keycloak, papéis do Postgres, nginx.conf
test/
  testenv/         PostgreSQL, Keycloak e MiniStack reais via testcontainers-go
  integration/     o serviço inteiro em processo, falando só HTTP com tokens do Keycloak real (auth e isolamento)
  e2e/             o binário em várias instâncias, com failpoints e falhas simuladas
  load/            gerador de carga (build tag load), roda num container na rede do compose
```

`internal/archtest` impõe a regra mecanicamente. Ele falha se `internal/domain` importar qualquer pacote além de
`google/uuid` e da stdlib sem I/O: o domínio decide, e quem fala com o mundo são os adaptadores. Ficam de fora da stdlib
`net`, `os`, `database/sql`, `io/fs`, `io/ioutil`, `log`, `syscall`, `plugin`, `crypto/rand` e `math/rand`, com tudo o
que está abaixo de cada um (`net/http`, `os/exec`…); só conta o import direto do domínio, porque a stdlib usa alguns
deles por dentro, como o `fmt` usa o `os`. Um segundo teste, sobre a AST de todo o código que não é de teste, `test/`
inclusive (onde fica o gerador de carga), em qualquer build tag, recusa `float32`, `float64`, as conversões de
`strconv`/`math` que passam por float e literais com ponto decimal. A única exceção é `internal/platform/metrics`: a API
do Prometheus recebe `float64`, e ali só passam contagens e durações, nunca dinheiro. Um terceiro teste guarda o SDK do
OpenTelemetry: só `internal/bootstrap` e `internal/platform/tracing/otlp`, que monta o provider e o exportador, importam
o SDK e os exportadores, e só os testes importam `tracingtest`; o resto do código, `internal/app` inclusive, conhece só
a API do OTel, por `internal/platform/tracing`, e não arrasta o SDK, o exportador nem o gRPC que ele liga.

Todo horário e todo id que o serviço grava, publica em evento ou usa numa decisão de negócio vêm de `Clock` e
`IDGenerator` injetados, o que deixa os testes determinísticos: `uuid.New*()` só é chamado em `platform/idgen`, e
`time.Now()` só em `platform/clock`, com uma exceção, a medição de latência. O envio e a retomada de transações
(`internal/app/wagering`) e o middleware de acesso do HTTP (`internal/adapter/httpapi`) cronometram com `time.Now` e
`time.Since` a duração que vai para as métricas e, no HTTP, para a linha de acesso; esse valor não entra em nenhum dado
gravado nem em nenhuma decisão. Timeouts, timers e polling também usam o relógio do processo. Leases e prazos de
"vencido" são comparados com o `now()` do banco, para que réplicas com relógios diferentes concordem sobre quem é dono
de quê. O relógio do sistema sempre devolve UTC, e os ids são UUIDv7: o prefixo ordenado por tempo mantém as inserções
nos índices de chave primária quase sequenciais, ao contrário do UUIDv4.

## 3. Dinheiro

| Aspecto | Decisão |
|---|---|
| Representação | `money.Money{minor int64, currency Currency}`, campos não exportados, imutável |
| Escala | fixa em 2 casas para todas as moedas aceitas |
| Moedas | lista fechada ISO 4217: `BRL`, `USD`, `EUR` (todas com 2 casas); um `Currency` só nasce de `ParseCurrency` ou de `money.BRL()`, uma função e não uma variável, para que nenhum importador a reatribua |
| Limites | de −92.233.720.368.547.758,08 a 92.233.720.368.547.758,07, o intervalo de `int64` em centavos (`"-92233720368547758.08"` a `"92233720368547758.07"`); a entrada externa só chega ao máximo, porque nunca é negativa |
| Overflow | verificado em `Parse` (dígito a dígito) e em `Add` e `Sub`: `ErrOverflow`, nunca volta silenciosa |
| Moedas diferentes | `Add` e `Sub` devolvem `ErrCurrencyMismatch`; `Equal` é falso |
| Zero value | `Money{}` não tem moeda e é recusado por qualquer operação (`ErrUninitialized`) |
| Negativos | aceitos em cálculos internos (`FromMinor`, diferença da reconciliação); nunca em entrada externa nem no saldo |
| Banco | `amount_minor BIGINT` + `currency CHAR(3)`, sem conversão |
| JSON | `{"amount":"25.00","currency":"BRL"}`; `amount` como número JSON é recusado |
| Formatação | `String()` e `MarshalJSON` com aritmética inteira: `"975.00"`, `"-5.00"`, `"0.00"` |

`int64` em centavos foi preferido a uma biblioteca decimal. O desafio não precisa de precisão arbitrária, e um decimal
mapeia para `NUMERIC` e abre portas para conversões com float (`Float64()`, `NewFromFloat`). Inteiros mantêm a
aritmética exata e barata, e o limite é mais que suficiente para carteiras.

**Parsing estrito, sem normalização.** Valor, moeda, UUIDs e ids de texto só são aceitos na forma canônica, e qualquer
outra grafia é recusada com o motivo no campo, em vez de corrigida; as formas e os códigos de recusa estão em
[REGRAS §1](docs/REGRAS_DE_NEGOCIO.md#1-dinheiro) e [§4](docs/REGRAS_DE_NEGOCIO.md#4-operações). Como nada é
normalizado, cada valor tem uma única grafia: duas requisições que dizem o mesmo são idênticas byte a byte, o hash de
idempotência é calculado sobre exatamente o que foi recebido, e nenhuma regra de normalização pode produzir um conflito
de idempotência falso nem adivinhar o que o cliente quis dizer. Os dígitos e os ids de texto são conferidos byte a byte,
e não com `unicode.IsDigit` nem com expressão regular: dígitos não ASCII ficam de fora, e não há armadilhas como um `$`
que casa antes de uma quebra de linha final. Um UUID é comparado com a sua renderização canônica, porque `uuid.Parse`
sozinho aceita maiúsculas, chaves, o prefixo `urn:uuid:` e hífens faltando, o que daria a um id várias grafias e vários
hashes. As checagens do valor vão do diagnóstico mais específico ao mais genérico, para que `1e3` seja recusado como
notação científica e não como "malformado".

## 4. Modelo de domínio

Todas as entidades têm estado encapsulado (campos não exportados), construtores que validam e métodos explícitos de
transição. **Criação e reidratação são separadas**, menos no lançamento, um valor imutável que passa pelas mesmas
validações nos dois caminhos:

| Entidade | Criação | Reidratação |
|---|---|---|
| `wallet.Wallet` | `Open(...)`: versão 1; com saldo inicial positivo, já devolve o `LedgerEntry` da abertura | `Rehydrate(snapshot)`: valida o estado gravado; não gera lançamento, não muda versão, não emite evento |
| `wallet.LedgerEntry` | `NewLedgerEntry`, o único construtor, que valida `after = before ± valor`; o domínio o chama em `Open` (o lançamento da abertura) e em `Debit`/`Credit` | o repositório reconstrói o registro gravado pelo mesmo `NewLedgerEntry`, então um lançamento que viole a aritmética não carrega |
| `wager.Transaction` | `NewExternal(cmd)` (provedor) ou `NewOpening(...)` (interna), sempre em `PENDING` | `Rehydrate(snapshot)`: aceita qualquer estado válido sem reaplicar transições |

A carteira é a raiz do agregado financeiro. O saldo só muda por `Debit` e `Credit`, que conferem a moeda, preservam
saldo ≥ 0 e incrementam a versão exatamente uma vez por mudança de saldo. `updatedAt` nunca anda para trás (fica no
maior valor entre o atual e o `now` recebido), porque réplicas com relógios levemente diferentes não podem gravar uma
linha que a reidratação depois recusaria.

`wager.Command` é a entrada validada de uma operação externa. `NewCommand(CommandInput)` é o único caminho,
compartilhado por HTTP e SQS. Ele lista **todos** os campos inválidos de uma vez (`*ValidationError` com
`[]FieldError`). Também recusa o UUID nulo em `playerId`/`walletId` e uma referência igual ao próprio
`externalTransactionId` (`SELF_REFERENCE`, um 400: é erro de entrada corrigível, não uma rejeição de negócio).

`wager.Decide` é uma função pura: recebe a transação, a carteira, a referência encontrada (se houver) e o horário, e
devolve o novo estado, o lançamento e se a transação acabou de entrar em espera. Não lê relógio nem faz I/O; o mesmo
input dá sempre o mesmo resultado.

**Erros classificáveis.** Rejeição de negócio nunca é `error` em `Decide`: ela fica registrada na transação
(`REJECTED` + `FailureCode`). Os erros do domínio são sentinelas comparáveis com `errors.Is` (`ErrInvalidTransition`,
`ErrTerminalState`, `ErrInvalidTransaction`, `ErrCurrencyMismatch`…) ou tipos inspecionáveis com `errors.As`
(`*wager.ValidationError`). `panic` não representa nenhuma regra de negócio. Os únicos `panic` do código cobrem falhas
que nenhum chamador consegue tratar (o gerador de aleatoriedade do sistema operacional falhar, por exemplo). Os erros
nomeiam a regra violada e nunca o valor, porque os valores incluem texto do cliente e os erros acabam no log.

**Transições atômicas.** Toda transição trabalha numa cópia do estado e só a adota se o resultado passar pelas mesmas
invariantes da reidratação; uma transição que falha deixa a transação como estava, e nenhum estado alcançável deixa de
carregar. `Decide` move o dinheiro numa cópia da carteira, que substitui a original só depois que a transação registrou
o movimento. `NewExternal` confere os identificadores com as regras de `NewCommand`, mas `Rehydrate` não: elas são o
contrato da entrada, que pode mudar, e uma linha aceita sob um contrato anterior precisa continuar carregando. O status
é conferido antes dos argumentos, para que um chamador que chega a uma transação que outro acabou de decidir receba
`ErrTerminalState` e leia o resultado gravado.

## 5. Máquina de estados e classes de falha

```
             AwaitReference                RescheduleReference
  PENDING ─────────────────► PENDING_REFERENCE ◄──────┐
     │                           │   │  └─────────────┘
     │ MarkProcessed / Reject    │   │ MarkProcessed / Reject (inclusive expiração)
     ▼                           ▼   ▼
  PROCESSED · REJECTED · FAILED  (terminais: nenhuma transição sai deles)
                     ▲
                     └── NotePermanentFailure ao atingir o limite (resolvedor)
```

- `PENDING` existe só em memória: uma operação sem dependência é decidida na mesma transação SQL em que é registrada e é
  gravada já no estado final. A única pendência comitada é `PENDING_REFERENCE`.
- Transição inválida → `ErrInvalidTransition`; qualquer transição a partir de terminal → `ErrTerminalState`. O trigger
  `wager_transactions_guard` repete a regra no banco (ver §6).
- `FAILED` registra, para auditoria, uma falha permanente de infraestrutura que o resolvedor encontrou várias vezes na
  mesma linha (3, configurável em `PERMANENT_FAILURE_LIMIT`), com `failure_code = PROCESSING_FAILED`
  ([REGRAS §6](docs/REGRAS_DE_NEGOCIO.md#6-referências-pendentes)).

**Transitória × permanente.** O adaptador do Postgres classifica todo erro em exatamente uma das classes de
`internal/app`, pelo SQLSTATE e pelo nome da constraint, nunca pela mensagem:

| Erro | Classe (`app.Err…`) | Tratamento |
|---|---|---|
| `40001` serialização, `40P01` deadlock, `55P03` lock timeout | `ErrRetryable` | o caso de uso repete a transação inteira (até `DB_TX_MAX_ATTEMPTS`, padrão 3, com backoff exponencial e full jitter); esgotado, vira `ErrUnavailable` |
| versão da carteira mudou (UPDATE condicionado afetou 0 linhas) | `ErrRetryable` | idem |
| `23505` em `ux_tx_provider_key`, `ux_tx_provider_external`, `ux_tx_single_reversal` | `ErrRetryable` | corrida entre duas primeiras entregas: o retry encontra a linha vencedora e responde replay, 409 ou `REFERENCE_ALREADY_REVERSED` |
| `23505` em `ux_wallets_player_currency` | `ErrAlreadyExists` | `409 WALLET_ALREADY_EXISTS`, com o id da carteira existente em `walletId` (§8) |
| falha de conexão, `08*`, `53*`, `57*` (incl. `statement_timeout`), rede, EOF, conexão fechada, contexto expirado, pool fechado | `ErrUnavailable` | HTTP `503` + `Retry-After: 2`; SQS: a mensagem volta com backoff; resolvedor: tenta de novo depois. Nada é gravado como falha |
| `22*`, `23*` restantes, `42*`, `WL001`, `WL002`, `P0001`, escrita em transação somente leitura, erro do driver que não vem da rede (valor que não codifica, conexão ainda ocupada com outro comando) | `ErrPermanent` | é bug: HTTP 500 sem persistir; SQS: DLQ; resolvedor: conta para `FAILED` |

A espera entre as tentativas parte de 10ms e para em 100ms: quem venceu a disputa segura a carteira por uma transação
curta, de poucos milissegundos, e o teto mantém as tentativas de uma requisição bem abaixo do `WriteTimeout`. O sorteio
evita que dois escritores que colidiram na mesma linha colidam de novo em compasso. O motivo de cada repetição é um
conjunto fechado (`serialization`, `deadlock`, `lock_timeout`, `version`, `unique_race`, ou `retryable` para o resto),
porque vira rótulo de `db_tx_retries_total` e um rótulo tirado do texto do erro cresceria sem limite. Uma falha ao abrir
a sessão (excesso de conexões, servidor subindo, credencial recusada) é indisponibilidade mesmo que o servidor tenha
respondido: o comando nem rodou, e isso não pode ir para a DLQ nem contar como falha permanente de uma pendência.

O pgx marca como "seguro repetir" (`pgconn.SafeToRetry`) todo erro que acontece antes de ele enviar algo, e isso inclui
a conexão ocupada com um comando anterior, que só acontece por bug do chamador e falharia de novo igual. Por isso só a
conexão já fechada entra como indisponibilidade, e o resto desses erros é permanente.

Um commit em dúvida (a conexão cai durante o `COMMIT`) é classificado como indisponibilidade. Repetir é seguro porque a
operação é idempotente: se o commit tiver passado, o retry vira replay.

## 6. Persistência

**Biblioteca.** `pgx v5` (`pgxpool`) com SQL escrito à mão. Locks, isolamento e constraints ficam visíveis no código
(`FOR NO KEY UPDATE`, `SKIP LOCKED`, `WHERE version = $n`). Um ORM esconderia exatamente o que precisa ser auditado.
`sqlc` foi dispensado: é mais ferramental para pouco ganho com este volume de queries.

**Delimitação da transação.** `app.UnitOfWork` é o único lugar com `BEGIN` e `COMMIT`:

```go
Do(ctx, func(ctx context.Context, r app.Repos) error) error        // READ COMMITTED, leitura e escrita
Snapshot(ctx, func(ctx context.Context, r app.Repos) error) error  // REPEATABLE READ READ ONLY
```

O caso de uso abre a unidade de trabalho e recebe repositórios presos àquela transação. Os repositórios nunca veem o
pool, então não conseguem abrir nem fechar transação por conta própria. Commit se `fn` devolve `nil`; rollback em erro
ou `panic`, num contexto desacoplado do chamador, para não perder a conexão quando o chamador já cancelou. As triggers
deferidas rodam no commit, então uma invariante violada aparece como erro do commit (`ErrPermanent`).

`READ COMMITTED` basta porque nenhuma decisão se apoia em linha não travada: a carteira é travada antes de o saldo ser
lido, e o banco reconfere todas as invariantes no commit. O nível é fixado explicitamente, não herdado do servidor. A
reconciliação usa `Snapshot`, uma visão única e consistente sem travar escritores; o `READ ONLY` transforma em erro uma
escrita que escape para dentro dela.

Cada conexão nasce com `application_name` = id da réplica, `lock_timeout` (padrão 2s) e `statement_timeout` (padrão 5s),
como parâmetros de inicialização da sessão, e não como `SET`, para valer também em conexões que o pool recria. Os
tempos vão em milissegundos, arredondando para cima: um valor positivo arredondado para 0 desligaria o timeout. A
configuração exige `lock_timeout` menor que `statement_timeout`: assim a espera por uma carteira disputada termina como
`55P03`, que o caso de uso repete, e não como `57014`, que é indisponibilidade (§5). O pgx lê `timestamptz` no fuso
local; os repositórios devolvem todo horário em UTC, que é o que o domínio, os eventos e os replays usam.

**Repositórios.** Os repositórios gravam o que o agregado expõe no seu snapshot e carregam as linhas de volta pela
reidratação do domínio, então uma linha que viola uma invariante é reportada como erro permanente, nunca entregue. As
consultas pelas chaves de um provedor repetem `origin = 'EXTERNAL'`, que as colunas já implicam, porque é o predicado
dos índices únicos parciais e sem ele o planejador não os usa. O `UPDATE` de uma transação escreve só as colunas que uma
transição muda: as colunas de negócio são de escrita única, e `wager_transactions_guard` recusaria até um `SET` com o
valor que elas já têm. Ele não confere versão: conta com o `FOR NO KEY UPDATE` da linha, tomado antes na mesma unidade
de trabalho, e com a guarda, que recusa escrever numa transação final. Valor ausente é sempre `NULL`, nunca string
vazia, porque os CHECKs de formato recusam `''`. Os comandos de `INSERT` e `UPDATE` de transação usam argumentos
nomeados, no modo estrito do pgx, que recusa placeholder sem argumento e argumento sem placeholder em vez de gravar
`NULL`.

**Migrations.** `golang-migrate`, com arquivos `NNNNNN_nome.up.sql`/`.down.sql` embutidos no binário e o comando
`migrate up | down N | version | force V`. No compose roda como serviço one-shot antes das réplicas. Cada arquivo roda
como uma única consulta com vários comandos, que o PostgreSQL executa numa transação implícita: uma migration que falha
não deixa nada aplicado, e o golang-migrate marca a versão como `dirty` até um `force`. `down` exige o número de passos,
para que derrubar o schema seja sempre explícito, e o advisory lock do golang-migrate serializa duas execuções. A
conexão é aberta com o contexto do comando, e o comando inteiro tem 5 min de prazo (o provisionamento, 2 min), para que
um one-shot diante de um banco ou endpoint que não responde falhe em vez de segurar o que depende dele. O teste de
integração aplica tudo, desfaz migração por migração conferindo o que sobra e aplica de novo.

| Migration | Conteúdo |
|---|---|
| 000001 | `wallets` |
| 000002 | `wager_transactions` com todos os CHECKs e índices únicos |
| 000003 | `wallet_ledger_entries` |
| 000004 | `outbox_events`, `inbox_messages` |
| 000005 | guardas: imutabilidade, append-only, encadeamento do ledger, saldo × ledger no commit |
| 000006 | `GRANT`s do papel da aplicação |
| 000007 | consistência ledger ↔ transação, UUID nulo proibido, inbox imutável |
| 000008 | `public.schema_version()`: o serviço confere na partida a versão do schema sem privilégio na tabela de migrations. A função roda com os privilégios do dono (`SECURITY DEFINER`), lê só a versão e a marca `dirty`, só `wallet_app` pode chamá-la, e o `search_path` fixado e os nomes qualificados pelo schema impedem que algo criado numa sessão tome o lugar do que ela lê |
| 000009 | `outbox_events.traceparent`: o contexto W3C da unidade de trabalho que gravou o evento, opcional, conferido no formato do W3C e imutável como o resto do evento (`outbox_guard`); o `CHECK` entra `NOT VALID`, para não varrer uma outbox grande sob o lock exclusivo do `ALTER TABLE` (as linhas que já existem são `NULL`, e as novas são conferidas); os `GRANT`s de tabela da 000006 já o cobrem ([Tracing](#tracing)) |
| 000010 | remove `ix_outbox_due`, que o claim não usa mais (as cabeças saem de `ix_outbox_aggregate`): com estatísticas tiradas quando nada estava pendente, o planner achava o índice vazio, o preferia e lia o backlog inteiro a cada cabeça ([Outbox](#12-outbox-e-eventos)) |

**Papéis.** `wallet_owner` é dono do schema e roda as migrations. `wallet_app` é o papel com que o serviço conecta: tem
`SELECT, INSERT, UPDATE` nas tabelas de estado, só `SELECT, INSERT` no ledger, e nenhum `DELETE` ou `TRUNCATE`. Como não
é dono de nada, não consegue alterar tabela nem desligar trigger. O banco revoga de `PUBLIC` o `CONNECT` no banco e o
`USAGE` no schema `public` e os concede só aos dois papéis (o `USAGE`, só a `wallet_app`, além do dono). A separação é o
que deixa a regra de ledger append-only valer por `REVOKE`, e não só pelo código. Os papéis são criados por
`deploy/postgres/initdb/00-roles.sql`, que exige superusuário. O mesmo script é montado no compose e no testcontainers;
as migrations só fazem `GRANT`.

**Invariantes no banco.** Tudo que é invariante está no schema; a aplicação também valida, mas o banco recusa o estado
inválido mesmo que o código erre. As triggers levantam SQLSTATEs próprios, com o nome da trigger como constraint do
erro: `WL001` (o ledger não explica um saldo ou uma transação) e `WL002` (escrita proibida pelas guardas). O adaptador
troca a mensagem do servidor, que cita saldos e valores, por um texto fixo com o código e a trigger (§15). Toda função
de trigger fixa `search_path` com `pg_temp` por último.

| Invariante | Proteção |
|---|---|
| saldo nunca negativo | `CHECK (balance_minor >= 0)`; `balance_before/after >= 0` no ledger |
| uma carteira por jogador e moeda | `UNIQUE (player_id, currency)` |
| sem lost update | `wallets_guard`: mudança de saldo exige `version = OLD.version + 1`; versão não muda sem saldo mudar |
| identidade da carteira imutável, sem DELETE/TRUNCATE | `wallets_guard`, `wallets_no_truncate` |
| todo saldo tem lançamento | constraint triggers deferidas `wallet_matches_ledger` e `ledger_matches_wallet`: no commit, `balance_minor` = `balance_after_minor` do último lançamento (ou 0) |
| aritmética do lançamento | `CHECK`: `after = before + amount` (CREDIT) ou `after = before − amount` (DEBIT); `amount > 0` |
| ledger encadeado | `ledger_entry_check`: o lançamento continua a carteira (`seq` crescente, `before` = `after` anterior, primeiro parte de 0) e bate com a transação em carteira, valor e moeda |
| ledger append-only | `ledger_no_update`, `ledger_no_truncate` (para todos, inclusive o dono) + `REVOKE` do papel da aplicação |
| um lançamento por transação e carteira | `UNIQUE (wallet_id, transaction_id)` |
| lançamento só de transação `PROCESSED`, na direção certa | `ledger_entry_matches_transaction` (deferida): OPENING/WIN/REFUND → CREDIT, BET → DEBIT, ROLLBACK → o oposto da referência (de BET → CREDIT; de WIN/REFUND → DEBIT), LOSS nunca |
| todo movimento processado tem exatamente um lançamento | `processed_transaction_has_entry` (deferida, em INSERT e em UPDATE de status) |
| idempotência | `UNIQUE (provider_id, idempotency_key)` e `UNIQUE (provider_id, external_transaction_id)`, parciais em `origin = 'EXTERNAL'` |
| crédito inicial único | `UNIQUE (wallet_id) WHERE kind = 'OPENING'` |
| origem interna × externa | `ck_origin_shape`: OPENING é sempre INTERNAL e sem provedor, chaves, hash, rodada, jogo e referência; os demais tipos são EXTERNAL com todos preenchidos |
| política de zero | `ck_zero_policy`: LOSS = 0; demais > 0 |
| uma reversão bem-sucedida por referência | `UNIQUE (reference_transaction_id) WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')` |
| estado terminal imutável | `wager_transactions_guard`: linha final não muda; colunas de negócio nunca mudam; sem volta a PENDING; contadores não diminuem; prazo da espera não se estende; DELETE/TRUNCATE proibidos |
| coerência de estado | CHECKs de código de falha por status, agenda das pendências, `processed_at`, resultado de replay, formatos de ids, chave e hash |
| UUID nulo proibido | `ck_<tabela>_<coluna>_not_nil` em todas as colunas de identidade e referência |
| evento é snapshot | `outbox_guard`: payload, tipo, versão, agregado e ids imutáveis; `published_at` preenchido não volta |
| inbox à prova de adulteração | `inbox_guard`: `consumer_name`, `message_id`, `payload_hash`, `received_at` imutáveis; conclusão gravada uma vez só; DELETE/TRUNCATE proibidos |

O custo das triggers é de buscas por chave primária ou índice dentro da própria transação. É desprezível perto do que
elas garantem.

## 7. Concorrência

**Estratégia: lock pessimista por carteira + versão condicionada + CHECK + retry limitado.**

1. `SELECT … FROM wallets WHERE id = $1 FOR NO KEY UPDATE`: os escritores de uma mesma carteira entram em fila; os de
   carteiras diferentes não disputam nada.
2. `UPDATE wallets SET … WHERE id = $1 AND version = $esperada`: zero linhas afetadas vira `ErrRetryable`. O lock torna
   isso impossível no caminho normal; a condição pega o caminho que esquecesse o lock.
3. `CHECK (balance_minor >= 0)` e as triggers da §6: a última barreira, independente do código.
4. `lock_timeout` de 2s: um escritor não fica preso atrás de uma carteira disputada; faz retry, com jitter, até o
   limite, e então responde 503.

`FOR NO KEY UPDATE` em vez de `FOR UPDATE`: serializa os escritores da carteira do mesmo jeito, mas não conflita com o
`FOR KEY SHARE` que o PostgreSQL toma na carteira quando um `INSERT` em `wager_transactions` ou no ledger verifica a
chave estrangeira. Com `FOR UPDATE`, até inserts que só referenciam a carteira esperariam o escritor. É também o lock
que o `UPDATE` de saldo e a trigger `ledger_entry_check` tomam, então todos os caminhos pedem o mesmo lock. A linha da
transação pendente é travada do mesmo jeito, pela mesma razão (uma reversão que a referencia é inserida sem esperar), e
o claim do resolvedor usa esse lock com `SKIP LOCKED`, aplicado antes do `LIMIT`: as linhas puladas não gastam o lote,
e com `FOR UPDATE` ele também pularia uma transação só referenciada por uma linha sendo inserida.

O otimista puro foi descartado. Numa carteira disputada (um jogador apostando rápido, ou o teste de 50 envios iguais)
geraria uma tempestade de retries justamente quando mais importa.

**Sem deadlock.** Toda decisão sobre uma carteira (saldo, lançamento, status de transação) acontece com o lock dela, e
nenhum caminho trava duas carteiras. Os caminhos que tomam mais de um lock os tomam sempre na mesma ordem: pelo SQS, a
claim da mensagem em `inbox_messages` vem antes do lock da carteira; no resolvedor, o lock da carteira vem antes do da
linha da transação pendente. O claim do resolvedor, que só move o prazo das linhas, e o da outbox não tomam o lock da
carteira e usam `SKIP LOCKED`: não esperam ninguém. Sem ordem invertida, não existe ciclo.

**Sem lock global.** O serviço não toma mutex de processo, lock de tabela nem advisory lock; o único advisory lock é o
que o `golang-migrate` toma no comando `migrate`, para serializar duas aplicações de migrations. A prova de paralelismo
é determinística: uma conexão segura o lock da carteira A, operações na carteira B terminam normalmente enquanto isso, e
a operação em A espera o `lock_timeout` em cada uma das `DB_TX_MAX_ATTEMPTS` tentativas e desiste sem efeito: nenhuma
transação, lançamento ou evento gravado, e a chave continua livre. `TestSubmit_ParallelWallets` (integração) prova isso
no caso de uso; `TestE2E_ParallelWallets/a_held_wallet_delays_only_its_own_bets` prova com três instâncias e os dois
canais: as apostas das outras carteiras, em qualquer instância e pela fila, respondem em menos de 1s; a requisição presa
responde `503 TEMPORARILY_UNAVAILABLE` depois de 3 × 2s; e a mensagem presa volta à fila e é processada quando o lock é
liberado.

**Cenário 2 × 80.00 sobre 100.00 (instâncias e canais diferentes):**

| t | Instância 1 · BET A de 80.00 (HTTP) | Instância 2 · BET B de 80.00 (SQS) |
|---|---|---|
| t0 | `BEGIN`; trava a carteira (100.00, v1) | `BEGIN`; tenta travar a mesma carteira e espera |
| t1 | debita: 100.00 → 20.00, v2; grava transação PROCESSED, lançamento DEBIT e eventos | continua esperando (até 2s) |
| t2 | `COMMIT` → `201`, saldo 20.00 | obtém o lock, lê 20.00, v2 |
| t3 | — | 20.00 < 80.00 → transação REJECTED `INSUFFICIENT_FUNDS` + evento; `COMMIT`; apaga a mensagem |
| fim | saldo 20.00 · 1 PROCESSED · 1 REJECTED · 1 DEBIT · reenvios de A ou B devolvem o mesmo resultado (replay) | |

## 8. Idempotência

As regras (escopo da chave, hash canônico v1 com o vetor de teste, replay com o saldo original, conflitos `409`, a mesma
operação pelos dois canais e o `messageId` da fila) estão em [REGRAS §7](docs/REGRAS_DE_NEGOCIO.md#7-idempotência). Aqui
fica o mecanismo.

**Persistida no banco.** A chave escopada por provedor e o id externo são índices únicos parciais
(`ux_tx_provider_key`, `ux_tx_provider_external`), então a idempotência sobrevive ao reinício de todos os processos e
não depende de cache nem de memória. No HTTP a chave do header é usada exatamente como recebida: o servidor nunca a
substitui por uma calculada. Duas "primeiras" requisições simultâneas são decididas pelo índice: a perdedora recebe
`23505`, classificado como `ErrRetryable` (§5), refaz a unidade de trabalho e encontra a linha vencedora.

**Hash do comando validado.** O hash é calculado do `wager.Command` validado, não do corpo bruto, pela mesma função
canônica que o serviço usa para todo hash (o da inbox incluído). Por isso HTTP e SQS produzem o mesmo hash para o mesmo
comando, e espaços, ordem de chaves e o envelope do transporte não mudam nada. O hash é gravado e comparado em
requisições futuras, então o formato é congelado e versionado (v1): qualquer mudança sai como uma versão nova.

**Replay antes do lock, só no HTTP.** Uma requisição HTTP procura a chave `(provider, chave)` antes de travar a
carteira, numa leitura `REPEATABLE READ READ ONLY` sem lock: uma chave conhecida responde ali mesmo, como replay ou
como `409 IDEMPOTENCY_KEY_REUSED`, sem entrar na fila dos escritores da carteira, o que deixa barato o cenário de 50
envios iguais. Sob o lock, a chave é procurada de novo, para pegar uma primeira requisição concorrente que comitou
nesse intervalo, e o `externalTransactionId` também (`409 DUPLICATE_EXTERNAL_TRANSACTION`). A mensagem do SQS pula a
pré-checagem: responder antes da transação deixaria a mensagem fora da inbox. Ela grava a claim da inbox, trava a
carteira e faz as mesmas conferências sob o lock; uma reentrega da mesma mensagem é reconhecida pela inbox, que
responde com a transação registrada sem esperar o lock da carteira.

**`POST /wallets` não tem `Idempotency-Key`.** A abertura é protegida pela unicidade de `(player_id, currency)`: só
existe uma carteira por jogador e moeda, e uma segunda abertura, inclusive a repetição de uma que já comitou, responde
`409 WALLET_ALREADY_EXISTS`, sem criar outra. Esse `409` diz qual é a carteira: o problem+json leva o id dela no
membro extra `walletId`, e quem perdeu a resposta `201` o recupera na repetição. O `23505` aborta a transação do
PostgreSQL, então o id não pode ser lido na mesma unidade de trabalho; o serviço o busca por `(player_id, currency)`
numa transação nova, somente leitura (`IDByPlayerAndCurrency`), e devolve um `*AlreadyExistsError` que continua
casando com `ErrWalletAlreadyExists`. Se essa busca falha, o chamador recebe a falha (por exemplo, `503`), não um
`409` sem o id. A rota é interna (`wallets:manage`), então o id não chega a nenhum provedor.

## 9. Operações e reversões

Os tipos, a política de valor zero, a ordem das regras, a matriz de referência e a regra de uma reversão bem-sucedida
por referência estão em [REGRAS §4](docs/REGRAS_DE_NEGOCIO.md#4-operações) e
[§5](docs/REGRAS_DE_NEGOCIO.md#5-referências-e-reversões). Aqui fica como elas se mantêm sob concorrência.

A referência é buscada e a pergunta "ela já tem reversão processada?" é feita sob o lock da carteira, que serializa
todos os escritores dela, então nem a referência nem a resposta mudam antes de a decisão ser gravada. A pergunta usa
exatamente o predicado do índice único parcial `ux_tx_single_reversal`, que a responde e garante que exista no máximo
uma. O índice é a rede de segurança: se dois caminhos escapassem, o segundo commit falharia com `23505`, classificado
como corrida (§5); a transação seria refeita e terminaria em `REFERENCE_ALREADY_REVERSED`. Uma reversão processada
precisa gravar a referência resolvida (`reference_transaction_id`), porque é por essa coluna que o índice a conta.

Uma transação nova e uma retomada pelo resolvedor passam pelo mesmo código (`settle`, que chama `wager.Decide`), então
não podem sair diferentes. Uma rejeição de negócio nunca é `error`: ela fica registrada na transação e segue o mesmo
caminho de gravação de um sucesso, com o evento correspondente.

## 10. Referências pendentes

Quando entra em espera, o que acontece a cada busca, os parâmetros (tentativas, backoff com jitter, TTL), a expiração e
o `FAILED` estão em [REGRAS §6](docs/REGRAS_DE_NEGOCIO.md#6-referências-pendentes). Aqui fica como a espera é agendada
e retomada.

**Agenda.** `PENDING_REFERENCE` é a única pendência comitada, com `next_attempt_at` e `expires_at` na própria linha; a
espera não vive na memória de nenhuma réplica. A política de espera é um valor do domínio, e a aleatoriedade entra de
fora, como o relógio: o jitter é sorteado na metade superior do intervalo, arredondando para cima, porque a política
recusa uma espera de zero como decisão que não consegue tomar. A configuração recusa `REFERENCE_BACKOFF_BASE` maior que
`REFERENCE_BACKOFF_MAX`, porque o teto trocaria a base sem aviso, e `Decide` recusa uma política que não consiga agendar
já na primeira transação com referência, chegue ela fora de ordem ou não, para que uma configuração ruim apareça logo. A
próxima busca nunca é marcada depois de `expires_at`: a busca no prazo é a que desiste, então um TTL curto (3s nos
testes) expira no prazo, e não um teto de backoff depois. O cálculo de `base << tentativa` nunca é comparado com o teto
diretamente, porque o deslocamento estoura `int64` em poucas dezenas de tentativas; é o teto que é deslocado no sentido
contrário.

**Retomada durável.** O resolvedor de qualquer réplica reivindica as linhas vencidas numa transação curta, sem lock de
carteira e sem esperar ninguém. O claim é um único comando: um `SELECT id … WHERE status IN ('PENDING',
'PENDING_REFERENCE') AND next_attempt_at <= now() ORDER BY next_attempt_at LIMIT n FOR NO KEY UPDATE SKIP LOCKED`
seguido do `UPDATE … SET next_attempt_at = now() + lease` das linhas escolhidas. O lease é o próprio `next_attempt_at`,
no relógio do banco, e o claim devolve o valor gravado, na mesma precisão, para que a comparação posterior não perca
nada por arredondamento. O claim é comitado antes de qualquer resolução; o failpoint `resolver.after_claim` fica
exatamente ali. O lease não é limitado por `expires_at`: o limite tornaria o claim de uma transação já vencida um lease
de zero, entregando-a a todos os outros resolvedores enquanto ela é processada. O claim também não mexe em `updated_at`,
que é o horário da última transição, no relógio da aplicação, e um claim não é uma transição. O lote vai até 500
linhas (`RESOLVER_BATCH_SIZE`); acima disso uma transação seguraria locks demais por tempo demais.

Cada item é então decidido numa transação própria, sob o lock da carteira e depois o da linha, e só enquanto o lease
ainda é daquele claim: a linha continua pendente e o seu `next_attempt_at` é o que o claim gravou. Se outro resolvedor
a resolveu ou a reivindicou de novo depois que o lease venceu, a resolução sai sem escrever nada. Em seguida vem a
busca da referência e o `Decide`, o mesmo código que decide uma transação nova. Cada resolução tem como prazo o próprio
lease, e o shutdown não a cancela no meio. Se o processo morrer depois do claim, a linha fica com o processo morto até
o lease vencer, e então qualquer instância a retoma (`TestE2E_ResolverKilledAfterClaim`). O resolvedor também aceita
`PENDING` vencido, o que cobre a retomada durável mesmo para um registro que algum dia fosse comitado nesse estado.

Quais falhas contam até `FAILED`, e por que uma linha que nem carrega fica pendente, estão em
[REGRAS §6](docs/REGRAS_DE_NEGOCIO.md#6-referências-pendentes). Do lado do resolvedor: a falha permanente é contada na
própria linha, numa unidade de trabalho separada da que falhou, que confere o lease de novo antes de gravar; a linha que
não carrega falha igual em todo claim e gera um WARN a cada um, espaçado pelo lease; e uma indisponibilidade não conta e
interrompe a passada, para o runner aplicar backoff.

## 11. SQS de entrada

**Filas.** `wager-transactions.fifo` com redrive para `wager-transactions-dlq.fifo` (`maxReceiveCount` 30). Todas as
filas são FIFO, com `ContentBasedDeduplication=false` (dois corpos iguais podem ser operações diferentes),
`VisibilityTimeout` 30s e retenção de 14 dias. Elas são criadas pelo programa `provision`, que só usa chamadas padrão de
SQS e IAM e por isso provisionaria uma conta AWS real do mesmo jeito. Ele é idempotente: filas e usuários são
reaproveitados, atributos e políticas reescritos, e uma chave de acesso só é criada quando o arquivo de credenciais não
tem uma que o IAM ainda aceite. Um segredo só pode ser lido quando a chave é criada, por isso o arquivo é o registro de
quais chaves estão em uso: um arquivo que existe mas não pode ser lido é erro, e não arquivo vazio, que criaria chaves
novas e deixaria os processos com chaves que já não estão nele; e uma chave ausente do arquivo só é apagada quando o
usuário atinge o limite do IAM, a mais antiga primeiro. O arquivo é escrito num temporário ao lado e renomeado, com
permissão 0600, para que ninguém leia um arquivo pela metade e uma execução que falha deixe o anterior intacto.
`CreateQueue` recebe só `FifoQueue`, que não muda depois da criação, e o resto vai por `SetQueueAttributes`, porque
`CreateQueue` recusa uma fila existente com atributos diferentes; cada usuário tem uma única política inline, reescrita
a cada execução. Os 14 dias de retenção são o máximo do SQS, para guardar o mais possível uma mensagem presa.

**Contrato do produtor.**

| Item | Valor |
|---|---|
| corpo | `{"messageId","type":"WagerTransactionRequested","occurredAt","data":{…}}`, estrito: campo desconhecido em qualquer nível, campo ausente, tipo errado ou `null` são recusados; `occurredAt` só como `2006-01-02T15:04:05.000Z` |
| `MessageGroupId` | `walletId`: ordem por carteira, paralelismo entre carteiras |
| `MessageDeduplicationId` | `messageId` do envelope. Atenção: o escopo de deduplicação do SQS é a fila inteira, então os ids precisam ser únicos entre provedores (UUIDs). Dois corpos iguais podem ser operações diferentes, por isso a deduplicação nunca é pelo conteúdo |
| atributo `ProviderToken` (String) | access token do Keycloak do próprio provedor (ver identidade abaixo) |
| `messageId` | identidade durável da mensagem (chave da inbox `(consumerName, messageId)`), `^[\x21-\x7E]{1,255}$`, globalmente único |

O envelope é tão estrito quanto o HTTP, para que o mesmo comando chegue ao caso de uso por qualquer canal: um campo
desconhecido é recusado, e não ignorado, para que um erro de digitação do produtor não descarte um campo em silêncio;
`null` é recusado porque o `encoding/json` o transformaria em string vazia. O `occurredAt` é lido e formatado de novo
para comparação, porque `time.Parse` também aceitaria uma vírgula antes dos milissegundos. Os motivos de recusa nomeiam
a regra e o campo, nunca o valor, e uma chave desconhecida é citada entre aspas e cortada em 64 bytes, porque é texto do
produtor que vai para o log e para o atributo da DLQ; as chaves são conferidas em ordem, então uma mensagem com vários
problemas sempre relata o mesmo.

**Identidade do produtor.** Passar pela fila não torna a mensagem confiável. O consumidor lê o atributo `ProviderToken`
e o valida com o mesmo verificador do HTTP (assinatura, `iss`, `aud`), julgando a expiração no `SentTimestamp` da
mensagem e não no horário do processamento. `SentTimestamp` é carimbado pelo broker, não pelo produtor; uma mensagem
pode esperar na fila mais do que um token vive, e o que importa é que o provedor tinha um token válido quando enviou. O
principal precisa ser um provedor com o escopo `wagering:write` (a mesma regra do POST HTTP), e o `data.providerId`
precisa ser igual ao `provider_id` do token.

A identidade é conferida antes de qualquer leitura do corpo, como o HTTP autentica antes de decodificar: uma mensagem
que ninguém assina é recusada como tal, seja o que for que ela carregue. Um `data.providerId` bem formado que não é o do
token é recusado antes de qualquer outro problema do comando, porque é sinal de personificação; um ausente ou malformado
é só uma mensagem quebrada. A classificação de cada falha de identidade, permanente (DLQ na hora) ou transitória
(retry), está em [REGRAS §10](docs/REGRAS_DE_NEGOCIO.md#10-mensageria). Um veredito sobre o token dado depois de o prazo
da mensagem acabar não é confiável, porque a busca de chaves pode ter sido cortada, e vira retry, que o shutdown
transforma em devolução.

Transitória não quer dizer para sempre: o limite de tentativas e a única exceção a ele, a queda do banco, estão em
[REGRAS §10](docs/REGRAS_DE_NEGOCIO.md#10-mensageria). Um JWKS fora do ar por mais tempo que esse limite leva mensagens
válidas para a DLQ; elas não se perdem, e o provedor as reprocessa reenviando-as com token novo (ver §18).

O token com `kid` desconhecido é o único veredito de token que a fila trata como retry. No HTTP ele é um `401`; na fila,
só quem tem as credenciais AWS de um provedor consegue enviar, e uma rotação de chaves em andamento é bem mais provável
que uma falsificação, então a mensagem espera a próxima busca do JWKS, dentro das `CONSUMER_MAX_RECEIVES` tentativas.
Uma assinatura que não confere sob um `kid` já publicado continua indo direto para a DLQ.

Por que não o `SenderId`: na AWS real, o `SenderId` de uma mensagem é o `UserId` do usuário IAM que a enviou, e seria a
identidade natural. O MiniStack devolve o id da conta (`000000000000`) para todo remetente, então ele não distingue
provedores. Os usuários IAM por provedor continuam existindo e limitam quem pode enviar (ver §13), mas a identidade
usada para autorizar a operação é o token.

**Consumo.**

| Parâmetro | Valor | Por quê |
|---|---|---|
| polling | long polling de 20s (`CONSUMER_WAIT_TIME`), até 10 mensagens, só quando há vaga para mais um grupo | menos chamadas e latência baixa; um backlog espera na fila, onde qualquer réplica o pega, e não na memória de uma |
| paralelismo | grupos em paralelo (até 8, `CONSUMER_MAX_GROUPS`); mensagens do mesmo grupo em série; se uma falha, o resto do grupo é devolvido | preserva a ordem FIFO dentro da carteira |
| visibilidade | 30s; prazo por mensagem de 15s (`CONSUMER_PROCESSING_TIMEOUT`, sempre menor que a visibilidade e maior que os 5s que a busca do JWKS pode levar); um extensor renova, a cada terço da visibilidade, a de toda mensagem ainda em mãos, em lotes de 10 (`ChangeMessageVisibilityBatch`), e larga a mensagem que o SQS diz não estar mais em voo | um receive traz várias mensagens do mesmo grupo, processadas em série; sem o extensor, as últimas reapareceriam para outra réplica. Se o processo morre, o extensor morre junto e a mensagem volta em até 30s |
| backoff, `RETRIES_EXHAUSTED` e redrive | os valores estão em [REGRAS §10](docs/REGRAS_DE_NEGOCIO.md#10-mensageria); o backoff é um `ChangeMessageVisibility` calculado do `ApproximateReceiveCount`, e o `maxReceiveCount` 30 do redrive fica acima do `CONSUMER_MAX_RECEIVES` padrão, 10 | quem desiste é o consumidor, que grava o motivo explícito na DLQ; o redrive é só a rede de segurança, alto porque o FIFO entrega o grupo junto e as mensagens atrás de uma cabeça que falha herdam o receive count dela |
| circuit breaker | uma falha com o banco indisponível dispara um ping, e só um ping que também falha pausa o polling (o comportamento está em [REGRAS §10](docs/REGRAS_DE_NEGOCIO.md#10-mensageria)); a readiness, que pinga o banco por conta própria, já responde 503 nesse meio-tempo | não gasta receive count de mensagens válidas durante uma queda; o timeout de uma mensagem lenta ou o lock disputado de uma carteira não param as outras carteiras |

O que acontece com cada resultado (apagar, DLQ na hora com qual motivo, retry, devolução) está em
[REGRAS §10](docs/REGRAS_DE_NEGOCIO.md#10-mensageria). Os erros que o consumidor manda para a DLQ na hora são uma lista
fechada de vereditos que a reentrega repetiria; todo o resto é tratado como transitório, então um erro que ninguém
classificou custa no máximo as cerca de 15 min de tentativas, e uma falha passageira nunca vai para a DLQ na hora. Um
`panic` no processamento de uma mensagem é recuperado como retry, com a pilha no log, para que uma mensagem ruim não
derrube o processo nem se perca. Cada receive leva um `ReceiveRequestAttemptId` próprio: quando o SDK repete um receive
cuja resposta se perdeu, o SQS devolve as mesmas mensagens em vez de mantê-las, e os grupos inteiros delas, ocultas até
a visibilidade vencer. O extensor renova a um terço da visibilidade para sobrar dois terços para uma renovação lenta, ou
que falha uma vez e passa no tique seguinte. Receives que falham seguidos são espaçados de 1s até 30s, para não martelar
o broker numa queda e ainda perceber logo a volta. As chamadas que encerram uma mensagem (apagar, mover, devolver,
renovar) saem mesmo depois de o processamento ser cancelado, para que uma mensagem processada logo antes do shutdown
ainda seja apagada, e cada uma tem 5s de prazo (`config.SQSCallTimeout`).

**Inbox na mesma transação.** A inbox (`consumer_name`, `message_id`, `payload_hash`, recebimento e conclusão) é gravada
na mesma transação SQL das mudanças de domínio, do ledger e da outbox, então "marquei como lida mas não debitei" não
existe. A claim são dois comandos de propósito: quando outra unidade de trabalho insere a mesma mensagem, o `INSERT`
espera ela terminar e insere (se ela desfez) ou não faz nada (se comitou), e o `SELECT` seguinte, por ser outro comando
sob `READ COMMITTED`, enxerga o que ela comitou; dentro do mesmo comando, mesmo numa CTE, a linha continuaria invisível.
A conclusão grava processamento, transação e resultado num único comando, porque depois dele `inbox_guard` congela os
três juntos. O que entra no hash da inbox está em [REGRAS §7](docs/REGRAS_DE_NEGOCIO.md#7-idempotência). A mensagem só é
apagada depois do commit; um crash entre o commit e o delete faz a mensagem voltar, e ela é reconhecida como duplicata.
Um delete que falha tem o mesmo efeito, e `sqs_messages_total` o conta como `retry`: `processed` e `duplicate` só contam
depois que o delete deu certo, e a mensagem que volta conta `duplicate`.

**DLQ "na hora".** O que a cópia leva, e por que nunca o `ProviderToken`, está em
[REGRAS §10](docs/REGRAS_DE_NEGOCIO.md#10-mensageria). A mudança é um `SendMessage` na DLQ, com o `MessageId` original
como `MessageDeduplicationId`, seguido do `DeleteMessage` na origem. Se o envio falha, nada é apagado. Se o processo cai
entre os dois passos, a mensagem volta e é movida de novo; a cópia duplicada na DLQ é deduplicada pelo id ou, depois de
5 minutos, é inofensiva. Os motivos são sanitizados porque podem carregar texto do produtor, e um valor que o SQS
recusasse travaria o grupo.

**Concorrência HTTP × SQS.** Os dois canais chamam o mesmo caso de uso e disputam a mesma linha de carteira; o lock, a
idempotência e as triggers não sabem de onde a operação veio. `TestE2E_TwoBetsOf80_HTTPandSQS` roda o cenário 2 ×
80.00 (§7) com uma aposta pela API de uma instância e a outra pela fila, no consumidor de outra, as duas esperando a
carteira ao mesmo tempo; reenviar cada uma pelo outro canal só a reproduz. `TestE2E_CrossChannelSameOperation` envia
a mesma operação pelos dois canais, nas duas ordens, e a segunda chegada é replay da primeira, qualquer que seja o
canal e a instância.

## 12. Outbox e eventos

**Gravação.** Os eventos são gravados em `outbox_events` na mesma transação da mudança que os originou, já como snapshot
JSON pronto, numa coluna `JSONB` que `outbox_guard` torna imutável. Um evento nunca é publicado antes do commit: um
rollback leva o evento junto. O corpo publicado é o JSONB como o PostgreSQL o devolve: os mesmos campos e valores do
construtor, com as chaves em outra ordem e espaços depois de `:` e `,`. O consumidor lê JSON, nunca compara bytes, e a
identidade do evento é o `eventId`. O snapshot é tirado no momento da mudança, e não quando o relay publica, e é isso
que torna o evento imutável: o que o consumidor lê é o que a mudança foi, qualquer que seja o código ou o schema que
viria depois. Os eventos de uma unidade de trabalho vão num único lote de `INSERT`s, uma ida e volta ao banco, o que
importa porque ela costuma estar segurando o lock de uma carteira; os comandos rodam em ordem, então o `seq` segue a
ordem das decisões. É o lock da carteira que garante a ordem: um evento anexado depois mas comitado antes seria
publicado primeiro. Apagar eventos antigos (retenção) fica com o dono do schema; o papel do serviço não tem `DELETE`.
Com tracing ligado, a linha guarda também o `traceparent` da unidade de trabalho que a gravou, para o relay publicar o
evento no mesmo trace da mudança ([Tracing](#tracing)).

**Relay** (`OUTBOX_RELAY_ENABLED`):

1. Claim da cabeça de cada agregado. A cabeça é o pendente de menor `seq` do `aggregate_id`, qualquer que seja o lease
   ou o prazo dele, então um lote nunca leva dois eventos da mesma carteira. As cabeças saem de uma CTE recursiva que
   salta por `ix_outbox_aggregate` de um agregado ao seguinte (um *loose index scan*), e não de uma varredura dos
   pendentes em ordem de `seq` com uma sonda por linha: o custo acompanha o número de carteiras com evento pendente, e
   não o backlog (com 50 mil pendentes, o claim caiu de 90 ms–1,5 s para 1–3 ms, medido pelo
   `BenchmarkOutbox_ClaimHeads`: 50 mil eventos já publicados e 50 mil pendentes, lote de 500, com 5 e com 200 carteiras
   e as estatísticas da tabela colhidas antes e depois do backlog; precisa de Docker e roda com
   `go test -tags=integration -run '^$' -bench ClaimHeads ./internal/adapter/postgres/`). Em ordem de `seq`, cada cabeça
   é travada com `FOR UPDATE SKIP LOCKED` se ainda estiver pendente, vencida (`next_attempt_at <= now()`) e sem lease,
   até completar `n`; o lock reconfere isso na versão mais nova da linha, então uma cabeça que outro relay publicou ou
   reservou no meio do caminho é pulada, nunca trocada pela seguinte. As cabeças são ordenadas antes de travar, para que
   só as que entram no lote fiquem travadas. Depois o claim marca `locked_by`, `locked_until = now() + lease` (30s) e
   `attempts + 1`. Vários publishers dividem a fila sem se bloquear. O dono é o id da instância com um sufixo aleatório
   sorteado na partida, para que um processo novo com o mesmo hostname não confirme o que o anterior reservou. A seleção
   é uma CTE `MATERIALIZED`, que roda uma vez, inteira, antes do `UPDATE`: dobrada dentro dele, a subconsulta que trava
   poderia rodar de novo no join e travar mais que `n` linhas; e o `SKIP LOCKED` vem antes do `LIMIT`, então uma linha
   ocupada não gasta o lote. O lote vai até 500 (`OUTBOX_BATCH_SIZE`), porque acima disso uma transação seguraria locks
   demais por tempo demais.
2. Publica fora da transação, com `SendMessageBatch` (10 por chamada). Publicar e confirmar têm, cada um, metade do
   lease como prazo, para que o lease não vença com o lote ainda nas mãos; um lote reservado é publicado e confirmado
   mesmo que o shutdown comece no meio, para não deixar publicado sem confirmar o que poderia ser confirmado.
3. Ack com fencing: `UPDATE … SET published_at = now() WHERE id = $1 AND locked_by = $me AND published_at IS NULL`. Um
   dono antigo que acorda depois que o lease venceu não sobrescreve nada.
4. Falha: `next_attempt_at = now() + min(2^attempts s, 5 min)` com equal jitter (entre a metade e o valor cheio, para
   espalhar os eventos que falharam juntos sem adiantar demais nenhum retry), `last_error`, e o lock é liberado.
   `last_error` guarda até 1000 caracteres de UTF-8 válido sem `NUL`: o bastante para diagnosticar, e sem deixar que um
   erro que arrasta um corpo de resposta inteiro inche uma linha em retry por horas, nem que a própria falha deixe de
   ser gravada (e o evento espere o lease em vez do backoff). **Sem limite de tentativas**: um evento nunca é
   descartado; a métrica de idade do pendente mais antigo é o alarme.

Janelas de falha: um crash entre o commit e a publicação deixa o evento pendente, e outra instância o publica. Um crash
entre o claim e a publicação (failpoint `outbox.after_claim`) deixa o lote reservado ao processo morto até o lease
vencer, e outra instância o publica. Um crash entre a publicação e o ack (`outbox.after_publish_before_ack`) faz o
lease vencer, e outra instância republica **com o mesmo `eventId`**; o broker descarta o reenvio pelo
`MessageDeduplicationId` dentro de 5 minutos, e depois disso os consumidores deduplicam por `eventId`. Os dois crashes
são os cenários `TestE2E_OutboxCrashAfterClaim` e `TestE2E_OutboxCrashAfterPublishBeforeAck`, com dois relays na
mesma outbox. Um ack que chega depois que outro relay assumiu o evento não grava nada e é contado como `lease_lost` em
`outbox_publish_total`. No shutdown, depois que o laço parou, o relay solta os leases que ainda tem, para outro relay
assumir os eventos na hora em vez de esperar o lease. A idade do pendente mais antigo e o número de pendentes são
medidos a cada 5s, por um laço próprio, com `clock_timestamp()` e não `now()`: `now()` é o início da transação, e um
evento comitado no meio dela ficaria com idade negativa.

O publicador usa o `eventId` como id de cada entrada do lote, então a resposta do SQS volta aos registros sem depender
da ordem; uma entrada sobre a qual a resposta não diz nada conta como falha, porque só o SQS sabe se ela saiu. Uma
chamada que falha falha o lote inteiro, e uma entrada recusada falha só o seu registro. Cada lote que não sai inteiro
vira um único WARN, com quantos eventos tinha e por quê: numa queda, uma linha por evento enterraria a causa. Um
atributo vazio nunca é enviado, porque o SQS o recusaria sempre e seguraria o resto da carteira para sempre.

**Contrato de roteamento e consumo.**

| Item | Valor |
|---|---|
| destino | `wallet-events.fifo` (DLQ `wallet-events-dlq.fifo`) |
| `MessageGroupId` | `aggregateId` = `walletId` nos quatro eventos (na outbox, `aggregate_type` é sempre `Wallet`) |
| `MessageDeduplicationId` | `eventId` |
| atributos | `eventType`, `eventVersion`, `correlationId` (roteamento sem abrir o corpo); com tracing ligado, `traceparent` |

A entrega, a ordem por carteira, a deduplicação e a evolução do contrato estão em
[REGRAS §11](docs/REGRAS_DE_NEGOCIO.md#11-eventos).

A carteira é o agregado: a transação vive dentro da fronteira de consistência da carteira e só é escrita com o lock
dela. O `transactionId` vai em `data`.

Os quatro eventos, quando cada um sai e os campos de `data` estão em
[REGRAS §11](docs/REGRAS_DE_NEGOCIO.md#11-eventos). Um exemplo:

```json
{
  "eventId": "0192f2a0-1b7e-7c11-9a3e-2f0c1d4e5a6b",
  "eventType": "WalletBalanceChanged",
  "aggregateId": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "correlationId": "c-0192f298-345e",
  "occurredAt": "2026-09-08T12:00:00.123Z",
  "version": 1,
  "data": {
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "transactionId": "0192f298-345e-7e38-af88-e43f851a819d",
    "direction": "DEBIT",
    "money": { "amount": "25.00", "currency": "BRL" },
    "balanceBefore": { "amount": "1000.00", "currency": "BRL" },
    "balanceAfter": { "amount": "975.00", "currency": "BRL" },
    "walletVersion": 2
  }
}
```

Cada evento tem um tipo concreto em Go e um único construtor, que fixa `eventType` e `version` e valida os metadados. O
formato de `occurredAt` e do dinheiro está em [REGRAS §11](docs/REGRAS_DE_NEGOCIO.md#11-eventos). O truncamento dos
milissegundos, e nunca o arredondamento, é para que um horário nunca pareça posterior ao instante de origem, e as três
casas fixas deixam os consumidores comparar e ordenar horários como texto; o horário zero é recusado em vez de publicado
como `0001-01-01T00:00:00.000Z`. Um campo monetário nunca leva `omitempty`. Os ids, tipos e direções do evento são
strings na grafia do contrato, não os tipos internos do domínio, para que uma refatoração interna não mude o que é
publicado; os ids do envelope precisam estar na grafia canônica, porque são o texto pelo qual os consumidores deduplicam
e ordenam (duas grafias da mesma carteira seriam dois agregados cujos eventos poderiam se ultrapassar). De onde vêm
`correlationId` e `causationId` está em [REGRAS §11](docs/REGRAS_DE_NEGOCIO.md#11-eventos): no HTTP e na abertura de
carteira não há causa além da requisição, e `causationId` é omitido, como no exemplo acima, que é o formato de um
`WalletBalanceChanged` de uma aposta por HTTP (ids ilustrativos, chaves na ordem do construtor).

## 13. Autenticação e autorização

**IdP.** Keycloak 26 com o realm `wagering` importado no boot (`deploy/keycloak/realm-wagering.json`). Todos os clients
são confidenciais e só têm `serviceAccountsEnabled`, sem fluxos interativos (`standardFlowEnabled`,
`implicitFlowEnabled` e `directAccessGrantsEnabled` desligados). A comunicação entre serviços usa `client_credentials`.
O serviço não cadastra senha nem emite token.

| Client | Escopos | Claims |
|---|---|---|
| `provider-a` | `wagering:write`, `wagering:read` | `provider_id=provider-a`, `aud=wagering-api` |
| `provider-b` | `wagering:write`, `wagering:read` | `provider_id=provider-b`, `aud=wagering-api` |
| `wallet-service` | `wallets:manage` | sem `provider_id`, `aud=wagering-api` |
| `provider-a-short` | `wagering:write`, `wagering:read` | `provider_id=provider-a`, `aud=wagering-api`; token de 2s, para o teste de expiração |

Os segredos são de desenvolvimento e estão versionados no realm e, para os alvos `make token-*`, no `Makefile`; o
`.env.example` não tem nenhum, porque o serviço só valida tokens e não precisa de segredo de client.

**Por que Keycloak.** O enunciado pede um IdP externo OAuth 2.0/OIDC e recomenda o Keycloak, que cobre o que o serviço
precisa:

- padrão aberto: tokens JWT assinados com RS256 e chaves publicadas num JWKS, validados com bibliotecas genéricas de
  JWT, sem SDK do fornecedor. Outro IdP OIDC que emita as mesmas claims entra por configuração (`OIDC_ISSUER`,
  `OIDC_JWKS_URL`, `OIDC_AUDIENCE`), não por código;
- `client_credentials` nativo: provedores e serviço interno são sistemas, não pessoas. Cada um é um client confidencial
  com o próprio segredo, e o serviço não guarda usuário nem senha;
- local e reprodutível: a imagem oficial sobe no compose e no testcontainers sem conta em nuvem, e os testes de
  integração falam com o mesmo IdP real;
- realm como código: clients, escopos, mappers de claim e audiência estão em `deploy/keycloak/realm-wagering.json`,
  importado no boot pelo compose e pelos testes, então o modelo de permissões é revisado no diff como o resto.

**Por que escopos e a claim `provider_id`.** São duas perguntas diferentes, respondidas por duas claims que o IdP
assina:

- o escopo diz o que o token pode fazer: `wagering:write` para enviar, `wagering:read` para consultar,
  `wallets:manage` para as carteiras. Cada rota protegida exige o seu, e quem decide quem recebe qual é o IdP: um
  provedor só de leitura seria um client sem `wagering:write`, sem mudar o serviço;
- `provider_id` diz em nome de quem o token age. A identidade do provedor fica presa ao token por um mapper de valor
  fixo no client, e o serviço nunca a tira do corpo, do path ou da mensagem: esses valores só são conferidos contra a
  claim (`403 PROVIDER_MISMATCH` no HTTP, DLQ `PROVIDER_IDENTITY_MISMATCH` na fila), e consultas, chaves de
  idempotência e referências são escopadas por ela;
- a claim é separada do client id de propósito: um provedor pode ter mais de um client (o `provider-a-short` age como
  `provider-a`), e trocar ou girar um client não muda a identidade de negócio.

**Alternativas descartadas.**

| Alternativa | Por que não |
|---|---|
| chaves de API estáticas | segredo de vida longa em toda requisição, sem expiração nem escopo padronizado; o serviço passaria a guardar e girar segredos, e o enunciado pede um IdP OAuth 2.0/OIDC |
| mTLS | autentica a conexão, não a operação: pediria uma CA e um certificado por provedor, com escopos e `provider_id` em campos do certificado; com o nginx na frente, a identidade chegaria às réplicas num header em que elas teriam de confiar, e não alcança a mensagem que passa pelo SQS |
| Ory Hydra ou Dex | o Hydra também faz `client_credentials`, mas os clients são criados por API depois da subida e uma claim própria como `provider_id` pede um webhook; o Dex é feito para login de pessoas por conectores. Nenhum é a recomendação do enunciado |
| JWT HS256 com segredo compartilhado | quem valida também consegue assinar: qualquer réplica, ou quem lesse a configuração dela, emitiria token de qualquer provedor; e sem JWKS girar a chave exige trocar o segredo em todo lugar ao mesmo tempo |
| introspecção do token (RFC 7662) a cada requisição | põe o IdP no caminho de toda requisição e mensagem; a validação local só depende dele para buscar as chaves, e o preço, um token revogado continuar valendo até expirar (5 min), é aceito |

**Validação do token** (`internal/adapter/oidc`), local, sem chamar o IdP a cada requisição:

- `golang-jwt/jwt/v5` + `MicahParks/keyfunc/v3`. O JWKS é baixado na partida (se não estiver acessível, a partida falha)
  e atualizado periodicamente, a cada 5 min, o que também faz uma chave revogada deixar de valer em minutos. Um `kid`
  desconhecido faz o verificador buscar o JWKS de novo, no máximo a cada 30s por instância: uma rotação precisa de uma
  busca só, mas qualquer um pode mandar um `kid` inventado, e o limite impede que isso vire carga no IdP. Cada busca tem
  5s no máximo (`config.KeySetFetchTimeout`), então `HTTP_REQUEST_TIMEOUT`, com a API ligada, e
  `CONSUMER_PROCESSING_TIMEOUT`, com o consumidor ligado, precisam ser maiores que isso, ou a busca cortada pelo prazo
  da requisição ou da mensagem seria tomada por uma queda do IdP. Gasto o limite, o token com `kid` desconhecido recebe
  `401` na hora, sem esperar a próxima busca; se a última busca falhou, o conjunto de chaves é tratado como fora do ar e
  a resposta é `503`. Os tempos são constantes do pacote, não variáveis de ambiente: seguem o ritmo com que um IdP gira
  chaves, não o deploy.
- "Chave não encontrada" só é veredito quando o conjunto de chaves está completo. Não está enquanto a última busca
  falhou, enquanto o IdP respondeu sem nenhuma chave utilizável, nem quando o contexto acabou durante a busca: nesses
  casos o erro não carrega o sentinela de token inválido, e o chamador repete em vez de responder `401` ou mandar a
  mensagem para a DLQ. Um cliente que desistiu (contexto cancelado) não marca o JWKS como fora do ar, senão qualquer um
  derrubaria um conjunto saudável. Um token sem `kid` é conferido contra as chaves em cache, sem buscar nada, então só
  um `kid` gasta o limite de buscas. Um token expirado é um erro à parte, porque o cliente só precisa pedir outro.
- Algoritmo fixado em `RS256`, o que barra `alg: none` e HMAC assinado com a chave pública. `iss` = valor configurado,
  `aud` contém `wagering-api` (aceita string ou array), `exp` obrigatório, `nbf` respeitado, tolerância de relógio
  `OIDC_CLOCK_SKEW` (30s por padrão), `sub` obrigatório.
- O principal sai só de `scope` (separado por espaço), que vira a lista de escopos, e de `provider_id`, que identifica
  o provedor. `iss`, `aud`, `exp`, `nbf` e `sub` só são conferidos; `azp` não é lido.
- Um token que seria provedor **e** interno (tem `provider_id` e `wallets:manage`), ou nenhum dos dois, é recusado como
  configuração inválida do IdP.

`coreos/go-oidc` foi descartado: foi feito para ID token, não tem tolerância configurável para `exp` e confere a
audiência pelo client id.

**Respostas.** O `401` de um token recusado e o `503` de um verificador que não consegue julgar estão em
[REGRAS §9](docs/REGRAS_DE_NEGOCIO.md#9-autorização). O log registra só a classe da falha (missing/invalid/expired em
DEBUG, misconfigured em WARN), nunca o token.

**Permissões.** A matriz de quem pode o quê, por rota, e as regras de isolamento entre provedores estão em
[REGRAS §9](docs/REGRAS_DE_NEGOCIO.md#9-autorização). O mecanismo: a autenticação é aplicada por rota, e não num
middleware global, porque o health é público. Cada rota declara quem passa (só o serviço interno; só um provedor com o
escopo; ou os dois, para leitura), e o handler confere a posse, porque só ele sabe de quem são os dados que a
requisição nomeia. O `providerId` autorizado vem sempre da identidade, e corpo ou path com outro provedor é recusado
antes de qualquer leitura ou escrita. Um client ligado a um provedor nunca é interno, mesmo que receba `wallets:manage`
por engano, então um provedor não administra carteiras por erro de configuração. A transação de outro provedor responde
**404**, e não 403, para não revelar que o id existe; só o log distingue os dois casos.

**Mensageria.** O MiniStack roda com `AUTH=true`: assina e confere as requisições feitas com chaves de usuário IAM e
avalia as políticas. Há um usuário por papel, com o mínimo de privilégio:

| Principal | Permissões |
|---|---|
| `provider-a-sqs`, `provider-b-sqs` | `SendMessage` e `GetQueueUrl` na fila de entrada |
| `wallet-app` | `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility` (também em lote, para o extensor de visibilidade), `GetQueueAttributes`, `GetQueueUrl` na entrada; `SendMessage`, `GetQueueAttributes` e `GetQueueUrl` na DLQ de entrada e em `wallet-events.fifo`, onde também `SendMessageBatch`, para o relay |
| `events-reader` | consumo de `wallet-events.fifo` |
| root `test`/`test` | só o provisionamento e, à mão, a leitura das DLQs, que nenhum usuário IAM lê |

O teste de integração prova que o provedor não consegue receber (`AccessDenied…`, HTTP 403) e que uma chave falsa é
recusada (`InvalidClientTokenId`). As credenciais geradas vão para um arquivo AWS padrão (perfis `[wallet-app]`,
`[provider-a]`…) num volume compartilhado. O arquivo tem permissão 0600 e o dono é o mesmo usuário não root com que os
containers da aplicação rodam. A aplicação usa `AWS_SHARED_CREDENTIALS_FILE` + `AWS_PROFILE`, o mecanismo padrão do SDK,
e resolve as URLs das filas com `GetQueueUrl` (nunca as copia de configuração, porque a URL depende do host pelo qual o
broker é acessado); uma fila ausente falha a partida, nomeando todas as que faltam. Nada da AWS é fixado no código,
então o mesmo binário fala com o MiniStack e com a AWS só pelo ambiente. A política do `wallet-app` nomeia também as
ações de lote: a AWS autoriza um lote pela ação da chamada simples e não conhece ação de lote, mas o MiniStack confere
cada uma pelo próprio nome, e sem elas nenhuma das duas funcionaria localmente.

**Realm de desenvolvimento.** O realm declara `"sslRequired": "none"`. Ele só existe para o ambiente local e os testes,
que falam HTTP; em produção haveria TLS e um realm próprio. O padrão do Keycloak, `"external"`, responde `403` a HTTP
puro vindo de qualquer endereço fora das faixas privadas, que é como o Docker pode apresentar o host a um container. No
compose, `KC_HOSTNAME=http://localhost:8080` fixa o issuer, para que tokens obtidos de dentro e de fora do Docker tenham
o mesmo `iss`. A API valida `iss` contra esse valor e busca o JWKS pela URL interna.

## 14. Fx, ciclo de vida e shutdown

**Composição.** `cmd/wallet` lê e valida a configuração e roda `fx.New(bootstrap.Module(cfg)).Run()`. O grafo é
organizado em `fx.Module` por área, na ordem em que os hooks sobem: `platform` (logger, tracer, métricas e servidor
admin, readiness, relógio, ids), `postgres` (pool e unidade de trabalho), `aws` (cliente SQS e URLs das filas), `auth`
(verificador OIDC), `app` (casos de uso), `http` (a API) e `workers` (consumidor, relay da outbox com a medição do
atraso, e resolvedor). A injeção é por construtor (`fx.Provide`), e `fx.Invoke` só registra hooks. Os construtores são
puros, sem I/O: todo I/O acontece em hooks `OnStart`/`OnStop`. O pool do banco só nasce no hook de partida dele, porque
o `pgxpool` roda uma goroutine desde a criação: um pool montado com o grafo estaria rodando mesmo que um componente
anterior falhasse ao subir, e ninguém o fecharia. Pelo mesmo motivo, o cliente SQS e as URLs das filas só existem depois
do hook `sqs`, porque carregar a configuração da AWS pode ler arquivos de credenciais e resolver uma URL é uma chamada
ao SQS. Os workers, por isso, são montados dentro do hook de partida do módulo `workers`, e não por `fx.Provide`: eles
precisam do cliente SQS e das URLs das filas, que só existem depois que o hook `sqs` rodou. Componente desligado por
configuração não registra hook, e sem a API não há dreno. `TestModule_Validates` roda `fx.ValidateApp` para cada
combinação de componentes ligados, sem Docker; na integração, `TestApp_StartStop_NoLeaks` sobe e para o processo inteiro
com `fxtest` e confere com `goleak` que nenhuma goroutine ficou, e `TestApp_StopOrder` confere a ordem de parada.

**Partida com falha rápida.** A configuração inteira é validada de uma vez, com todos os problemas listados numa única
mensagem. Na partida: ping no banco, resolução das URLs das filas (fila ausente falha a partida), download do JWKS
(falha se inacessível), `net.Listen` síncrono do HTTP e do servidor admin (porta ocupada falha a partida) e conferência
da versão do schema (§18). É melhor não subir do que subir quebrado e aceitar tráfego. Cada dependência falha rápido por
conta própria; um prazo de 30s para a partida inteira só impede que uma travada segure o processo para sempre. Uma
partida que falha desfaz o que já tinha subido, na ordem inversa, e não deixa viva nenhuma goroutine do serviço, nem a
do componente que falhou, que o Fx não para. Só as conexões ociosas que o SDK da AWS guarda para reuso duram até o fim
do seu prazo de ociosidade, 90s.

**Falha em execução.** Se a API ou o servidor admin param de servir sozinhos (o listener falhou), o erro é logado uma
vez e o processo entra em shutdown com código de saída 1, parando o resto na ordem de sempre: um processo sem API ou
sem métricas não pode passar por saudável.

**Ordem de parada.** O Fx para na ordem inversa da partida: pool, cliente SQS e verificador OIDC são construídos antes
dos componentes que dependem deles e fecham por último.

1. SIGTERM → a readiness passa a responder 503 e o serviço espera `SHUTDOWN_DRAIN_DELAY` (2s), tempo para balanceadores
   tirarem a réplica de rotação. Com a API desligada não há dreno: é ela que serve a readiness.
2. Workers, todos em paralelo num único hook: consumidor, relay da outbox, a medição do atraso da outbox e resolvedor,
   cada um dentro do seu orçamento. Resolvedor e relay: o contexto do laço é cancelado, o item em andamento termina
   (é sempre uma transação curta) e o `Stop` espera o término real. Um laço que não termina no prazo faz o `Stop`
   devolver erro (término observável), e ele é logado. Terminado o laço, o relay solta os leases que ainda tem, para
   outro relay assumir os eventos na hora, com 2s só para isso: se esgotar, vira um aviso e não falha o shutdown, porque
   os leases expiram sozinhos. Consumidor: para o polling, drena por até `CONSUMER_DRAIN_TIMEOUT` (20s), cancela o que
   ainda estiver em processamento e devolve a visibilidade do que sobrou e do que cancelou, com uma chamada ao SQS de
   até 5s cada.
3. HTTP: `Shutdown` recusa conexões novas e espera as requisições em voo por até `HTTP_SHUTDOWN_TIMEOUT` (10s); o que
   passar disso é cortado. Toda requisição em voo chegou antes do `Shutdown`, e o trabalho de cada uma termina em
   `HTTP_REQUEST_TIMEOUT` (10s nos padrões, o mesmo que `HTTP_SHUTDOWN_TIMEOUT`), então ela recebe resposta antes de o
   HTTP parar, um `503` com `Retry-After` no pior caso (banco que não responde), e não um corte de conexão. Só uma cujo
   rollback ainda espere, por até 5s, um banco que não responde pode passar do prazo e ser cortada.
4. Verificador OIDC e cliente SQS liberam seus recursos; o pool fecha depois deles; depois o servidor admin, para que
   as métricas fiquem disponíveis até o fim; e, com tracing ligado, por último o tracer, que exporta os spans ainda na
   fila, inclusive os do próprio shutdown.

A ordem real, tirada do log de uma instância parada com `SIGTERM` (a instância do host do README): `drain`;
`resolver`, `outbox-stats`, `outbox-relay` e `consumer`, juntos; `workers`; `http`; `verifier`; `sqs`; `postgres`;
`admin`. Com nada em andamento, a parada levou os 2s do dreno e poucos milissegundos para o resto.

**Prazo do shutdown.** O Fx roda os hooks de parada um depois do outro, então o prazo do shutdown inteiro,
`SHUTDOWN_TIMEOUT` (60s, passado a `fx.StopTimeout`), precisa cobrir a soma dos passos: dreno + o maior orçamento entre
os workers ligados + `HTTP_SHUTDOWN_TIMEOUT` + 5s de folga para o que fecha na hora. O orçamento de cada worker é o pior
caso do seu `Stop`: `CONSUMER_DRAIN_TIMEOUT` mais 5s para devolver o que o dreno cancelou; `RESOLVER_LEASE`;
`OUTBOX_LEASE` mais 2s para soltar os leases (o pior caso de uma passada do relay ou do resolvedor é o lease do que ela
reservou). É por isso que os workers param juntos: em sequência, os orçamentos deles também somariam. Com tracing
ligado, soma-se a exportação dos spans na fila, com prazo de 5s; esgotado, os spans restantes se perdem com um aviso no
log, e o shutdown não falha. Com os padrões, 2 + 32 + 10 + 5 = 49s, ou 54s com tracing. A configuração recusa um
`SHUTDOWN_TIMEOUT` menor que essa soma, contando só os componentes ligados; a soma satura em vez de estourar, para que
orçamentos absurdamente altos falhem a conferência em vez de dar a volta e passar.

No compose, `stop_grace_period: 65s`, acima do `SHUTDOWN_TIMEOUT` padrão de 60s.

**Workers com término observável.** `internal/worker.Runner` roda um laço por goroutine própria. `Stop` espera o canal
de término ou o prazo do contexto e devolve `ErrStopTimeout` quando o laço não terminou. Uma passada que achou trabalho
emenda na próxima, para drenar um backlog sem esperar o polling, e só uma passada vazia espera o intervalo. Falhas
consecutivas aplicam backoff a partir do dobro do intervalo, para que uma tarefa que falha nunca seja repetida tão
rápido quanto uma ociosa é consultada, com teto de 30s, para que uma queda longa ainda seja notada logo depois de
passar. Um `panic` dentro de uma passada é recuperado e logado com a pilha, sem matar o processo nem encerrar o laço em
silêncio. O laço, como o consumidor, roda num contexto próprio que guarda os valores do contexto de partida mas não o
cancelamento dele, porque o Fx cancela o contexto do `OnStart` assim que a partida termina.

## 15. Observabilidade

**Logs.** JSON via `log/slog`, uma linha por evento. Toda linha leva o id da instância (`instance`), e os
identificadores (`correlationId`, `messageId`, `transactionId`, `walletId`, `providerId`) viajam no `context`: um
handler os anexa a toda linha registrada com ele. Um id do `context` não pode ser apagado depois de definido, para que
uma camada que só conhece parte da história não apague o que uma camada de fora registrou. Os campos saem na ordem do
`slog`: `time` (RFC 3339, fração até o nanossegundo), `level`, `msg`, `instance` e os outros atributos fixados no logger
(como `consumer`), os da chamada e, por último, os ids do `context`. As chaves são camelCase (`retryIn`,
`consecutiveErrors`, `eventId`…), e a mensagem é uma frase sem prefixo de pacote: quem registrou está nos atributos
(`consumer`, `worker`, `component`). Uma latência medida sai em milissegundos inteiros, com o sufixo `Ms` na chave;
qualquer outra duração (configuração, espera, backoff) sai como texto no formato do Go, sem sufixo. A linha de acesso de
uma aposta e a da partida do consumidor:

```json
{"time":"2026-10-16T23:45:29.207913482Z","level":"INFO","msg":"http request","instance":"app-2","method":"POST","route":"POST /wagering/transactions","status":201,"durationMs":7,"providerId":"provider-a","correlationId":"01a1471b-3a76-7d2e-9b1f-5f0a3c2d1e4b"}
{"time":"2026-10-16T23:45:12.481253917Z","level":"INFO","msg":"consumer started","instance":"app-2","consumer":"wager-transactions","maxGroups":8,"visibilityTimeout":"30s","processingTimeout":"15s","maxReceives":10}
```

Nunca vão para o log, sem exceção: `Authorization`, tokens, segredos, corpo de requisição, de resposta ou de mensagem,
nem valores monetários. Nenhum campo de log leva valor, saldo ou diferença: o log da divergência da reconciliação
registra o `walletId`, a moeda (`currency`) e quantos lançamentos conferiu (`checkedEntries`), e os valores ficam na
resposta. O mesmo vale para o erro de uma invariante que o banco recusa (`WL001` ou `WL002`, sinal de bug). A mensagem
do servidor cita saldos e valores em centavos, então o adaptador a troca por um texto fixo com o código e o nome da
trigger, como `the ledger does not explain a balance or a transaction (SQLSTATE WL001, wallet_matches_ledger)`. A
mensagem original continua na cadeia de erros, alcançável com `errors.As` por um teste ou um depurador, mas não chega ao
log; para diagnosticar bastam a trigger, o caminho do código e os ids da linha. A rota registrada é o padrão casado
(`GET /wallets/{walletId}`), nunca o path cru. Ids, tipo, status e códigos bastam para rastrear uma operação entre
réplicas.

**Métricas** (Prometheus, servidor admin em `:9090` com `/metrics`, fora da API pública para poder ficar sem exposição).
O registro é próprio, e não o global, para que testes e uma segunda instância no mesmo processo não colidam no
registro. O servidor admin limita quanto tempo um cliente segura uma conexão (5s para os headers, 30s para escrever,
60s ocioso), para que um coletor travado não acumule conexões. Os rótulos são conjuntos fechados: rota e método crus
são entrada do cliente e criariam uma série por requisição.

| Métrica | Rótulos |
|---|---|
| `wagering_transactions_total` | channel, kind, status, failure_code |
| `wagering_duplicates_total` | channel, source (idempotency, inbox) |
| `wagering_processing_seconds` (histograma) | channel, kind |
| `db_tx_retries_total` | reason (serialization, deadlock, lock_timeout, version, unique_race, retryable) |
| `sqs_messages_total` | result (processed, duplicate, retry, dlq) |
| `sqs_dlq_total` | reason |
| `outbox_pending_events`, `outbox_oldest_pending_age_seconds` | — (atraso da outbox) |
| `outbox_publish_total` | result |
| `reference_resolutions_total` | result |
| `reconciliation_divergences_total` | — (o `walletId` vai para o log, não para o rótulo, por cardinalidade) |
| `http_requests_total` | route, method, code (rota sem casamento = `unmatched`; método fora do padrão = `OTHER`) |
| `http_request_seconds` (histograma) | route, method |

**Health.** `GET /health/live` responde 200 enquanto o processo atende. `GET /health/ready` responde 200 ou 503 com
`{"status","checks"}`: banco, SQS e o estado de shutdown. As checagens rodam em paralelo, cada uma com 2s de prazo: um
balanceador consulta a cada poucos segundos, e um "não pronto" rápido vale mais que uma resposta demorada. Uma checagem
que entra em pânico conta como falha, porque um endpoint público consultado o tempo todo nunca pode derrubar o processo.
A checagem do SQS é um `GetQueueAttributes` sem atributos na fila de entrada: a chamada sozinha prova que a fila é
alcançável e legível com as credenciais do processo. Como o endpoint é público, o corpo mostra só `ok`/`fail` por
checagem e o motivo vai para o log.

**Reconciliação.** O que ela responde e como uma divergência é reportada está em
[REGRAS §3](docs/REGRAS_DE_NEGOCIO.md#3-ledger). O saldo armazenado e o ledger são lidos numa única transação
`REPEATABLE READ READ ONLY`, a mesma visão para os dois, sem travar escritores. O líquido é calculado em SQL, em
`numeric`, que não estoura, e convertido uma única vez para `bigint`, que falha com `numeric_value_out_of_range` quando
não cabe: os créditos de uma carteira de muito giro podem somar mais que `int64`, mas só o líquido, que é um saldo,
precisa caber. A diferença usa a aritmética de `Money`, também com overflow verificado. Nada é corrigido
automaticamente: uma carteira que discorda do seu ledger pede uma pessoa.

### Tracing

Diferencial opcional do enunciado, desligado por padrão. Com `OTEL_EXPORTER_OTLP_ENDPOINT` definido, cada réplica grava
spans do OpenTelemetry e os exporta em lote por OTLP/HTTP para `<endpoint>/v1/traces`; `OTEL_SERVICE_NAME` (padrão
`wallet`) e o id da instância (`service.instance.id`) identificam o processo. Uma operação vira um trace que atravessa
réplicas e canais:

| Começa em | Spans |
|---|---|
| requisição HTTP | `POST /wagering/transactions` (servidor) → `wagering.submit` → `uow.snapshot` e `uow.do` → um span por comando SQL → depois, na réplica cujo relay publicar, um `send wallet-events.fifo` por evento |
| mensagem SQS | `process wager-transactions.fifo` (consumidor; continua o trace do produtor quando a mensagem traz `traceparent`) → `wagering.submit` → `uow.do` → SQL → `send wallet-events.fifo` |
| passada do relay | `outbox.relay` (só quando reivindicou eventos), com um link para a unidade de trabalho que gravou cada um → `uow.do` da confirmação |
| passada do resolvedor | `resolver.pass` (só quando reivindicou pendências) → `wagering.resume` por transação → `uow.do` → SQL |

O trace de uma aposta pelo nginx, como o Jaeger o devolve (a `app-2` atendeu a requisição; o relay da `app-1` publicou
os dois eventos, cada um num span filho da unidade de trabalho que o gravou):

```text
POST /wagering/transactions               app-2  3.87ms
└─ wagering.submit                        app-2  wagering.status=PROCESSED
   ├─ uow.snapshot                        app-2  uow.outcome=committed
   │  ├─ BEGIN
   │  ├─ SELECT wager_transactions
   │  └─ COMMIT
   └─ uow.do                              app-2  uow.outcome=committed
      ├─ BEGIN
      ├─ SELECT wallets
      ├─ SELECT wager_transactions        (duas vezes: chave de idempotência e id externo)
      ├─ INSERT wager_transactions
      ├─ INSERT wallet_ledger_entries
      ├─ UPDATE wallets
      ├─ BATCH INSERT outbox_events
      ├─ COMMIT
      ├─ send wallet-events.fifo          app-1  WagerTransactionProcessed, link para outbox.relay
      └─ send wallet-events.fifo          app-1  WalletBalanceChanged, link para outbox.relay
```

**Quem começa um trace.** Só as bordas: o servidor HTTP (`otelhttp`), o consumidor, e uma passada do relay ou do
resolvedor que encontrou trabalho. A unidade de trabalho, o SQL, `Submit` e `Resume` só abrem span debaixo de um span
que está gravando (`Tracer.Child`), nunca um trace próprio. É isso que deixa de fora o polling dos workers, que roda
várias vezes por segundo em cada réplica e quase sempre não acha nada: sem essa regra, o Jaeger se encheria de traces
de um `SELECT` vazio. Cada componente recebe o `Tracer` na construção, com o tracer do escopo já resolvido: pedir esse
tracer ao provider a cada span passaria por um mutex do provider inteiro. A readiness e a liveness (`/health/*`)
também não são rastreadas: o balanceador as consulta o tempo todo.

**Propagação.** W3C Trace Context e Baggage na entrada: o header `traceparent` do HTTP e os atributos `traceparent` e
`tracestate` da mensagem SQS (o consumidor já recebe todos os atributos). Uma chamada que chega com contexto continua o
trace de quem chamou; sem ele, o serviço começa um. A outbox leva o contexto até a publicação: o `INSERT` do evento
grava na coluna `traceparent` (migration 000009) o contexto da unidade de trabalho que está gravando, e o publicador
envia cada evento num span `send` filho desse contexto, com um link para a passada do relay, e põe o `traceparent`
desse span no atributo da mensagem, para que um consumidor de `wallet-events.fifo` continue o mesmo trace. A coluna
guarda só o `traceparent`, de tamanho fixo e conferido por `CHECK`: `tracestate` e baggage vêm do cliente, sem tamanho
que o serviço controle, e não ficam gravados nem seguem adiante. O resolvedor não liga a retomada ao trace do envio,
porque a transação não guarda contexto; as duas pontas levam `wagering.transaction_id`, e a busca por tag do Jaeger
acha os dois traces.

**Amostragem e limites.** `ParentBased(AlwaysSample)`: todo trace novo é gravado, e uma chamada que chega com um
`traceparent` não amostrado (flag `00`) não é. Não há amostragem por proporção, que pede um `float` e o guarda de
arquitetura só permite em `internal/platform/metrics`. O processador em lote do SDK guarda até 2048 spans e descarta o
excesso se o coletor não acompanhar; exportar nunca bloqueia uma requisição nem uma mensagem. Os erros do exportador
viram linhas `WARN` no log JSON. O `otelhttp` recebe um `MeterProvider` no-op: as métricas continuam só no Prometheus.

**Cardinalidade e o que nunca vai num span.** Os nomes vêm de conjuntos fechados: a rota casada (`GET
/wallets/{walletId}`, nunca o path cru; uma rota sem casamento fica só com o método), o resumo do comando SQL
(`operação tabela`, tirado do texto do SQL, que é sempre uma constante do código) e nomes fixos (`uow.do`,
`wagering.submit`, `send wallet-events.fifo`…). Os atributos são canal, tipo, status, código de falha, resultado e ids
(`wallet.id`, `wagering.transaction_id`, `wagering.provider_id`, `outbox.event_id`, `messaging.message.id`), mais o
texto parametrizado do SQL (`db.query.text`, com `$1`, nunca os argumentos). Valores, saldos, tokens, `Authorization` e
corpos nunca entram, e os testes conferem isso em cada span. Uma falha marca o span com `error.type` de um conjunto
fechado (SQLSTATE, a classe do erro como `unavailable` ou `lock_timeout`, o motivo da DLQ, o código de erro do SQS); a
mensagem do erro fica no log, porque pode citar dados e porque o log já leva o `traceId` e o `spanId` de toda linha
gravada sob um span. Do log de uma requisição chega-se ao trace, e do trace ao log.

**Ligar e ver.** `docker compose -f docker-compose.yml -f docker-compose.tracing.yml up --build -d` acrescenta o Jaeger
2.20.0 (all-in-one, em memória; UI em `127.0.0.1:16686`, OTLP/HTTP em `127.0.0.1:4318`) e liga as três réplicas a
ele. O compose base não muda. A 2.20.0 é a última versão que ainda serve a API JSON `/api/traces`, que o README usa para
conferir os traces pela linha de comando; a 2.21 só tem a `/api/v3`.

**Custo desligado.** Sem endpoint, o provider é no-op: nenhum exportador, nenhum processador, nenhuma goroutine. O HTTP
não é embrulhado pelo `otelhttp`, o pool não recebe o tracer de SQL e o consumidor não lê o contexto da mensagem; um
`traceparent` que chega é ignorado e nenhum sai nos eventos. A unidade de trabalho, `Submit` e `Resume` recebem um
`Tracer` desligado, e o `Child` dele confere um campo e volta, sem consultar o `context`; o consumidor confere o mesmo
campo antes de anotar no span o resultado de cada mensagem. O que sobra no caminho quente é uma consulta ao `context`
em três pontos: por requisição, para dar ao span o nome da rota casada; por `Append` da outbox, para ler o
`traceparent`; e por linha de log, para o `traceId`. Os benchmarks `BenchmarkTracingOff*` medem isso
num `context` com cinco valores, como o de uma requisição (Apple M5): `Child` e `Start` num `Tracer` desligado, cerca
de 2,6 ns; `TraceParent`, cerca de 7 ns, a mesma consulta que nomeia a rota e que a linha de log faz; nenhum aloca. Uma
linha de log inteira, com essa consulta dentro, custa cerca de 720 ns e três alocações. Para medir de novo:
`go test -run '^$' -bench TracingOff -benchmem ./internal/platform/tracing/ ./internal/platform/logging/`. Ligado, uma
aposta pelo HTTP grava 18 spans (o trace acima). O binário cresce 5,2 MB (de 20,8 para 26,0 MB): o exportador OTLP
oficial liga `google.golang.org/grpc` e o gateway gRPC mesmo no transporte HTTP, pela configuração que compartilha com o
exportador gRPC e pelos stubs do coletor, embora nenhuma conexão gRPC seja aberta. O peso fica só no binário: o SDK e o
exportador moram em `internal/platform/tracing/otlp`, que só o bootstrap importa. Há saídas, nenhuma tomada. A menor é
`otlptrace.New` com um cliente HTTP pequeno escrito aqui, que monte o envelope `ExportTraceServiceRequest` com
`protowire` em vez de importar o pacote do coletor: o gRPC sai, o `otlptrace` continua convertendo os spans para o
protobuf do OTLP, e o cliente só os envelopa e envia. Um exportador inteiro escrito à mão, ou o Zipkin, também tiraria o
peso, ao preço de código próprio de serialização ou de um protocolo fora do padrão OTLP. Fica o exportador oficial, que
o projeto OpenTelemetry mantém e testa, em troca dos 5,2 MB.

**Shutdown.** O tracer para por último (§14): exporta o que ainda está na fila, inclusive os spans do próprio shutdown,
em até 5s, que entram na conta do `SHUTDOWN_TIMEOUT`. Uma exportação que não termina a tempo vira um aviso, e o
shutdown não falha por spans perdidos.

## 16. Contrato de erros HTTP

Erros vão em `application/problem+json` (RFC 9457): `type: "about:blank"`, `title`, `status`, `code` estável, `detail`,
`correlationId` e, quando há erro por campo, `errors[]` com `field` e `code`. O único membro extra é o `walletId` do
`409 WALLET_ALREADY_EXISTS`. O cliente decide pelo `code`, nunca pelo texto. "Persistido" separa entrada corrigível
(nada gravado, chave livre para reenvio) de resultado definitivo (linha REJECTED ou FAILED gravada e evento emitido).
Uma rota inexistente (`404 NOT_FOUND`) e um método que a rota não aceita (`405 METHOD_NOT_ALLOWED`, com o `Allow` do
roteador) também respondem no contrato, e não com o texto puro do roteador da biblioteca padrão: o `ServeMux` só deixa
o padrão da requisição vazio quando nenhuma rota casou, então só os `404` e `405` dele são trocados, mantendo status e
`Allow`, e um `404` que um handler escreveu, como `WALLET_NOT_FOUND`, passa intacto. O middleware mais externo
transforma um `panic` de handler, ou de outro middleware, num `500` problem+json e o loga com a pilha;
`http.ErrAbortHandler` é relançado, porque é como um handler pede ao `net/http` para derrubar a conexão.

A tabela completa de códigos (code × HTTP × persistido × quando), com o tratamento de cada um na fila, está em
[REGRAS §8](docs/REGRAS_DE_NEGOCIO.md#8-rejeições-e-códigos-de-falha); respostas reais de cada classe estão no
[README](README.md#contrato-de-erros). As classes, distinguíveis pelo status:

| Classe | HTTP | Corpo | Persistido |
|---|---|---|---|
| entrada inválida | `400`, `413`, `415` | problem+json, com `errors[]` por campo quando cabe | nada; a chave fica livre para o reenvio corrigido |
| autenticação e autorização | `401` (com `WWW-Authenticate`), `403` | problem+json | nada |
| não encontrado | `404` | problem+json (`WALLET_NOT_FOUND`, `TRANSACTION_NOT_FOUND`, este também para a transação de outro provedor) | nada |
| rota ou método inexistente | `404`, `405` | problem+json `NOT_FOUND` ou `METHOD_NOT_ALLOWED`, este com `Allow` | nada |
| conflito | `409` | problem+json (`IDEMPOTENCY_KEY_REUSED`, `DUPLICATE_EXTERNAL_TRANSACTION`, `WALLET_ALREADY_EXISTS`, este com `walletId`) | nada |
| resultado da transação | `201` processada, `200` replay, `202` + `Location` pendente, `422` rejeitada, `500` falhada | o corpo da transação (`transactionId`, `status`, `failureCode`, `balance` quando há, `idempotentReplay`) | a transação, no estado final ou em `PENDING_REFERENCE` |
| indisponibilidade | `503` + `Retry-After: 2` | problem+json `TEMPORARILY_UNAVAILABLE` | nada; repetir é seguro |
| erro inesperado | `500` | problem+json `INTERNAL_ERROR`, sem detalhe; o detalhe vai para o log | nada |

Uma rejeição de negócio não é um erro HTTP no sentido de "não aconteceu": ela aconteceu, ficou gravada, emitiu
`WagerTransactionRejected` e o replay a devolve igual, por isso sai no corpo da transação, e não em problem+json. Um
`500` com `INTERNAL_ERROR` é um bug, nunca gravado; um `500` no corpo da transação é um `FAILED` registrado pelo
resolvedor, o único estado terminal que não é decisão de negócio.

O decoder é estrito: `DisallowUnknownFields`, um único objeto JSON (`{}{}` e `{} x` são recusados), corpo até 64 KiB e
`Content-Type: application/json` (com parâmetros como `charset`). O servidor define `ReadHeaderTimeout`, `ReadTimeout`,
`WriteTimeout`, `IdleTimeout` e `MaxHeaderBytes` (16 KiB: o maior header esperado é um bearer token de poucos
kilobytes, bem abaixo do 1 MB padrão do `net/http`), e encerra o contexto de cada requisição em
`HTTP_REQUEST_TIMEOUT` (10s; com a API ligada, maior que os 5s da busca do JWKS): um banco que para de responder sem
fechar as conexões, como um atrás de uma rede partida, faria a requisição esperar enquanto a queda durasse, e o
`WriteTimeout` só cortaria a conexão, sem resposta. Com o prazo, o trabalho falha como indisponível e o cliente recebe
`503` + `Retry-After` a tempo: a configuração exige que o prazo, somado aos 5s que o rollback do trabalho cortado pode
levar (`config.RollbackTimeout`), caiba no `WriteTimeout`. Com os padrões, 10 + 5 = 15s, o `WriteTimeout` inteiro, mas o
rollback só chega aos 5s numa conexão aberta que não responde: o prazo que interrompe um comando fecha a conexão em que
ele rodava, e o rollback numa conexão fechada volta na hora. O prazo também convém passar de `DB_LOCK_TIMEOUT` ×
`DB_TX_MAX_ATTEMPTS` mais as esperas curtas entre as tentativas (6s com os padrões), para que uma requisição presa a uma
carteira travada esgote as tentativas antes de ser cortada; isso não é validado, porque as duas saídas são o mesmo
`503`.

O cursor do ledger é base64url de `{"v":1,"seq":N}`, opaco para o cliente e sem HMAC, de propósito: adulterá-lo só move
a página dentro de uma carteira que o chamador interno já pode ler. O `seq` é uma identity com cache 1, então cresce
também entre sessões, e os escritores de uma carteira são serializados pelo lock dela, então dentro da carteira ele
segue a ordem dos movimentos; `ledger_entry_check` recusa um lançamento que não seguisse. Só a grafia exata produzida
pelo servidor é aceita (o cursor é decodificado, codificado de novo e comparado), e qualquer outra dá `INVALID_CURSOR`,
em vez de ser meio entendida. A query é lida com `url.ParseQuery`, e não com `r.URL.Query`, que descarta em silêncio um
par que não decodifica (`cursor=%zz` viraria "sem cursor" e responderia a primeira página); um parâmetro vazio ou
repetido é tão errado quanto um adulterado, e `limit` tem uma grafia só (sem sinal, sem zero à esquerda, até três
dígitos). A página pede um item a mais que o `limit` para saber se há próxima, então uma página que termina exatamente
no último lançamento não manda o cliente buscar uma vazia. A paginação é por `seq` crescente, que só cresce: inserções
durante a leitura nunca repetem nem pulam itens.

## 17. Testes e simulação de falhas

| Camada | Comando | Infra |
|---|---|---|
| unitários | `go test ./...`, `go test -race ./...` | nenhuma (sem Docker) |
| unitários com os failpoints reais | `go test -tags=failpoints -race ./...` (`make test-failpoints`) | nenhuma (sem Docker) |
| integração | `go test -tags=integration -race ./...` | PostgreSQL 18, Keycloak 26 e MiniStack reais via testcontainers-go |
| e2e multi-instância | `go test -tags=e2e -race -count=1 ./test/e2e/...` | 3 processos do binário compilado com `-tags failpoints` |
| gerador de carga, partes puras | `go test -tags=load ./test/load/...` (também em `make test`) | nenhuma (sem Docker) |
| teste de carga | `make load-test` ([`docs/TESTE_DE_CARGA.md`](docs/TESTE_DE_CARGA.md)) | o compose inteiro, com o gerador num container na rede dele |

Nada da infraestrutura é substituído por mock: os testes de integração usam o banco, o IdP e o broker de verdade, com o
mesmo script de papéis e o mesmo realm do compose.

**Failpoints.** `failpoint.Inject(nome)` é uma função vazia em qualquer build sem a tag `failpoints`, e o compilador a
elimina. Com a tag, `FAILPOINTS=nome=exit` faz o processo sair com código 137 naquele ponto exato, o mesmo de um `kill
-9`. Um nome desconhecido ou uma especificação malformada encerra com código 2 na primeira chamada, para que um erro de
digitação não deixe o teste passar sem disparar o ponto.

| Failpoint | O que prova | Cenário |
|---|---|---|
| `submit.before_commit` | nada gravado, nem a claim da inbox; o retry HTTP ou a reentrega da mensagem processa uma vez | `TestE2E_SubmitKilledBeforeCommit`, `TestE2E_ConsumerKilledBeforeCommit` |
| `submit.after_commit` | o retry em outra instância vira replay, com o saldo original | `TestE2E_SubmitKilledAfterCommit` |
| `consumer.after_commit_before_delete` | a reentrega, no consumidor de outra instância, vira duplicata da inbox | `TestE2E_ConsumerKilledAfterCommit` |
| `outbox.after_claim` | o lease vence e o outro relay publica | `TestE2E_OutboxCrashAfterClaim` |
| `outbox.after_publish_before_ack` | o outro relay republica com o mesmo `eventId` | `TestE2E_OutboxCrashAfterPublishBeforeAck` |
| `resolver.after_claim` | outra instância retoma a pendência quando o lease vence | `TestE2E_ResolverKilledAfterClaim` |

Os outros cenários e2e, todos com três instâncias: `TestE2E_SameBet50Times` (a mesma BET 50 vezes ao mesmo tempo, 25
pelas APIs das três instâncias e 25 pela fila, como mensagens distintas; um débito e 49 duplicatas contadas pelo
próprio serviço); `TestE2E_TwoBetsOf80_HTTPandSQS`; `TestE2E_ParallelWallets` (muitas carteiras ao mesmo tempo, e uma
carteira presa que só atrasa as próprias apostas); `TestE2E_CrossChannelSameOperation`;
`TestE2E_PendingReferenceResolvedAndExpired` (um ROLLBACK pela fila antes da BET, resolvido quando ela chega por HTTP, e
outro cuja BET nunca chega, rejeitado ao fim do TTL de 3s, com o evento publicado);
`TestE2E_RestartPreservesEverything` (todas as instâncias mortas de uma vez com requisições, mensagens e pendências em
voo, e três novas no lugar); `TestE2E_PostgresAndSQSOutage` (o banco e depois o MiniStack pausados por 10s cada, com
apostas chegando pelos dois canais: nada perdido, nada duplicado, nenhuma mensagem na DLQ, e a API respondendo `503`
ao que não pode fazer); e `TestE2E_SIGTERMUnderLoadThroughNginx` (uma de três instâncias parada com `SIGTERM` atrás do
nginx do compose, com carga pelos dois canais: sai com código 0 dentro do orçamento, nenhum cliente vê resposta fora do
contrato e toda aposta é decidida uma vez). Ao fim de cada cenário, a reconciliação de todas as carteiras dele precisa
dar consistente. O mapa dos cenários obrigatórios do enunciado para os testes está no
[README](README.md#testes).

## 18. Limitações, interpretações e trabalho não concluído

**Emulador (MiniStack).**
- O root do MiniStack não é autenticado: a chave `test` com qualquer segredo, qualquer chave de 12 dígitos e até
  requisições sem assinatura agem como root. As garantias de IAM valem para as chaves de usuário (assinatura conferida,
  política avaliada). Na AWS real esse caminho não existe; localmente, o root serve só ao provisionamento e à leitura
  manual das DLQs.
- O `SenderId` é sempre o id da conta, então não identifica o produtor; por isso a identidade é o token no atributo
  (§11).
- O estado é só em memória: reiniciar o MiniStack perde filas, usuários e chaves. O `aws-provision` refaz tudo (o
  compose o roda a cada `up`), mas as réplicas que já estavam no ar precisam reiniciar, porque o SDK lê as chaves do
  arquivo de credenciais uma vez, na partida.
- `maxReceiveCount` volta como string no MiniStack e como número na AWS. O `provision` o grava como string, a forma da
  documentação da AWS; o serviço nunca o lê, e o teste do provisionamento, o único que o lê, aceita as duas formas.

**Mensageria.**
- O redrive da DLQ de entrada é manual. As cópias na DLQ não levam o `ProviderToken`: ele é uma credencial, e não
  serviria depois do redrive porque o `SentTimestamp` muda. Reprocessar uma mensagem da DLQ exige que o provedor a envie
  de novo, com token novo.
- O `messageId` é a identidade global da mensagem. Se um provedor reutilizar o id de outro, a mensagem vai para a DLQ
  como `MESSAGE_ID_REUSED`. É uma violação do contrato do produtor e não tem efeito financeiro.
- A deduplicação do SQS FIFO tem escopo de fila: produtores precisam de `MessageDeduplicationId` únicos entre
  provedores.
- A ordem por carteira no relay implica bloqueio de cabeça de fila: um evento que falha sempre segura os seguintes da
  mesma carteira. É aceito de propósito (a ordem vale mais) e alertado pela idade do pendente mais antigo.
- O claim visita, a cada passada, toda carteira com evento pendente, e não só as que entram no lote: o custo cresce com
  o número delas, não com o backlog. Com 50 mil carteiras com um evento pendente cada, um claim leva cerca de 120 ms (a
  consulta anterior levava 11 ms com estatísticas em dia e minutos com estatísticas velhas), menos que publicar os 500
  eventos do lote. Um teto na varredura recursiva limitaria esse custo, mas ela visitaria sempre os menores
  `aggregate_id` e deixaria as outras carteiras sem vez (§12).
- Fan-out via SNS FIFO seria o desenho natural em produção; ficou fora da stack obrigatória e é a evolução prevista.

**Interpretações adotadas.** As regras completas estão em [`docs/REGRAS_DE_NEGOCIO.md`](docs/REGRAS_DE_NEGOCIO.md).
- Aceite síncrono: uma operação sem dependência é decidida na mesma transação em que é registrada; `PENDING` não é
  comitado. O resolvedor ainda retoma `PENDING` vencido.
- Uma reversão bem-sucedida por referência, seja REFUND ou ROLLBACK, e WIN sobre BET revertida é rejeitado
  ([REGRAS §5](docs/REGRAS_DE_NEGOCIO.md#5-referências-e-reversões)).
- Um `REFUND` ou `ROLLBACK` de uma `BET` que já teve `WIN` é aceito, uma única vez. O enunciado não trata o caso, o
  `WIN` nem sempre referencia a aposta e quem decide o resultado do jogo é o provedor: a carteira garante que o débito
  volta no máximo uma vez, sem saldo negativo e com tudo no ledger. Para anular o prêmio, o provedor faz o `ROLLBACK`
  do `WIN` ([REGRAS §5](docs/REGRAS_DE_NEGOCIO.md#5-referências-e-reversões)).
- Rejeições de posse (`CURRENCY_MISMATCH`, `WALLET_PLAYER_MISMATCH`) não guardam saldo e respondem sem `balance`.
- Entrada só na forma canônica, sem normalização (§3).
- Toda mensagem do SQS precisa levar o access token do Keycloak do próprio provedor no atributo `ProviderToken`: uma
  mensagem montada exatamente como o exemplo do enunciado, sem esse atributo, vai para a DLQ como `UNAUTHENTICATED` /
  `MISSING_PROVIDER_TOKEN`. O `SenderId` do SQS no MiniStack é o mesmo para todo remetente e não identifica o provedor
  (§11; [REGRAS §10](docs/REGRAS_DE_NEGOCIO.md#10-mensageria)).
- O `occurredAt` da mensagem só é aceito na forma canônica, em UTC com milissegundos (`2026-09-08T12:00:00.000Z`);
  qualquer outra grafia vai para a DLQ como `INVALID_MESSAGE`, porque a entrada é estrita e nunca normalizada (§3, §11).
- Um `WIN` cuja referência opcional ainda não chegou espera em `PENDING_REFERENCE`, como um `REFUND` ou um `ROLLBACK`,
  em vez de pagar um prêmio de uma aposta que o serviço ainda não viu (§10;
  [REGRAS §6](docs/REGRAS_DE_NEGOCIO.md#6-referências-pendentes)).
- `db_tx_retries_total` conta a decisão de fazer retry de uma transação, inclusive quando o contexto cancela a espera em
  seguida.
- O decoder JSON da biblioteca padrão aceita chaves duplicadas (vale a última) e casa nomes de campo sem diferenciar
  maiúsculas de minúsculas. Os campos ainda precisam existir no contrato e ter o tipo certo, e o hash é calculado do
  comando validado, então isso não produz conflito falso nem movimento indevido.

**Limitações conhecidas.**
- A partida recusa qualquer schema diferente do que o binário embute, inclusive um mais novo (N+1). Isso impede que
  uma réplica sirva num schema que ela não conhece, mas também impede voltar só o binário depois de aplicar uma
  migration: para voltar, reverte-se a migration antes (`make migrate-down`). Um deploy contínuo pediria uma faixa de
  versões aceitas e migrations no estilo expand/contract; não foi feito.
- Uma pendência cuja linha não carrega (a da transação ou a da carteira violando uma invariante, que a reidratação
  recusa) não chega a `FAILED`, porque nem a falha pode ser gravada nela: ela volta a ser reivindicada a cada lease,
  com um WARN no log, até um operador corrigir o registro (§10).
- Um JWKS fora do ar por mais tempo que as `CONSUMER_MAX_RECEIVES` tentativas (cerca de 15 min) leva mensagens válidas
  para a DLQ, de onde só o provedor as reprocessa, reenviando (§11).
- O corpo publicado de um evento é o JSONB da outbox como o PostgreSQL o devolve, com outra ordem de chaves que a do
  construtor (§12); o contrato é o JSON, não os bytes.
- `HTTP_REQUEST_TIMEOUT` maior que `DB_LOCK_TIMEOUT` × `DB_TX_MAX_ATTEMPTS` é recomendado, não validado: as duas saídas
  são o mesmo `503`.

**Fora de escopo.**
- Ledger de partidas dobradas (o ledger é de uma entrada por movimento, encadeado e auditável).
- Reversão parcial e câmbio entre moedas.
- Reconciliação periódica automática: ela é sob demanda.

**Trabalho não concluído.**
- Dashboards, diferencial opcional do enunciado: não foram feitos. A observabilidade é a de logs JSON com os ids de
  correlação e de trace, métricas Prometheus, health checks e o tracing OpenTelemetry, que fica desligado por padrão
  (§15).
- Teste de carga, outro diferencial opcional: feito, com comando reproduzível, ambiente, metodologia, vazão,
  p50/p95/p99, erros, conflitos, atraso da outbox e integridade conferida no fim, em
  [`docs/TESTE_DE_CARGA.md`](docs/TESTE_DE_CARGA.md). Ele roda num host só e com o MiniStack no lugar do SQS, que
  limita o caminho da fila e dos eventos; o documento separa o que é do serviço e o que é do emulador.
- O cenário e2e do nginx (`TestE2E_SIGTERMUnderLoadThroughNginx`) passou no Docker Desktop (macOS) e num Docker Engine
  29.8.2 de Linux (arm64), pelo `host-gateway` e com as instâncias escutando em `0.0.0.0` ([README](README.md#testes)).
  Não foram exercitados o Linux em amd64, as distribuições com glibc, o Docker rootless e o SELinux.
- Não há CI: os alvos do `Makefile` (os `make test*`, `lint`, `vet` e `fmt`) reproduzem localmente tudo o que um
  pipeline rodaria.
- Não há ferramenta de redrive da DLQ nem de reparo de pendências presas: as duas coisas são operações manuais.
