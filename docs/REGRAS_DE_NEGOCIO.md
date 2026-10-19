# Regras de negócio

Este documento reúne as regras de negócio do serviço de carteiras: o que cada operação faz, quando é recusada, o que
fica gravado e o que é publicado. Cada regra traz o resultado observável (status da transação, código de falha, status
HTTP, tratamento na fila, se algo é persistido) e os testes que a provam. As decisões técnicas e o porquê de cada uma
estão em [`ARCHITECTURE.md`](../ARCHITECTURE.md); como subir, chamar e testar, no [`README.md`](../README.md); o
enunciado, em [`CHALLENGE.md`](CHALLENGE.md).

**Convenções.**

- **Provedor** é o cliente externo que envia operações (provider-a, provider-b), identificado pela claim `provider_id`
  do token. **Serviço interno** é o cliente com o escopo `wallets:manage`, que administra carteiras.
- **Persistido** quer dizer que a transação fica gravada, num estado final ou em `PENDING_REFERENCE`, e que o replay a
  devolve igual. **Nada gravado** quer dizer nenhuma transação, lançamento, evento ou entrada na inbox; a chave de
  idempotência continua livre para o reenvio corrigido.
- Os testes são nomes de funções Go (`grep -rn 'func TestNome' .` os encontra). Os de arquivos `*_integration_test.go`
  e de `test/integration` rodam com `-tags=integration`; os `TestE2E_*`, com `-tags=e2e`; os demais são unitários.

## Sumário

1. [Dinheiro](#1-dinheiro)
2. [Carteira](#2-carteira)
3. [Ledger](#3-ledger)
4. [Operações](#4-operações)
5. [Referências e reversões](#5-referências-e-reversões)
6. [Referências pendentes](#6-referências-pendentes)
7. [Idempotência](#7-idempotência)
8. [Rejeições e códigos de falha](#8-rejeições-e-códigos-de-falha)
9. [Autorização](#9-autorização)
10. [Mensageria](#10-mensageria)
11. [Eventos](#11-eventos)

---

## 1. Dinheiro

| Regra | Resultado | Testes |
|---|---|---|
| Valores em centavos inteiros (`int64`), escala fixa de 2 casas; nenhum `float` no código de produção | aritmética exata | `TestNoFloatInProduction`, `TestMoney_Arithmetic` |
| Moedas aceitas: `BRL`, `USD` e `EUR`, só em maiúsculas. Uma carteira tem uma moeda, que nunca muda, e não há câmbio | outra moeda, ou `brl`: `400 UNSUPPORTED_CURRENCY` (campo `UNSUPPORTED`, ou `REQUIRED` se vazio); nada gravado | `TestParseCurrency`, `TestParse_UnsupportedCurrency` |
| O valor é uma string JSON exatamente na forma `^(0\|[1-9][0-9]*)\.[0-9]{2}$` | qualquer outra grafia: `400 INVALID_MONEY` com o motivo no campo (tabela abaixo); nada gravado | `TestParse_Valid`, `TestParse_Invalid`, `TestSubmit_ValidationErrors` |
| Nada é normalizado: sem trim, sem troca de caixa, sem completar casas | `" 25.00"` e `"25"` são recusados, não corrigidos | `TestNewCommand_NeverTrims` |
| Valor e moeda são conferidos juntos | os dois errados geram as duas recusas na mesma resposta | `TestParse_ReportsAmountAndCurrency`, `TestParse_ReportsOnlyWhatIsWrong` |
| Faixa aceita na entrada: de `0.00` a `92233720368547758.07` (o máximo de `int64` em centavos); a entrada nunca é negativa | acima do máximo: `OVERFLOW` | `TestParse_Invalid` |
| Nenhuma conta dá a volta: soma e subtração verificam overflow | um crédito que passaria do máximo vira `REJECTED BALANCE_LIMIT_EXCEEDED`; chegar exatamente ao máximo é permitido | `TestMoney_Overflow`, `TestMoney_BoundaryResultsFit`, `TestCredit_ReachesTheLimitExactly`, `TestDecide_CreditOverflow` |
| Na saída, o valor é string com duas casas (`"975.00"`, `"0.00"`), ao lado da moeda, e um valor zero nunca é omitido | `{"amount":"0.00","currency":"BRL"}` é um saldo real | `TestMoney_StringAndJSON`, `TestData_ZeroMoneyIsNotOmitted`, `TestSubmit_RejectedWithZeroBalanceKeepsIt` |

Motivos de recusa de um `amount` (`errors[].code` do campo), na ordem em que são conferidos:

| Motivo | Exemplo |
|---|---|
| `EMPTY` | `""` ou campo ausente |
| `NOT_A_NUMBER` | `"NaN"`, `"Infinity"`, `"-inf"` |
| `SCIENTIFIC_NOTATION` | `"1e3"` |
| `WHITESPACE` | `" 25.00"`, `"25.00 "` |
| `SIGNED` | `"+25.00"`, `"-25.00"` |
| `MALFORMED` | `".50"`, `"25,00"`, `"2a.00"` |
| `MISSING_SCALE` | `"25"` |
| `SCALE_EXCEEDED` | `"25.000"` |
| `SCALE_TOO_SMALL` | `"25.5"`, `"25."` |
| `LEADING_ZERO` | `"025.00"`, `"00.00"` |
| `OVERFLOW` | `"92233720368547758.08"` |
| `NOT_A_STRING` | `25.00` como número JSON (só no HTTP; na fila, um campo que não é string invalida o envelope) |

## 2. Carteira

| Regra | Resultado | Testes |
|---|---|---|
| Só o serviço interno abre carteiras: `POST /wallets` com `playerId` (UUID canônico, minúsculo, não nulo) e `initialBalance` (`>= 0`), cuja moeda é a da carteira | `201`, `Location: /wallets/{id}` e `{id, playerId, balance, version: 1}` | `TestOpenWallet_Created`, `TestOpenWallet_InvalidPlayer`, `TestOpenWallet_InvalidMoney`, `TestOpenWallet_StrictBody` |
| Saldo inicial positivo: no mesmo commit, a carteira na versão 1, a transação `OPENING` já `PROCESSED`, o lançamento `CREDIT` de 0 ao saldo inicial e os eventos `WagerTransactionProcessed` e `WalletBalanceChanged` | tudo ou nada | `TestOpen_PositiveIsAtomic`, `TestOpen_PositiveWritesInOrder`, `TestOpen_PositiveCreatesOpeningEntry` |
| Saldo inicial zero: só a carteira, na versão 1 | nenhuma `OPENING`, nenhum lançamento, nenhum evento | `TestOpen_ZeroCreatesNothingFinancial`, `TestOpen_ZeroHasNoEntry`, `TestOpenWallet_ZeroBalance` |
| Uma carteira por jogador e moeda; o mesmo jogador pode ter uma em cada moeda | segunda abertura, inclusive a repetição de uma já comitada ou duas simultâneas: `409 WALLET_ALREADY_EXISTS` com o id da existente no membro `walletId`; nada novo gravado | `TestOpen_DuplicateConflict`, `TestOpen_ConcurrentSamePlayerCurrency`, `TestOpen_AlreadyExistsNamesTheWalletOfThatCurrency`, `TestOpenWallet_ConflictBody` |
| O `409` só sai com o id: se a busca dele falha, o chamador recebe essa falha | por exemplo, `503 TEMPORARILY_UNAVAILABLE` | `TestOpen_AlreadyExistsLookupFailure` |
| `POST /wallets` não tem `Idempotency-Key`: a unicidade de (jogador, moeda) é o que torna a repetição segura | a repetição responde `409` com o `walletId` de quem perdeu a resposta `201` | `TestOpen_DuplicateConflict` |
| O saldo nunca é negativo. Debitar exatamente o saldo é permitido e o leva a zero | débito maior que o saldo nunca é aplicado (vira rejeição, §4); o banco recusa saldo negativo mesmo que o código erre | `TestDebit_ExactBalanceReachesZero`, `TestDebit_InsufficientLeavesStateUnchanged`, `TestSchema_BalanceCannotBeNegative` |
| A versão começa em 1 e sobe exatamente 1 a cada mudança de saldo. `LOSS`, rejeição e espera não a mudam | o banco recusa versão fora de passo com o saldo, e um escritor que leu uma versão antiga não grava | `TestSubmit_LossKeepsVersion`, `TestSchema_WalletGuard`, `TestWalletRepo_UpdateVersionConflict` |
| Id, jogador, moeda e criação são imutáveis; uma carteira nunca é apagada | `UPDATE` dessas colunas, `DELETE` e `TRUNCATE` recusados pelo banco | `TestSchema_WalletGuard` |
| `updatedAt` nunca anda para trás, mesmo com réplicas de relógios diferentes | fica no maior valor entre o atual e o do movimento | `TestMovement_UpdatedAtNeverMovesBack` |
| Consulta: `GET /wallets/{walletId}` | `200 {id, playerId, balance, version, createdAt, updatedAt}`, horários em UTC com milissegundos; inexistente: `404 WALLET_NOT_FOUND`; id não canônico: `400 INVALID_REQUEST` | `TestGetWallet`, `TestGetWallet_TimesInUTC`, `TestGetWallet_NotFound`, `TestGetWallet_InvalidID` |

## 3. Ledger

| Regra | Resultado | Testes |
|---|---|---|
| Todo movimento de saldo tem um lançamento: direção (`DEBIT`/`CREDIT`), valor `> 0` e os saldos antes e depois, com `depois = antes ± valor` | um lançamento se audita sozinho | `TestNewLedgerEntry_ValidatesArithmetic`, `TestSchema_LedgerArithmetic` |
| O ledger é imutável e só cresce, para o serviço e para o dono do schema | `UPDATE`, `DELETE` e `TRUNCATE` recusados | `TestSchema_LedgerAppendOnlyEvenForOwner`, `TestSchema_AppRoleCannotMutateLedger` |
| No máximo um lançamento por carteira e transação | um segundo é recusado pelo banco | `TestSchema_WalletAndLedgerChecks` |
| Encadeamento: cada lançamento continua o anterior da carteira (o primeiro parte de 0) | um lançamento que não encadeia, ou que bifurcaria o ledger, é recusado | `TestSchema_LedgerEntryContinuesTheWallet`, `TestLedgerRepo_AppendRefusesABrokenChain`, `TestSchema_StaleWriterCannotForkTheLedger`, `TestWallet_LedgerChainsAcrossMovements` |
| No commit, o saldo da carteira é o `balanceAfter` do último lançamento, ou 0 sem lançamentos | saldo sem lançamento, ou lançamento sem saldo, aborta o commit | `TestSchema_WalletMustMatchLedger` |
| Só uma transação `PROCESSED` gera lançamento, na direção do tipo: `OPENING`, `WIN` e `REFUND` creditam; `BET` debita; `ROLLBACK` faz o contrário da referência; `LOSS` nunca gera | e toda transação processada que move dinheiro tem exatamente um | `TestHardening_EntryRequiresProcessedTransaction`, `TestHardening_DirectionByKind`, `TestHardening_RollbackDirection`, `TestHardening_RollbackOfRefundDebits`, `TestHardening_LossHasNoEntry`, `TestHardening_ProcessedMovementHasEntry` |
| Leitura: `GET /wallets/{walletId}/ledger` em ordem de inserção, com cursor opaco; `limit` de 1 a 200, padrão 50; a última página traz `"nextCursor": null` | inserções durante a leitura nunca repetem nem pulam itens; cursor que o serviço não emitiu, ou `limit` fora da faixa: `400 INVALID_CURSOR`; carteira inexistente: `404` | `TestLedger_Pagination`, `TestLedger_DefaultLimit`, `TestLedger_BadLimit`, `TestCursor_RoundTripAndTamper`, `TestLedgerRepo_PageStableOrder`, `TestLedger_UnknownWallet` |

**Reconciliação.** `POST /wallets/{walletId}/reconciliation` reconstrói o saldo pelo ledger (créditos − débitos,
abertura incluída) e o compara com o armazenado, numa leitura consistente. Responde
`{walletId, storedBalance, calculatedBalance, difference, consistent, checkedEntries}`, com
`difference = armazenado − reconstruído`. Nunca altera o saldo: uma divergência sai na resposta (`consistent: false`),
num log ERROR com o `walletId`, a moeda e `checkedEntries` (sem valores) e na métrica
`reconciliation_divergences_total`. Uma carteira sem lançamentos reconcilia em zero. Os créditos de uma carteira podem
somar mais que `int64` ao longo da vida dela; só o líquido precisa caber, e um líquido que não cabe é erro (`500`), não
um número que deu a volta. Testes: `TestReconcile_Consistent`, `TestReconcile_ZeroEntries`,
`TestReconcile_DetectsDivergence`, `TestReconcile_DivergenceReportedOnce`, `TestLedgerRepo_TotalsBeyondInt64Turnover`,
`TestLedgerRepo_TotalsNetOverflowIsAnError`, `TestReconcile_ResponseShape`, `TestReconcile_NotFound`.

## 4. Operações

| Tipo | Quem envia | Efeito | Valor | Referência | Eventos quando `PROCESSED` |
|---|---|---|---|---|---|
| `OPENING` | só o serviço, na abertura com saldo positivo | crédito do saldo inicial | `> 0` | nenhuma | `WagerTransactionProcessed` + `WalletBalanceChanged` |
| `BET` | provedor | débito; exige saldo | `> 0` | proibida | `WagerTransactionProcessed` + `WalletBalanceChanged` |
| `WIN` | provedor | crédito do prêmio | `> 0`, qualquer valor (não precisa ser o da aposta) | opcional, a uma `BET` | `WagerTransactionProcessed` + `WalletBalanceChanged` |
| `LOSS` | provedor | nenhum: sem lançamento e sem mudar a versão | exatamente `0.00` | proibida | só `WagerTransactionProcessed` |
| `REFUND` | provedor | crédito integral de uma aposta | igual ao da `BET` | obrigatória, a uma `BET` | `WagerTransactionProcessed` + `WalletBalanceChanged` |
| `ROLLBACK` | provedor | o contrário da referência: `BET` vira crédito; `WIN` e `REFUND` viram débito | igual ao da referência | obrigatória, a `BET`, `WIN` ou `REFUND` | `WagerTransactionProcessed` + `WalletBalanceChanged` |

Testes: `TestDecide_Bet`, `TestDecide_Win`, `TestDecide_LossNoMovement`, `TestDecide_RefundOfBet`,
`TestSubmit_BetProcessed`, `TestSubmit_LossKeepsVersion`, `TestSubmit_RefundAfterBet`.

**Entrada.** `POST /wagering/transactions` (com `Idempotency-Key`) e a mensagem da fila passam pelas mesmas regras. São
obrigatórios `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind` e `money`. Os
ids de texto (`providerId`, `externalTransactionId`, `roundId`, `gameId`, `referenceExternalTransactionId`) seguem
`^[A-Za-z0-9._:-]{1,128}$`; `playerId` e `walletId` são UUIDs canônicos (minúsculos, com hífens) e nunca o UUID nulo.
Todos os campos errados vêm de uma vez em `errors[]`, e o `code` do topo é o mais específico, na ordem
`INVALID_IDEMPOTENCY_KEY` > `INVALID_KIND` > `INVALID_MONEY` > `UNSUPPORTED_CURRENCY` > `INVALID_AMOUNT_FOR_KIND` >
`REFERENCE_REQUIRED` > `INVALID_REQUEST`. Testes: `TestNewCommand_Invalid`, `TestNewCommand_ReportsEveryProblem`,
`TestSubmit_ValidationErrors`, `TestValidTextID`.

| Regra de entrada | Resultado (nada gravado) | Testes |
|---|---|---|
| `OPENING` só nasce dentro do serviço | `OPENING` vindo de fora: `400 INVALID_KIND` (campo `UNSUPPORTED`); na fila, DLQ `INVALID_MESSAGE` / `INVALID_KIND` | `TestSubmit_OpeningKindRejected`, `TestConsumer_OpeningKindToDLQ` |
| Política de zero: `LOSS` vale `0.00`; `BET`, `WIN`, `REFUND` e `ROLLBACK` valem mais que zero | `400 INVALID_AMOUNT_FOR_KIND` (`MUST_BE_ZERO` ou `MUST_BE_POSITIVE`); o banco repete a regra | `TestNewCommand_Invalid`, `TestSchema_ZeroPolicy` |
| `REFUND` e `ROLLBACK` exigem referência | `400 REFERENCE_REQUIRED` | `TestNewCommand_Invalid` |
| `BET` e `LOSS` não aceitam referência: ela é recusada, não ignorada, para que o hash nunca cubra um campo sem sentido | `400 INVALID_REQUEST` (campo `NOT_ALLOWED`) | `TestNewCommand_Invalid` |
| Uma transação não pode citar a si mesma | `400 INVALID_REQUEST` (campo `SELF_REFERENCE`) | `TestNewCommand_Invalid` |

**Ordem das regras.** Uma operação aceita na entrada é decidida sob o lock da carteira, e a primeira regra que se aplica
decide (`TestDecide_RuleOrder`):

1. **Posse**, para todo tipo, `LOSS` incluído: a carteira é do `playerId` informado (`WALLET_PLAYER_MISMATCH`) e está na
   moeda da operação (`CURRENCY_MISMATCH`).
2. **Referência**, quando há uma (§5).
3. **Espera**, se a referência ainda não chegou ou não terminou (§6).
4. **Movimento**: `BET` sem saldo (`INSUFFICIENT_FUNDS`); `ROLLBACK` de `WIN` ou `REFUND` sem saldo
   (`REVERSAL_INSUFFICIENT_FUNDS`); crédito além do limite de `int64` (`BALANCE_LIMIT_EXCEEDED`).

| Regra | Resultado | Testes |
|---|---|---|
| A carteira precisa existir | `404 WALLET_NOT_FOUND`, nada gravado; na fila, DLQ `WALLET_NOT_FOUND` | `TestSubmit_WalletNotFound` |
| Posse: carteira de outro jogador ou em outra moeda | `REJECTED` com `WALLET_PLAYER_MISMATCH` ou `CURRENCY_MISMATCH`, gravada como foi enviada, `422` **sem** `balance` (o saldo da carteira de outro jogador nunca chega a este provedor, e o de outra moeda não cabe na linha), evento `WagerTransactionRejected` | `TestDecide_WalletPlayerMismatch`, `TestDecide_CurrencyMismatch`, `TestSubmit_OwnershipRejectsWithoutBalance`, `TestSubmit_RejectedWithoutBalance` |
| `BET` maior que o saldo | `REJECTED INSUFFICIENT_FUNDS`, `422` com o saldo da época, evento `WagerTransactionRejected`; o saldo não muda | `TestDecide_BetInsufficient`, `TestSubmit_TwoBetsOf80On100`, `TestE2E_TwoBetsOf80_HTTPandSQS` |
| `ROLLBACK` de `WIN` ou `REFUND` que debitaria mais que o saldo: desfazer um crédito já gasto não é uma aposta recusada, por isso tem código próprio | `REJECTED REVERSAL_INSUFFICIENT_FUNDS`, `422` com o saldo | `TestDecide_RollbackOfWinInsufficient`, `TestSubmit_RollbackOfWinInsufficient` |
| Crédito que passaria do máximo representável | `REJECTED BALANCE_LIMIT_EXCEEDED`, `422` com o saldo | `TestDecide_CreditOverflow` |
| Uma operação sem dependência é decidida na mesma transação SQL em que é registrada e gravada já no estado final; `PENDING` nunca é comitado | o banco recusa gravar `PENDING` | `TestTxRepo_InsertRefusesPending` |
| Uma rejeição de negócio é definitiva: a transação final nunca muda, e o replay a devolve igual | `422` também no replay | `TestSubmit_RejectionPersistsAndReplays`, `TestSchema_TerminalImmutable` |

## 5. Referências e reversões

A referência é procurada por (`providerId` do token, `referenceExternalTransactionId`) e nunca cruza provedores: uma
reversão que cita o id externo de outro provedor espera uma transação que, para ela, não existe, e termina em
`REFERENCE_NOT_FOUND` (`TestAuthz_CrossProviderReversal`, `TestTxRepo_FindByExternalScopedByProvider`). Uma `OPENING`
não tem id externo e por isso nunca é alvo de referência.

**Tipos permitidos.** `REFUND` → `BET`; `WIN` → `BET`; `ROLLBACK` → `BET`, `WIN` ou `REFUND`. Nada referencia uma
`LOSS`, que não moveu dinheiro, nem um `ROLLBACK`, o que deixaria corrigir uma correção de volta.

**Ordem das checagens da referência**, depois da posse (§4): tipo permitido (`INVALID_REFERENCE_KIND`) → estado
(ainda em andamento: espera; `REJECTED` ou `FAILED`: `REFERENCE_NOT_PROCESSED`) → mesmo provedor, jogador, carteira,
moeda e rodada (`REFERENCE_MISMATCH`) → para `REFUND` e `ROLLBACK`, exatamente o valor da referência
(`REVERSAL_AMOUNT_MISMATCH`; um `WIN` não é comparado) → referência ainda não revertida (`REFERENCE_ALREADY_REVERSED`).
O tipo vem antes do estado: um `REFUND` que cita um `WIN` rejeitado, ou um `REFUND` ainda pendente, recebe
`INVALID_REFERENCE_KIND`. Testes: `TestDecide_ReferenceMatrix`, `TestDecide_RuleOrder`, `TestDecide_AmountMismatch`,
`TestDecide_ReferenceMismatch`, `TestDecide_ReferenceMismatchRound`.

**Matriz.** Linhas: a operação que chega; colunas: a referência encontrada.

| Operação ↓ · referência → | `BET` processada | `WIN` processado | `REFUND` processado | `ROLLBACK` ou `LOSS` (qualquer estado) | alvo permitido, `REJECTED` ou `FAILED` | alvo permitido, ainda em `PENDING_REFERENCE` | ausente |
|---|---|---|---|---|---|---|---|
| `REFUND` | crédito do valor da aposta | `INVALID_REFERENCE_KIND` | `INVALID_REFERENCE_KIND` | `INVALID_REFERENCE_KIND` | `REFERENCE_NOT_PROCESSED` | não ocorre: uma `BET` nunca espera | espera (§6) |
| `ROLLBACK` | crédito (a aposta é desfeita) | débito do prêmio | débito da devolução | `INVALID_REFERENCE_KIND` | `REFERENCE_NOT_PROCESSED` | espera (o `WIN` ou o `REFUND` terminar) | espera (§6) |
| `WIN` | crédito do prêmio; a referência só é validada | `INVALID_REFERENCE_KIND` | `INVALID_REFERENCE_KIND` | `INVALID_REFERENCE_KIND` | `REFERENCE_NOT_PROCESSED` | não ocorre: uma `BET` nunca espera | espera (§6) |

Toda rejeição desta seção é `REJECTED` persistida, `422` com o saldo da época e evento `WagerTransactionRejected`.

**Uma reversão bem-sucedida por referência.** Cada transação aceita no máximo uma reversão `PROCESSED`, somando `REFUND`
e `ROLLBACK` (não uma de cada tipo). Reversões rejeitadas, falhas ou ainda pendentes não contam. O efeito é que um
débito nunca é devolvido duas vezes:

| Sequência | Resultado | Testes |
|---|---|---|
| `REFUND` e depois `ROLLBACK` da mesma `BET`, ou o contrário | a segunda: `REJECTED REFERENCE_ALREADY_REVERSED` | `TestDecide_AlreadyReversed` |
| dois `REFUND` (ou dois `ROLLBACK`) da mesma `BET`, inclusive simultâneos, por canais ou instâncias diferentes | o segundo: `REFERENCE_ALREADY_REVERSED`; o índice único parcial é a rede de segurança quando dois chegam juntos | `TestSchema_SingleProcessedReversal`, `TestTxRepo_SecondReversalIsRetryable`, `TestResume_TwoPendingReversalsSameBet` |
| `REFUND` da `BET` e depois `ROLLBACK` **do `REFUND`** | permitido, porque a referência é outra: o `ROLLBACK` debita a devolução e a aposta volta a valer | `TestDecide_ReferenceMatrix`, `TestHardening_RollbackOfRefundDebits` |
| depois disso, nova reversão ou `WIN` sobre a mesma `BET` | `REFERENCE_ALREADY_REVERSED`: a `BET` continua com a reversão consumida | `TestDecide_AlreadyReversed` |
| `WIN` sobre uma `BET` já revertida | `REJECTED REFERENCE_ALREADY_REVERSED`: pagar prêmio de aposta cancelada quebraria a coerência financeira | `TestSubmit_WinOnReversedBetIsRejected`, `TestDecide_RuleOrder` |
| `WIN` sobre uma `BET` não revertida | permitido; o `WIN` não consome a reversão da `BET`, então ela ainda pode ser devolvida ou desfeita uma vez | `TestDecide_ReferenceMatrix`, `TestTxRepo_HasProcessedReversal` |

**Autorreferência.** Uma referência igual ao próprio `externalTransactionId` é erro de entrada corrigível:
`400 INVALID_REQUEST`, campo `referenceExternalTransactionId` com `SELF_REFERENCE`, nada gravado
(`TestNewCommand_Invalid`). Uma linha gravada antes de essa regra existir, que encontra a si mesma como referência, é
rejeitada com `INVALID_REFERENCE_KIND`, porque nenhum tipo é alvo do próprio tipo, em vez de esperar até expirar
(`TestDecide_SelfReference`).

## 6. Referências pendentes

**Quando.** Um `REFUND`, `ROLLBACK` ou `WIN` com referência espera quando a referência ainda não chegou (para aquele
provedor) ou quando chegou com tipo permitido mas ainda não terminou. Posse (carteira, jogador e moeda) é conferida já
na entrada, e uma falha dela rejeita na hora; o saldo só é avaliado na resolução.

| Momento | Resultado | Testes |
|---|---|---|
| entrada em espera | transação gravada `PENDING_REFERENCE`, com `attempts = 0`, `nextAttemptAt` e `expiresAt = agora + REFERENCE_TTL`; HTTP `202` com `Location: /wagering/transactions/{id}`, sem `balance`; pela fila, a mensagem é concluída e apagada depois do commit; evento `WagerTransactionPendingReference`, uma única vez | `TestDecide_MissingReferenceEntersPending`, `TestSubmit_RefundBeforeBetIsPending`, `TestSubmit_Outcomes`, `TestConsumer_PendingReferenceCompletesMessage`, `TestSubmit_InboxPendingCompletes` |
| busca sem achar, antes do fim | reagenda (`attempts + 1`), sem evento | `TestDecide_PendingRescheduleBackoff`, `TestResume_ReschedulesWithBackoff` |
| a referência chega `PROCESSED` | segue a matriz da §5, com o saldo de agora | `TestDecide_ResolvesWaitingTransaction`, `TestResume_ResolvesAfterReferenceArrives`, `TestResolver_ResolvesPendingRefund` |
| a referência termina `REJECTED` ou `FAILED` | `REJECTED REFERENCE_NOT_PROCESSED` + `WagerTransactionRejected` | `TestResume_ReferenceRejected` |
| última busca, ou `expiresAt` alcançado, com a referência ausente | `REJECTED REFERENCE_NOT_FOUND` + `WagerTransactionRejected` | `TestDecide_ExpiresByAttempts`, `TestDecide_ExpiresByTTL`, `TestResume_RejectsAfterTTL`, `TestResolver_ExpiresPending` |
| última busca, ou `expiresAt` alcançado, com a referência ainda em andamento | `REJECTED REFERENCE_NOT_PROCESSED` | `TestDecide_ExistingPendingReferenceExpiresAsNotProcessed` |

Uma transação que esperou responde `202` enquanto espera e, depois de resolvida, o replay da mesma chave e as consultas
devolvem o estado final. O cliente acompanha por `GET /wagering/transactions/{id}` ou
`GET /providers/{providerId}/wagering/transactions/{externalTransactionId}`; `attempts` conta as buscas que não acharam
a referência e reagendaram a transação. Fluxo completo com três instâncias:
`TestE2E_PendingReferenceResolvedAndExpired`.

**Tentativas, backoff e TTL.**

| Parâmetro | Padrão | Variável | Regra |
|---|---|---|---|
| buscas | 10 | `REFERENCE_MAX_ATTEMPTS` | o resolvedor procura a referência no máximo esse número de vezes, depois da entrada em espera; a última busca que não a acha rejeita |
| espera antes da busca *n* | 1s, dobrando até 60s | `REFERENCE_BACKOFF_BASE`, `REFERENCE_BACKOFF_MAX` | `min(base × 2^(n−1), teto)`, com jitter uniforme na metade superior (entre a metade e o valor cheio), para que pendências criadas juntas não vençam juntas; a base não pode passar do teto |
| TTL | 15 min | `REFERENCE_TTL` | conta da entrada em espera, é fixado nesse momento e nunca é estendido; nenhuma busca é marcada depois de `expiresAt`, então um TTL curto expira no prazo |

O que vier primeiro, buscas ou TTL, encerra a espera. Com os padrões, as dez esperas somam no máximo
1 + 2 + 4 + 8 + 16 + 32 + 4 × 60 = 303s (entre ~2,5 e ~5 min com o jitter), então as buscas se esgotam antes do TTL.
Testes: `TestReferencePolicy_NextAttemptAt`, `TestPositiveJitter_WithinUpperHalf`,
`TestPositiveJitter_NeverZeroForTinyBounds`, `TestRescheduleReference`, `TestSchema_TransactionGuard`.

**Em série por carteira.** Duas reversões pendentes da mesma `BET` são resolvidas sob o lock da carteira, uma depois da
outra: a primeira passa e a segunda recebe `REFERENCE_ALREADY_REVERSED` (`TestResume_TwoPendingReversalsSameBet`).

**Retomada por qualquer instância.** A pendência é retomada pelo resolvedor de qualquer réplica; uma réplica que morre
no meio não perde nem duplica nada, e outra a retoma quando o lease vence (`TestE2E_ResolverKilledAfterClaim`,
`TestResolver_TwoResolversNoDoubleProcessing`, `TestResume_StaleLeaseIsSkipped`). Como funciona está em
[ARCHITECTURE.md §10](../ARCHITECTURE.md#10-referências-pendentes).

**Falha permanente e `FAILED`.** Uma falha de infraestrutura que se repetiria igual (o banco recusar os dados, uma
invariante violada) na resolução de uma pendência é contada nela. Ao atingir `PERMANENT_FAILURE_LIMIT` (3), a transação
termina `FAILED` com `PROCESSING_FAILED`: é o único estado final que não é decisão de negócio. Não emite evento; sai um
log ERROR com o pedido e o provedor de origem, e a métrica `wagering_transactions_total`. O replay da mesma chave
responde `500` com o corpo da transação (`status: FAILED`); a consulta responde `200`. Uma indisponibilidade (banco fora
do ar, lock disputado) não conta: a pendência é tentada de novo depois. Uma linha que nem carrega (a da transação ou a
da carteira violando uma invariante) não chega a `FAILED`, porque nem a falha pode ser gravada nela: fica pendente, com
um WARN a cada lease, até um operador corrigi-la. Testes: `TestResume_PermanentFailureMarksFailedAtLimit`,
`TestNotePermanentFailure_FailsAtLimit`, `TestResume_StaleLeaseDoesNotCountAPermanentFailure`,
`TestResume_PermanentFailureThatCannotBeCounted`, `TestRunOnce_ReportsATransactionThatKeepsFailingOnEveryClaim`,
`TestRunOnce_StopsAtAnUnavailableDatabase`, `TestSubmit_Outcomes`.

## 7. Idempotência

**Escopo da chave.** A chave é única por provedor: `(providerId do token, chave)`. Separadamente,
`(providerId, externalTransactionId)` também é único. No HTTP a chave vem do header `Idempotency-Key`, no SQS de
`data.idempotencyKey`; em ambos segue `^[\x21-\x7E]{1,255}$` e é usada exatamente como recebida. A mesma chave em outro
provedor é uma operação independente. A idempotência está no banco e sobrevive ao reinício de todas as instâncias
(`TestE2E_RestartPreservesEverything`). Testes: `TestSchema_IdempotencyKeys`, `TestAuthz_ProviderIsolation`,
`TestSubmit_KeyIsScopedByProvider`.

| Header `Idempotency-Key` (HTTP) | Resultado (nada gravado) | Testes |
|---|---|---|
| ausente ou vazio | `400 IDEMPOTENCY_KEY_REQUIRED` | `TestSubmit_MissingIdempotencyKey` |
| enviado mais de uma vez, mesmo com valores iguais | `400 INVALID_IDEMPOTENCY_KEY` | `TestSubmit_RepeatedIdempotencyKey` |
| fora do padrão | `400 INVALID_IDEMPOTENCY_KEY` | `TestSubmit_InvalidIdempotencyKey` |

Na fila, uma `data.idempotencyKey` ausente ou fora do padrão vai para a DLQ como `INVALID_MESSAGE` /
`INVALID_IDEMPOTENCY_KEY`.

**Hash canônico v1.** O conteúdo de uma operação é identificado pelo SHA-256, em hexadecimal minúsculo, de um JSON
canônico do comando validado: chaves em ordem lexicográfica em todos os níveis, sem espaços, UTF-8, sem escape de HTML
(`<`, `>` e `&` literais) e sem quebra de linha final. Entram `externalTransactionId`, `gameId`, `kind`,
`money.amount`, `money.currency`, `playerId`, `providerId`, `roundId`, `walletId` e
`referenceExternalTransactionId` (omitido quando ausente). Ficam de fora a própria chave, `messageId`, `type`,
`occurredAt`, headers e correlation id, então HTTP e SQS produzem o mesmo hash para o mesmo comando. Nada é normalizado
antes do hash, porque a entrada só é aceita na forma canônica. O formato está congelado: qualquer mudança, mesmo um
campo novo, sai como v2. Vetor de teste (o exemplo do enunciado):

```
{"externalTransactionId":"transaction-123","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"},"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","providerId":"provider-a","roundId":"round-987","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37"}
→ 629836932b79106b99523d06a1e7fa80689b0ea1e1c47aa3f0a5a2c87d0c4344
```

Testes: `TestPayloadHash_Vector`, `TestPayloadHash_ExcludesIdempotencyKey`,
`TestPayloadHash_ChangesWithAnyBusinessField`, `TestPayloadHash_OmitsAbsentReference`, `TestPayloadHash_IsLowercaseHex`,
`TestCanonicalHash_NoHTMLEscape`, `TestHashEquivalence_HTTPvsSQS`.

**Decisão.** Os testes de `internal/app/wagering/submit_test.go` provam as decisões de replay e de conflito no caso de
uso, com repositórios em memória e sem banco; os de `submit_integration_test.go` e `inbox_integration_test.go` provam o
mesmo contra o PostgreSQL.

| Situação | HTTP | Fila | Gravado | Testes |
|---|---|---|---|---|
| chave nova, `externalTransactionId` novo | decide: `201`, `202` ou `422` | decide e apaga a mensagem | a transação | `TestSubmit_BetProcessed` |
| chave existe, mesmo hash | replay: o resultado gravado com `idempotentReplay: true` (`200` PROCESSED, `202` PENDING_REFERENCE, `422` REJECTED, `500` FAILED) | duplicata: apaga a mensagem | nada novo | `TestSubmit_SameKeySamePayloadIsAReplay`, `TestSubmit_ReplaySameKey`, `TestSubmit_Outcomes`, `TestSubmit_RejectionPersistsAndReplays` |
| chave existe, hash diferente | `409 IDEMPOTENCY_KEY_REUSED` | DLQ `IDEMPOTENCY_KEY_REUSED` | nada | `TestSubmit_SameKeyOtherPayloadIsRefused`, `TestSubmit_NewMessageReusingAKeyIsRefused`, `TestSubmit_KeyReusedDifferentPayload` |
| chave nova, `(providerId, externalTransactionId)` já existe | `409 DUPLICATE_EXTERNAL_TRANSACTION` | DLQ `DUPLICATE_EXTERNAL_TRANSACTION` | nada | `TestSubmit_KnownExternalIDUnderANewKeyIsRefused`, `TestSubmit_SameExternalOtherKey` |
| duas "primeiras" requisições simultâneas com a mesma chave | o índice único decide; a perdedora refaz a transação e cai em replay ou `409` | idem | uma transação | `TestSubmit_ConcurrentFirstUseOfKey`, `TestTxRepo_UniqueKeyRace`, `TestSubmit_SameBet50Concurrent`, `TestE2E_SameBet50Times` |
| a mesma operação pelos dois canais, em qualquer ordem e instância | a segunda chegada é replay da primeira | idem | uma transação | `TestE2E_CrossChannelSameOperation` |
| requisição recusada antes da decisão (entrada inválida, provedor errado, `503`) | `4xx` ou `503` | — | nada; a chave continua livre para o reenvio corrigido | `TestAuthz_BodyProviderMismatch`, `TestE2E_ParallelWallets` |

**Replay com o saldo original.** O `balance` do replay é o saldo e a versão gravados na linha no momento da decisão
(`result_balance_minor`/`result_wallet_version`), nunca o saldo atual. Só `PROCESSED` e `REJECTED` guardam saldo; uma
pendência e um `FAILED` não foram decididos contra ele e respondem sem `balance`, assim como as rejeições de posse
(`CURRENCY_MISMATCH`, `WALLET_PLAYER_MISMATCH`). Um replay não decide nada nem emite evento. Testes:
`TestSubmit_ReplaySameKey`, `TestSubmit_ReplayKeepsOriginalBalance`, `TestE2E_SubmitKilledAfterCommit`.

**`messageId` e inbox.** Na fila, o `messageId` do envelope é a identidade durável da mensagem: a chave da inbox é
`(nome do consumidor, messageId)`, sem o provedor, então o id precisa ser globalmente único (UUID). O hash da inbox é o
SHA-256 canônico de `{"type", "data"}` com **todos** os campos de `data`, inclusive `idempotencyKey`; `messageId` e
`occurredAt` ficam de fora porque descrevem a entrega.

| Situação | Resultado | Testes |
|---|---|---|
| mesmo `messageId`, mesmo hash (reentrega) | duplicata: responde com a transação registrada, sem decidir de novo nem esperar o lock da carteira; a mensagem é apagada | `TestSubmit_RedeliveredMessageIsADuplicate`, `TestConsumer_DuplicateDelivery`, `TestSubmit_InboxDuplicate`, `TestSubmit_InboxDuplicateDoesNotWaitForTheWalletLock`, `TestConsumer_CrashBetweenCommitAndDelete`, `TestE2E_ConsumerKilledAfterCommit` |
| mesmo `messageId`, hash diferente (outro conteúdo, outra chave ou outro provedor) | DLQ `MESSAGE_ID_REUSED`, sem efeito financeiro | `TestSubmit_MessageIDReusedForAnotherPayload`, `TestSubmit_InboxMessageIDReused`, `TestConsumer_MessageIDReusedToDLQ` |
| `messageId` novo, chave conhecida com o mesmo comando | replay pela chave; a inbox registra `REPLAY`; a mensagem é apagada | `TestSubmit_NewMessageWithAStoredKeyIsAReplay`, `TestConsumer_SameKeyDifferentMessageIDs`, `TestSubmit_InboxReplayRecordsReplay` |
| mensagem recusada (`409`, carteira inexistente, erro) | nenhuma entrada na inbox | `TestSubmit_InboxLeavesRefusedMessagesOut` |

A inbox, a transação, o ledger e a outbox vão no mesmo commit: não existe "marquei como lida mas não debitei"
(`TestSubmit_InboxAndDomainAtomic`).

## 8. Rejeições e códigos de falha

O cliente decide pelo `code`, nunca pelo texto. Erros saem em `application/problem+json`; o resultado de uma
transação (`201`, `200`, `202`, `422`, `500` com `status: FAILED`) sai no corpo da transação, com `status` e
`failureCode`. Na fila, a coluna diz o que acontece com a mensagem; DLQ é "na hora", com `FailureReason` (e
`FailureCode`, quando há) nos atributos.

**Entrada, autenticação e conflitos: nada gravado.**

| `code` | Quando | HTTP | Fila |
|---|---|---|---|
| `INVALID_REQUEST` | JSON malformado, campo desconhecido ou de tipo errado, mais de um objeto, campo obrigatório ausente, id fora do padrão, UUID não canônico ou nulo, referência em `BET`/`LOSS`, autorreferência, id do path inválido | 400 | envelope fora do formato (inclusive campo que não é string, `null` ou desconhecido) ou comando inválido: DLQ `INVALID_MESSAGE` / `INVALID_REQUEST` |
| `INVALID_MONEY` | valor fora da forma canônica, inclusive número JSON | 400 | DLQ `INVALID_MESSAGE` / `INVALID_MONEY` |
| `UNSUPPORTED_CURRENCY` | moeda fora de `BRL`, `USD`, `EUR`, ou em minúsculas | 400 | DLQ `INVALID_MESSAGE` / `UNSUPPORTED_CURRENCY` |
| `INVALID_KIND` | tipo vazio, desconhecido ou `OPENING` | 400 | DLQ `INVALID_MESSAGE` / `INVALID_KIND` |
| `INVALID_AMOUNT_FOR_KIND` | `LOSS` diferente de `0.00`, ou outro tipo igual a `0.00` | 400 | DLQ `INVALID_MESSAGE` / `INVALID_AMOUNT_FOR_KIND` |
| `REFERENCE_REQUIRED` | `REFUND` ou `ROLLBACK` sem referência | 400 | DLQ `INVALID_MESSAGE` / `REFERENCE_REQUIRED` |
| `IDEMPOTENCY_KEY_REQUIRED` | header ausente ou vazio | 400 | — (na fila, chave ausente é `INVALID_IDEMPOTENCY_KEY`) |
| `INVALID_IDEMPOTENCY_KEY` | chave fora do padrão, ou header repetido | 400 | DLQ `INVALID_MESSAGE` / `INVALID_IDEMPOTENCY_KEY` |
| `INVALID_CURSOR` | cursor do ledger que o serviço não emitiu, ou `limit` fora de 1..200 | 400 | — |
| `PAYLOAD_TOO_LARGE` | corpo acima de 64 KiB (o nginx responde o mesmo antes de repassar) | 413 | — |
| `UNSUPPORTED_MEDIA_TYPE` | `Content-Type` que não é `application/json` | 415 | — |
| `UNAUTHENTICATED` | token ausente, inválido, expirado ou mal configurado | 401 + `WWW-Authenticate: Bearer` | DLQ `UNAUTHENTICATED` / `MISSING_PROVIDER_TOKEN` ou `INVALID_PROVIDER_TOKEN` |
| `FORBIDDEN` | falta o escopo ou o papel da rota | 403 | token válido que não é de provedor com `wagering:write`: DLQ `FORBIDDEN` / `NOT_PROVIDER` |
| `PROVIDER_MISMATCH` | `providerId` do corpo ou do path diferente do provedor do token | 403 | DLQ `PROVIDER_IDENTITY_MISMATCH` |
| `WALLET_NOT_FOUND` | carteira inexistente | 404 | DLQ `WALLET_NOT_FOUND` |
| `TRANSACTION_NOT_FOUND` | transação inexistente, de outro provedor ou uma `OPENING`, lida por um provedor | 404 | — |
| `NOT_FOUND`, `METHOD_NOT_ALLOWED` | caminho que nenhuma rota tem; método que a rota não aceita (com `Allow`) | 404, 405 | — |
| `IDEMPOTENCY_KEY_REUSED` | mesma chave com conteúdo diferente | 409 | DLQ `IDEMPOTENCY_KEY_REUSED` |
| `DUPLICATE_EXTERNAL_TRANSACTION` | mesmo `(providerId, externalTransactionId)` com outra chave | 409 | DLQ `DUPLICATE_EXTERNAL_TRANSACTION` |
| `WALLET_ALREADY_EXISTS` | já existe carteira para o jogador e a moeda; `walletId` no corpo | 409 | — |
| `MESSAGE_ID_REUSED` | mesmo `messageId` com outro conteúdo | — | DLQ `MESSAGE_ID_REUSED` |

**Códigos por campo.** Uma recusa de entrada lista em `errors[]` cada campo errado, com `field` e um `code` próprio; a
ordem que escolhe o `code` do topo está na §4.

| `errors[].code` | Quando |
|---|---|
| `REQUIRED` | o campo não foi enviado, ou veio vazio |
| `INVALID_FORMAT` | id, chave, cursor ou `limit` fora do padrão; UUID fora da forma minúscula com hífens, ou o UUID nulo |
| `UNSUPPORTED` | tipo ou moeda que a entrada não aceita, inclusive `OPENING` |
| `NOT_ALLOWED` | referência num tipo que não aceita referência (`BET`, `LOSS`) |
| `SELF_REFERENCE` | referência à própria transação |
| `MUST_BE_ZERO`, `MUST_BE_POSITIVE` | valor válido, mas incompatível com o tipo |
| `NOT_A_STRING` | valor monetário enviado como número JSON (`25.00` em vez de `"25.00"`) |
| `EMPTY`, `NOT_A_NUMBER`, `SCIENTIFIC_NOTATION`, `WHITESPACE`, `SIGNED`, `MALFORMED`, `MISSING_SCALE`, `SCALE_TOO_SMALL`, `SCALE_EXCEEDED`, `LEADING_ZERO`, `OVERFLOW` | o motivo da recusa de um `amount` (§1); um `amount` vazio ou ausente é `EMPTY`, e não `REQUIRED` |

**Rejeições de negócio: `REJECTED`, persistida, `422` (também no replay), evento `WagerTransactionRejected`; na fila, a
mensagem é concluída e apagada** (`TestConsumer_RejectionIsTerminal`).

| `failureCode` | Quando | `balance` na resposta |
|---|---|---|
| `INSUFFICIENT_FUNDS` | `BET` maior que o saldo | sim |
| `REVERSAL_INSUFFICIENT_FUNDS` | `ROLLBACK` de `WIN` ou `REFUND` debitaria mais que o saldo | sim |
| `BALANCE_LIMIT_EXCEEDED` | o crédito passaria do máximo de `int64` em centavos | sim |
| `CURRENCY_MISMATCH` | moeda da operação diferente da moeda da carteira | não |
| `WALLET_PLAYER_MISMATCH` | a carteira não é do `playerId` informado | não |
| `INVALID_REFERENCE_KIND` | o tipo da referência não é alvo permitido deste tipo | sim |
| `REFERENCE_NOT_PROCESSED` | a referência terminou `REJECTED` ou `FAILED`, ou não terminou até a espera acabar | sim |
| `REFERENCE_MISMATCH` | provedor, jogador, carteira, moeda ou rodada diferentes dos da referência | sim |
| `REVERSAL_AMOUNT_MISMATCH` | `REFUND`/`ROLLBACK` com valor diferente do da referência | sim |
| `REFERENCE_ALREADY_REVERSED` | a referência já tem reversão processada (vale também para `WIN` sobre aposta revertida) | sim |
| `REFERENCE_NOT_FOUND` | a referência não chegou dentro das buscas ou do TTL; só acontece depois de uma espera, então a primeira resposta foi `202` | sim |

**Falha registrada.**

| `failureCode` | Quando | HTTP | Fila |
|---|---|---|---|
| `PROCESSING_FAILED` (status `FAILED`) | falha permanente de infraestrutura repetida `PERMANENT_FAILURE_LIMIT` vezes na resolução de uma pendência (§6); persistida, sem evento | `500` com o corpo da transação no replay; `200` na consulta | uma mensagem repetida dessa operação é duplicata e é apagada |

**Infraestrutura: nada gravado.**

| `code` | Quando | HTTP | Fila |
|---|---|---|---|
| `TEMPORARILY_UNAVAILABLE` | banco ou SQS indisponível, contenção que esgotou as tentativas, verificador de tokens sem conseguir julgar, nenhuma réplica no nginx | `503` + `Retry-After: 2`; repetir é seguro | nova tentativa com backoff; esgotadas, DLQ `RETRIES_EXHAUSTED` (§10) |
| `INTERNAL_ERROR` | erro inesperado (bug); o detalhe vai só para o log | `500` | erro classificado como falha permanente (por exemplo, o banco recusar os dados ou uma invariante violada): DLQ `PROCESSING_FAILED` na hora; erro que ninguém classificou: novas tentativas até a DLQ `RETRIES_EXHAUSTED` (§10) |

O `FailureReason` `PROCESSING_FAILED` da DLQ é um bug no processamento da mensagem, sem nada gravado, e não se confunde
com a transação `FAILED` de mesmo código, que é persistida pelo resolvedor.

## 9. Autorização

O papel vem do token, assinado pelo IdP: **provedor** tem a claim `provider_id` e não tem `wallets:manage`; **serviço
interno** tem `wallets:manage` e não tem `provider_id`. Um token que seria os dois, ou nenhum, é configuração inválida
do IdP e recebe `401` (`TestPrincipal_Kind`, `TestVerify_Misconfigured`). O `providerId` autorizado vem sempre do token,
nunca do corpo, do path ou da mensagem: esses valores só são conferidos contra ele.

| Rota | provider-a | serviço interno | sem token |
|---|---|---|---|
| `POST /wallets`, `GET /wallets/{id}`, `GET /wallets/{id}/ledger`, `POST /wallets/{id}/reconciliation` | `403 FORBIDDEN` | ✓ (`wallets:manage`) | `401` |
| `POST /wagering/transactions` com `providerId=provider-a` | ✓ (exige `wagering:write`) | `403 FORBIDDEN` | `401` |
| `POST /wagering/transactions` com `providerId=provider-b` | `403 PROVIDER_MISMATCH` | `403 FORBIDDEN` | `401` |
| `GET /wagering/transactions/{id}` de uma transação de provider-a | ✓ (exige `wagering:read`) | ✓ | `401` |
| `GET /wagering/transactions/{id}` de provider-b ou de uma `OPENING` | `404 TRANSACTION_NOT_FOUND` | ✓ | `401` |
| `GET /providers/provider-a/wagering/transactions/{ext}` | ✓ (exige `wagering:read`) | ✓ | `401` |
| `GET /providers/provider-b/wagering/transactions/{ext}` | `403 PROVIDER_MISMATCH` | ✓ | `401` |
| `GET /health/live`, `GET /health/ready` | ✓ | ✓ | ✓ |

| Regra | Resultado | Testes |
|---|---|---|
| Cada rota exige o seu escopo: `wagering:write` para enviar, `wagering:read` para consultar, `wallets:manage` para carteiras | sem o escopo: `403 FORBIDDEN` | `TestAuthz_ScopeRequired`, `TestRequireProvider`, `TestRequireReader`, `TestRequireInternal`, `TestSubmit_Authorization` |
| As rotas de carteira são só do serviço interno: um provedor movimenta dinheiro por transações, nunca administrando carteiras | `403 FORBIDDEN` para provedores | `TestAuthz_WalletEndpointsInternalOnly`, `TestAuth_ProviderOnWalletRoute` |
| Um provedor nunca opera em nome de outro: o `providerId` do corpo é conferido contra o token. No HTTP, a validação do corpo (`400`) vem antes dessa conferência; na fila, a identidade vem antes de qualquer outra recusa | `403 PROVIDER_MISMATCH`, nada gravado e a chave livre; na fila, DLQ `PROVIDER_IDENTITY_MISMATCH` com um log WARN marcado `security` | `TestAuthz_BodyProviderMismatch`, `TestSubmit_ProviderMismatchDoesNotCallService`, `TestConsumer_ProviderIdentityMismatchToDLQ`, `TestConsumer_ProviderMismatchNeverReachesSubmit` |
| Um provedor só lê o que é seu. Pelo id interno, a transação de outro provedor (ou uma `OPENING`) responde `404`, e não `403`, para não revelar que o id existe; pelo path, outro `providerId` é `403` | — | `TestAuthz_ProviderIsolation`, `TestGetTransaction_OtherProvider404`, `TestGetByExternal_OtherProvider403`, `TestGetForProvider_Isolation` |
| O serviço interno lê qualquer transação, pelos dois caminhos | `200` | `TestAuth_InternalReadsAnyTransaction` |
| Chaves de idempotência, ids externos e referências são escopados pelo provedor do token | um provedor nunca reproduz nem referencia a transação de outro | `TestAuthz_CrossProviderReversal`, `TestTxRepo_FindByExternalScopedByProvider` |
| Acesso negado não tem efeito financeiro nem expõe dados | nada gravado | `TestAuthz_NoSideEffects` |
| Token ausente, inválido, expirado ou mal configurado | `401 UNAUTHENTICATED` com `WWW-Authenticate: Bearer` | `TestAuth_MissingInvalidExpiredToken`, `TestAuth_RejectedTokens` |
| Verificador sem conseguir julgar o token (JWKS fora do ar) | `503` com `Retry-After`: o cliente deve repetir, não pedir outro token | `TestAuth_VerifierFailureIsUnavailable`, `TestVerify_KeySetOutageIsNotAVerdict` |
| Na fila vale a mesma regra do `POST`: o token do atributo `ProviderToken` precisa ser de um provedor com `wagering:write`, e a validade é julgada no `SentTimestamp` da mensagem | §10 | `TestTokenAttributeIdentity` |

## 10. Mensageria

**O que entra.** A fila `wager-transactions.fifo` recebe `WagerTransactionRequested` no envelope estrito descrito em
[ARCHITECTURE.md §11](../ARCHITECTURE.md#11-sqs-de-entrada), com o token do provedor no atributo `ProviderToken`. O
comando passa pelas mesmas regras e pelo mesmo caso de uso do HTTP. A identidade é conferida antes de qualquer leitura
do corpo; depois vêm o envelope, a validação do comando e a decisão.

**Resultado de cada mensagem.**

| Resultado | Ação na fila (`FailureReason` / `FailureCode` na DLQ) | Testes |
|---|---|---|
| `PROCESSED`, `REJECTED` ou `PENDING_REFERENCE` comitado | `DeleteMessage` depois do commit | `TestConsumer_ProcessesBet`, `TestConsumer_RejectionIsTerminal`, `TestConsumer_PendingReferenceCompletesMessage` |
| duplicata (inbox com o mesmo hash) ou replay pela chave | `DeleteMessage` e métrica de duplicata | `TestConsumer_DuplicateDelivery`, `TestConsumer_SameKeyDifferentMessageIDs` |
| sem `ProviderToken` | DLQ `UNAUTHENTICATED` / `MISSING_PROVIDER_TOKEN` | `TestTokenAttributeIdentity`, `TestConsumer_Outcomes` |
| token inválido, expirado no `SentTimestamp` ou com papéis mal configurados | DLQ `UNAUTHENTICATED` / `INVALID_PROVIDER_TOKEN` | `TestTokenAttributeIdentity`, `TestConsumer_Outcomes` |
| token válido que não é de provedor com `wagering:write` | DLQ `FORBIDDEN` / `NOT_PROVIDER` | `TestTokenAttributeIdentity`, `TestConsumer_Outcomes` |
| `data.providerId` bem formado de outro provedor | DLQ `PROVIDER_IDENTITY_MISMATCH`, antes de qualquer outro problema do comando | `TestConsumer_ProviderIdentityMismatchToDLQ`, `TestConsumer_Outcomes` |
| envelope inválido, tipo desconhecido ou comando inválido | DLQ `INVALID_MESSAGE` / o código que o HTTP daria (`INVALID_REQUEST`, `INVALID_KIND`…) | `TestConsumer_InvalidMessageToDLQ`, `TestParseEnvelope_Invalid`, `TestApp_MovesInvalidMessagesToTheInputDLQ` |
| mesmo `messageId` com hash diferente | DLQ `MESSAGE_ID_REUSED` | `TestConsumer_MessageIDReusedToDLQ` |
| chave reusada, id externo duplicado, carteira inexistente | DLQ `IDEMPOTENCY_KEY_REUSED`, `DUPLICATE_EXTERNAL_TRANSACTION` ou `WALLET_NOT_FOUND` | `TestConsumer_Outcomes` |
| erro do caso de uso classificado como falha permanente (bug) | DLQ `PROCESSING_FAILED` | `TestConsumer_Outcomes` |
| falha transitória | fica na fila com backoff; esgotada, DLQ `RETRIES_EXHAUSTED` | `TestConsumer_TransientRetry`, `TestConsumer_RetriesExhaustedToDLQ` |
| mensagem atrás de uma que ficou na fila, no mesmo grupo | devolvida na hora (visibilidade 0), sem tentativa | `TestConsumer_GroupStopsAtFirstRetry` |
| SIGTERM | o que está em andamento termina em até `CONSUMER_DRAIN_TIMEOUT`; o resto é devolvido à fila | `TestConsumer_StopDrainsThenGivesBackUnstarted`, `TestConsumer_StopCancelsProcessingPastDrainTimeout`, `TestE2E_SIGTERMUnderLoadThroughNginx` |

**O que é transitório (nova tentativa).** Tudo o que não é um veredito sobre a mensagem: banco indisponível, contenção
que esgotou as tentativas, prazo de processamento esgotado, verificador de tokens indisponível, token assinado com um
`kid` que o verificador ainda não tem (numa rotação o IdP passa a assinar antes de o JWKS em cache ter a chave nova),
`SentTimestamp` ausente (o consumidor não pediu o atributo: erro de configuração), um veredito sobre o token dado depois
de o prazo da mensagem acabar (a busca de chaves pode ter sido cortada), um `panic` (logado com a pilha), um erro que
ninguém classificou e a falha ao mover para a DLQ ou ao apagar. Uma mensagem sem `MessageGroupId`, que uma fila FIFO
nunca entrega, não pode ir para a DLQ e só é tentada de novo. Testes: `TestConsumer_Outcomes`,
`TestTokenAttributeIdentity_UnfetchedKeyIsTransient`, `TestTokenAttributeIdentity_VerifierFailures`,
`TestConsumer_IdentityVerdictOnCanceledContextIsRetried`, `TestConsumer_PanicStackIsLoggedOnce`,
`TestConsumer_DeadLetterMoveFailureRetries`, `TestConsumer_DeleteFailureEndsGroup`.

**Backoff e `RETRIES_EXHAUSTED`.** Depois do *n*-ésimo recebimento, a mensagem fica oculta por `min(2^n s, 300s)`. Se
ela falha de novo quando já foi recebida `CONSUMER_MAX_RECEIVES` vezes (10), o consumidor a move para a DLQ com
`RETRIES_EXHAUSTED`: cerca de 15 min de tentativas, então um erro sem classificação custa no máximo isso. O
`maxReceiveCount` 30 do redrive é só a rede de segurança acima desse limite: o FIFO entrega o grupo inteiro, e as
mensagens atrás de uma cabeça que falha herdam o receive count dela sem terem sido tentadas; contado pelo consumidor,
cada uma ainda recebe uma tentativa real. Testes: `TestConsumer_DeadLettersOnceRetriesAreExhausted`,
`TestConsumer_InflatedFollowerGetsARealAttempt`, `TestConsumer_RedriveAfterMaxReceives`.

**Queda do banco não é veredito.** Quando uma mensagem falha com o banco indisponível e um ping também falha, o
consumidor pausa o polling e pinga a cada 2s até o banco responder; enquanto isso, as mensagens ficam na fila sem gastar
receive count, e o limite de `RETRIES_EXHAUSTED` não se aplica a elas. Se o ping responde, só aquela mensagem é refeita
(o problema era dela, como um lock disputado). Testes: `TestConsumer_CircuitPausesPollingUntilDatabaseAnswers`,
`TestConsumer_UnavailableWithDatabaseAnsweringRetriesOnlyThatMessage`,
`TestConsumer_UnavailableWithDatabaseDownOpensCircuit`, `TestE2E_PostgresAndSQSOutage`.

**Ordem por carteira.** O `MessageGroupId` é o `walletId`: mensagens da mesma carteira são processadas em série, na
ordem de envio, e carteiras diferentes em paralelo. Uma mensagem que fica na fila encerra o grupo naquela entrega, e as
seguintes são devolvidas sem tentativa, para não passarem à frente dela (`TestConsumer_GroupStopsAtFirstRetry`).

**A cópia na DLQ.** Leva o mesmo corpo e o mesmo `MessageGroupId`, os atributos `FailureReason`, `SourceMessageId`,
`FailureCode` (quando o motivo tem um) e o `correlationId` do produtor, e nunca o `ProviderToken`: credencial não mora
na DLQ. Os motivos são sanitizados (caracteres de controle e UTF-8 inválido viram `?`, até 256 bytes). O redrive é
manual e passa pelo provedor: sem o token, uma cópia devolvida à entrada seria recusada de novo, e quem reprocessa é o
provedor, reenviando a mensagem com um token novo. Testes: `TestDLQMover_KeepsCorrelationButNotToken`,
`TestDLQMover_SanitizesReasonAndCode`, `TestDLQMover_SendsThenDeletes`, `TestDLQMover_KeepsMessageWhenSendFails`.

## 11. Eventos

Os eventos vão para `wallet-events.fifo`. São gravados na outbox no mesmo commit da mudança que os originou e nunca
publicados antes dele: um rollback leva o evento junto (`TestSubmit_RollbackOnFailure_NoOutbox`).

| Evento | Quando | `data` |
|---|---|---|
| `WagerTransactionProcessed` | uma transação chega a `PROCESSED`, inclusive `LOSS` e `OPENING` | `transactionId`, `walletId`, `playerId`, `kind`, `money`, `balanceAfter`, `walletVersion`; `providerId`, `externalTransactionId`, `roundId`, `gameId` e `referenceTransactionId` (o id interno da referência) quando se aplicam, todos ausentes na `OPENING` |
| `WalletBalanceChanged` | todo lançamento no ledger | `walletId`, `transactionId`, `direction`, `money` (o valor movido, sempre positivo), `balanceBefore`, `balanceAfter`, `walletVersion` |
| `WagerTransactionRejected` | uma transação chega a `REJECTED`, na hora ou ao fim da espera pela referência | `transactionId`, `providerId`, `externalTransactionId`, `walletId`, `kind`, `money`, `failureCode` (o mesmo devolvido ao provedor) |
| `WagerTransactionPendingReference` | uma transação entra em `PENDING_REFERENCE` | `transactionId`, `providerId`, `externalTransactionId`, `walletId`, `kind`, `referenceExternalTransactionId`, `nextAttemptAt`, `expiresAt` |

| Resultado | Eventos | Testes |
|---|---|---|
| `PROCESSED` com movimento | `WagerTransactionProcessed` e depois `WalletBalanceChanged` | `TestSubmit_BetProcessed` |
| `LOSS` | só `WagerTransactionProcessed` | `TestSubmit_LossKeepsVersion` |
| abertura com saldo positivo | `WagerTransactionProcessed` (`OPENING`) e `WalletBalanceChanged` | `TestOpen_PositiveIsAtomic` |
| abertura com saldo zero | nenhum | `TestOpen_ZeroCreatesNothingFinancial` |
| `REJECTED` | `WagerTransactionRejected` | `TestSubmit_RejectionPersistsAndReplays`, `TestSubmit_OwnershipRejectsWithoutBalance` |
| entrada em espera | `WagerTransactionPendingReference`, uma vez | `TestSubmit_RefundBeforeBetIsPending` |
| busca que reagenda | nenhum: a espera foi anunciada quando começou | `TestResume_ReschedulesWithBackoff` |
| `FAILED` | nenhum (o log ERROR e a métrica avisam a operação) | `TestResume_PermanentFailureMarksFailedAtLimit` |
| replay, duplicata, requisição recusada | nenhum | `TestSubmit_ReplaySameKey` |

**Envelope.** `eventId` (UUIDv7), `eventType`, `aggregateId` (= `walletId` nos quatro eventos), `correlationId`,
`causationId` (opcional), `occurredAt`, `version` (1 em todos hoje) e `data`. O formato está em
[ARCHITECTURE.md §12](../ARCHITECTURE.md#12-outbox-e-eventos).

- `occurredAt` é o instante da decisão, em UTC com milissegundos (`2026-09-08T12:00:00.123Z`), truncado, nunca
  arredondado para depois. Dinheiro sai como `{"amount":"25.00","currency":"BRL"}`, e um valor zero nunca é omitido.
- `correlationId` é o da requisição ou mensagem que criou a transação (`X-Correlation-Id`, o atributo `correlationId` da
  mensagem, ou um gerado/o `messageId` na falta deles), e também vai nos eventos que o resolvedor emite depois para ela.
  `causationId` é o `messageId` no SQS e o id da própria transação nos eventos do resolvedor; no HTTP e na abertura de
  carteira, é omitido.
- O `eventId` é estável: um evento republicado (por exemplo, depois de um crash entre a publicação e o ack) sai com o
  mesmo `eventId`. A entrega é at-least-once; o broker descarta o reenvio pelo `MessageDeduplicationId` (= `eventId`)
  dentro de 5 minutos, e os consumidores deduplicam pelo `eventId` depois disso.
- Ordem por carteira: `MessageGroupId` = `walletId`, e o relay só publica o evento pendente mais antigo de cada
  carteira. `walletVersion` cresce com cada mudança de saldo, e o consumidor descarta um evento com `walletVersion`
  menor que o que já aplicou. Um evento que falha sempre segura os seguintes da mesma carteira; nenhum evento é
  descartado.
- Evolução: uma versão nova é `version + 1`, e campos só são acrescentados.

Testes: `TestWalletBalanceChanged_Golden`, `TestConstructors_SetTypeAndVersion`,
`TestConstructors_NormalizeOccurredAtToUTC`, `TestTimestamp_UTCMillis`,
`TestProcessedData_OmitsExternalFieldsForOpening`, `TestData_ZeroMoneyIsNotOmitted`, `TestRejectedData_JSONFieldNames`,
`TestPendingReferenceData_JSONFieldNames`, `TestSubmit_GeneratesCorrelationIDWhenMissing`,
`TestRelay_PublishesInAggregateOrder`, `TestOutbox_ClaimHeadsOnePerAggregate`, `TestRelay_RepublishKeepsEventID`,
`TestE2E_OutboxCrashAfterPublishBeforeAck`, `TestApp_ConsumesAndPublishes`.
