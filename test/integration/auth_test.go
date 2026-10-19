//go:build integration

package integration

import (
	"bytes"
	"encoding/base64"
	"maps"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAuth_MissingInvalidExpiredToken(t *testing.T) {
	const skew = time.Second
	svc := startService(t, map[string]string{"OIDC_CLOCK_SKEW": skew.String()})
	internal := token(t, walletService, walletServiceSecret)
	provider := token(t, providerA, providerASecret)
	w := svc.openWallet(t, internal)
	bet := svc.placeBet(t, provider, providerA, w)
	short := shortLivedToken(t, skew)
	svc.send(t, bet.byIDCall(short)).want(t, http.StatusOK)

	routes := append(walletCalls(internal, w),
		betCall(provider, providerA, w), bet.byIDCall(provider), bet.byExternalCall(provider, providerA))
	before := takeFootprint(t, w)

	credentials := []struct {
		name          string
		authorization string
	}{
		{"no Authorization header", ""},
		{"client credentials instead of a token", "Basic " + base64.StdEncoding.EncodeToString([]byte(providerA+":"+providerASecret))},
		{"Bearer without a token", "Bearer"},
		{"not a JWT", bearer("not-a-jwt")},
		{"unsigned", bearer(unsigned(t, provider))},
		{"claims edited after signing", bearer(tampered(t, provider, "provider_id", providerB))},
		{"signed by a foreign key under Keycloak's key id", bearer(resigned(t, provider, keyIDOf(t, provider)))},
		{"signed by a foreign key under an unknown key id", bearer(resigned(t, provider, "unknown-"+newID()))},
		{"issued by Keycloak for another audience", bearer(otherAudienceToken(t, providerA))},
	}
	for _, cred := range credentials {
		t.Run(cred.name, func(t *testing.T) {
			for _, c := range routes {
				c.authorization = cred.authorization
				wantUnauthenticated(t, svc.send(t, c))
			}
		})
	}
	t.Run("expired", func(t *testing.T) {
		waitForExpiry(t, svc, bet.byIDCall(short), skew)
		for _, c := range routes {
			wantUnauthenticated(t, svc.send(t, c.as(short)))
		}
	})

	wantUnchanged(t, w, before)
}

func wantUnauthenticated(t *testing.T, r reply) {
	t.Helper()
	r.wantProblem(t, http.StatusUnauthorized, codeUnauthenticated)
	if challenge := r.header.Get("WWW-Authenticate"); !strings.HasPrefix(challenge, "Bearer ") {
		t.Errorf("%s: WWW-Authenticate = %q, want a Bearer challenge", r.call, challenge)
	}
}

func shortLivedToken(t *testing.T, skew time.Duration) string {
	t.Helper()
	tok := token(t, providerAShort, providerAShortSecret)
	c := claimsOf(t, tok)
	if now, expiry := time.Now(), time.Unix(c.Expiry, 0); !expiry.Add(skew).After(now) {
		t.Fatalf("the short-lived token expired at %s, and the service's leeway of %v ran out before it arrived at %s: "+
			"Keycloak clock is behind or its token endpoint slow to answer",
			expiry.UTC().Format(time.RFC3339), skew, now.UTC().Format(time.RFC3339))
	}
	return tok
}

func waitForExpiry(t *testing.T, svc *service, probe call, skew time.Duration) {
	t.Helper()
	tok := strings.TrimPrefix(probe.authorization, "Bearer ")
	expiry := time.Unix(claimsOf(t, tok).Expiry, 0)
	if lifetime := time.Until(expiry); lifetime > 10*time.Second {
		t.Fatalf("token expires in %v, want a short-lived one; is Keycloak's clock ahead of the test's?", lifetime)
	}
	time.Sleep(time.Until(expiry.Add(skew)))
	deadline := expiry.Add(skew + 3*time.Second)
	for {
		r := svc.send(t, probe)
		switch {
		case r.status == http.StatusUnauthorized:
			return
		case r.status != http.StatusOK:
			t.Fatalf("%s = %d %s while waiting for the token to expire", probe, r.status, r.body)
		case time.Now().After(deadline):
			t.Fatalf("token still accepted %v after its expiry, with a tolerance of %v", time.Since(expiry), skew)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestAuthz_BodyProviderMismatch(t *testing.T) {
	svc := startService(t, nil)
	internal := token(t, walletService, walletServiceSecret)
	tokens := map[string]string{
		providerA: token(t, providerA, providerASecret),
		providerB: token(t, providerB, providerBSecret),
	}
	w := svc.openWallet(t, internal)

	tests := []struct{ caller, claimed string }{
		{caller: providerA, claimed: providerB},
		{caller: providerB, claimed: providerA},
	}
	for _, tc := range tests {
		t.Run(tc.caller+" claiming "+tc.claimed, func(t *testing.T) {
			before := takeFootprint(t, w)
			c := betCall(tokens[tc.caller], tc.claimed, w)

			svc.send(t, c).wantProblem(t, http.StatusForbidden, codeProviderMismatch)

			wantUnchanged(t, w, before)
			for provider, tok := range tokens {
				lookup := call{
					method: http.MethodGet,
					path:   "/providers/" + provider + "/wagering/transactions/" + c.externalID(),
				}
				svc.send(t, lookup.as(tok)).wantProblem(t, http.StatusNotFound, codeTransactionNotFound)
			}
			corrected := c
			corrected.body = withProvider(c.body, tc.caller)
			if got := svc.send(t, corrected).want(t, http.StatusCreated); got["idempotentReplay"] != false {
				t.Errorf("%s with the refused request's key = %v, want a first submission", corrected, got)
			}
		})
	}
}

func withProvider(body any, providerID string) map[string]any {
	b, _ := body.(map[string]any)
	b = maps.Clone(b)
	b["providerId"] = providerID
	return b
}

func TestAuthz_ProviderIsolation(t *testing.T) {
	svc := startService(t, nil)
	internal := token(t, walletService, walletServiceSecret)
	a := token(t, providerA, providerASecret)
	b := token(t, providerB, providerBSecret)
	w := svc.openWallet(t, internal)
	bet := svc.placeBet(t, a, providerA, w)
	before := takeFootprint(t, w)

	t.Run("read by id answers as for an id that does not exist", func(t *testing.T) {
		other := svc.send(t, bet.byIDCall(b)).wantProblem(t, http.StatusNotFound, codeTransactionNotFound)
		missing := call{method: http.MethodGet, path: "/wagering/transactions/" + newID()}
		none := svc.send(t, missing.as(b)).wantProblem(t, http.StatusNotFound, codeTransactionNotFound)

		delete(other, "correlationId")
		delete(none, "correlationId")
		if !reflect.DeepEqual(other, none) {
			t.Errorf("another provider's transaction = %v, a missing one = %v, want the same answer", other, none)
		}
	})
	t.Run("replaying the other provider's exact request is refused", func(t *testing.T) {
		r := svc.send(t, bet.call.as(b))

		r.wantProblem(t, http.StatusForbidden, codeProviderMismatch)
		if bytes.Contains(r.body, []byte(bet.id)) {
			t.Errorf("%s reveals the original transaction: %s", r.call, r.body)
		}
	})
	t.Run("read by external id under the other provider's name is refused", func(t *testing.T) {
		svc.send(t, bet.byExternalCall(b, providerA)).wantProblem(t, http.StatusForbidden, codeProviderMismatch)
	})
	t.Run("read by external id under its own name finds nothing", func(t *testing.T) {
		svc.send(t, bet.byExternalCall(b, providerB)).wantProblem(t, http.StatusNotFound, codeTransactionNotFound)
	})
	wantUnchanged(t, w, before)

	t.Run("the same key in its own name is a transaction of its own", func(t *testing.T) {
		own := bet.call.as(b)
		own.body = withProvider(bet.call.body, providerB)

		got := svc.send(t, own).want(t, http.StatusCreated)

		if got["transactionId"] == bet.id || got["idempotentReplay"] != false {
			t.Errorf("%s with provider-a's key and external id = %v, want a new transaction of provider-b", own, got)
		}
		if mine := svc.send(t, bet.byIDCall(a)).want(t, http.StatusOK); mine["providerId"] != providerA {
			t.Errorf("provider-a's transaction now reads %v, want it untouched", mine)
		}
	})
}

func TestAuthz_CrossProviderReversal(t *testing.T) {
	svc := startService(t, nil)
	internal := token(t, walletService, walletServiceSecret)
	a := token(t, providerA, providerASecret)
	b := token(t, providerB, providerBSecret)
	w := svc.openWallet(t, internal)
	bet := svc.placeBet(t, a, providerA, w)
	reads := []call{
		bet.byIDCall(internal),
		{method: http.MethodGet, path: "/wallets/" + w.id, authorization: bearer(internal)},
		{method: http.MethodGet, path: "/wallets/" + w.id + "/ledger", authorization: bearer(internal)},
	}
	readAll := func(t *testing.T) []map[string]any {
		t.Helper()
		got := make([]map[string]any, len(reads))
		for i, c := range reads {
			got[i] = svc.send(t, c).want(t, http.StatusOK)
		}
		return got
	}
	before, footprintBefore := readAll(t), takeFootprint(t, w)

	type sent struct{ kind, id string }
	var reversals []sent
	for _, kind := range []string{"ROLLBACK", "REFUND"} {
		t.Run(kind+" is left waiting", func(t *testing.T) {
			got := svc.send(t, bet.reversalCall(b, providerB, kind)).want(t, http.StatusAccepted)

			id, _ := got["transactionId"].(string)
			if got["status"] != "PENDING_REFERENCE" || id == "" {
				t.Fatalf("provider-b's %s of provider-a's bet = %v, want PENDING_REFERENCE", kind, got)
			}
			reversals = append(reversals, sent{kind, id})
		})
	}
	if len(reversals) != 2 {
		t.Fatalf("%d of provider-b's 2 reversals were left waiting, so there is nothing to follow; see the failures above", len(reversals))
	}

	type reversal struct {
		Status                 string  `json:"status"`
		FailureCode            string  `json:"failureCode"`
		ReferenceTransactionID *string `json:"referenceTransactionId"`
	}
	deadline := time.Now().Add(20 * time.Second)
	for _, r := range reversals {
		read := call{method: http.MethodGet, path: "/wagering/transactions/" + r.id, authorization: bearer(b)}
		var got reversal
		for {
			got = reversal{}
			svc.send(t, read).decode(t, http.StatusOK, &got)
			if got.Status != "PENDING_REFERENCE" || got.ReferenceTransactionID != nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if got.Status != "REJECTED" || got.FailureCode != "REFERENCE_NOT_FOUND" || got.ReferenceTransactionID != nil {
			t.Errorf("provider-b's %s = %+v, want it REJECTED with REFERENCE_NOT_FOUND, bound to no transaction", r.kind, got)
		}
	}

	if after := readAll(t); !reflect.DeepEqual(after, before) {
		t.Errorf("provider-a's bet, the wallet and its ledger =\n%v\nwant them as before provider-b's reversals,\n%v", after, before)
	}
	footprintAfter := takeFootprint(t, w)
	if footprintAfter.wallet != footprintBefore.wallet || footprintAfter.ledgerEntries != footprintBefore.ledgerEntries {
		t.Errorf("wallet row or ledger changed:\n before %+v\n after  %+v", footprintBefore, footprintAfter)
	}
	if got, want := footprintAfter.transactions, footprintBefore.transactions+int64(len(reversals)); got != want {
		t.Errorf("transactions = %d, want %d: provider-b's reversals and nothing else", got, want)
	}

	t.Run("provider-a still refunds its bet", func(t *testing.T) {
		got := svc.send(t, bet.reversalCall(a, providerA, "REFUND")).want(t, http.StatusCreated)

		balance, _ := got["balance"].(map[string]any)
		if got["status"] != "PROCESSED" || balance["amount"] != "100.00" {
			t.Errorf("provider-a's refund of its own bet = %v, want PROCESSED with the 100.00 the wallet opened with", got)
		}
	})
}

func TestAuthz_WalletEndpointsInternalOnly(t *testing.T) {
	svc := startService(t, nil)
	internal := token(t, walletService, walletServiceSecret)
	a := token(t, providerA, providerASecret)
	b := token(t, providerB, providerBSecret)
	w := svc.openWallet(t, internal)
	svc.placeBet(t, a, providerA, w)
	before := takeFootprint(t, w)

	for name, tok := range map[string]string{providerA: a, providerB: b} {
		t.Run(name, func(t *testing.T) {
			for _, c := range walletCalls(tok, w) {
				svc.send(t, c).wantProblem(t, http.StatusForbidden, codeForbidden)
			}
		})
	}
	t.Run("back office betting", func(t *testing.T) {
		svc.send(t, betCall(internal, providerA, w)).wantProblem(t, http.StatusForbidden, codeForbidden)
	})
	t.Run("provider client also granted wallets:manage", func(t *testing.T) {
		tok := providerClientToken(t, providerA, scopeWageringWrite, scopeWageringRead, scopeWalletsManage)
		for _, c := range walletCalls(tok, w) {
			wantUnauthenticated(t, svc.send(t, c))
		}
	})
	wantUnchanged(t, w, before)

	t.Run("the back office reads the wallet", func(t *testing.T) {
		svc.send(t, call{method: http.MethodGet, path: "/wallets/" + w.id}.as(internal)).want(t, http.StatusOK)
	})
}

func TestAuthz_ScopeRequired(t *testing.T) {
	svc := startService(t, nil)
	internal := token(t, walletService, walletServiceSecret)
	w := svc.openWallet(t, internal)
	bet := svc.placeBet(t, token(t, providerA, providerASecret), providerA, w)
	readOnly := providerClientToken(t, providerA, scopeWageringRead)
	writeOnly := providerClientToken(t, providerA, scopeWageringWrite)
	before := takeFootprint(t, w)

	t.Run("submit without wagering:write", func(t *testing.T) {
		svc.send(t, betCall(readOnly, providerA, w)).wantProblem(t, http.StatusForbidden, codeForbidden)
		svc.send(t, bet.byIDCall(readOnly)).want(t, http.StatusOK)
	})
	t.Run("read without wagering:read", func(t *testing.T) {
		svc.send(t, bet.byIDCall(writeOnly)).wantProblem(t, http.StatusForbidden, codeForbidden)
		svc.send(t, bet.byExternalCall(writeOnly, providerA)).wantProblem(t, http.StatusForbidden, codeForbidden)
	})

	wantUnchanged(t, w, before)
}

func TestAuthz_NoSideEffects(t *testing.T) {
	svc := startService(t, nil)
	internal := token(t, walletService, walletServiceSecret)
	a := token(t, providerA, providerASecret)
	b := token(t, providerB, providerBSecret)
	readOnly := providerClientToken(t, providerA, scopeWageringRead)
	forged := resigned(t, a, keyIDOf(t, a))
	w := svc.openWallet(t, internal)
	bet := svc.placeBet(t, a, providerA, w)
	walletRead := call{method: http.MethodGet, path: "/wallets/" + w.id}.as(internal)
	ledgerRead := call{method: http.MethodGet, path: "/wallets/" + w.id + "/ledger"}.as(internal)
	walletBefore := svc.send(t, walletRead).want(t, http.StatusOK)
	ledgerBefore := svc.send(t, ledgerRead).want(t, http.StatusOK)
	before := takeFootprint(t, w)

	type refusal struct {
		name   string
		call   call
		status int
		code   string
	}
	noToken := betCall(a, providerA, w)
	noToken.authorization = ""
	refused := []refusal{
		{"bet without a token", noToken, http.StatusUnauthorized, codeUnauthenticated},
		{"bet with a forged token", betCall(forged, providerA, w), http.StatusUnauthorized, codeUnauthenticated},
		{"bet by the back office", betCall(internal, providerA, w), http.StatusForbidden, codeForbidden},
		{"bet without wagering:write", betCall(readOnly, providerA, w), http.StatusForbidden, codeForbidden},
		{"bet in another provider's name", betCall(b, providerA, w), http.StatusForbidden, codeProviderMismatch},
		{"replay of another provider's bet", bet.call.as(b), http.StatusForbidden, codeProviderMismatch},
		{"read of another provider's transaction", bet.byIDCall(b), http.StatusNotFound, codeTransactionNotFound},
		{"read under another provider's name", bet.byExternalCall(b, providerA), http.StatusForbidden, codeProviderMismatch},
	}
	route := func(c call) string { return strings.Replace(c.String(), w.id, "{walletId}", 1) }
	for _, c := range walletCalls(a, w) {
		refused = append(refused, refusal{"provider " + route(c), c, http.StatusForbidden, codeForbidden})
	}
	for _, c := range walletCalls(forged, w) {
		refused = append(refused, refusal{"forged token " + route(c), c, http.StatusUnauthorized, codeUnauthenticated})
	}
	for _, r := range refused {
		t.Run(r.name, func(t *testing.T) {
			svc.send(t, r.call).wantProblem(t, r.status, r.code)
		})
	}

	wantUnchanged(t, w, before)
	if got := svc.send(t, walletRead).want(t, http.StatusOK); !reflect.DeepEqual(got, walletBefore) {
		t.Errorf("wallet = %v, want it as before the refused requests, %v", got, walletBefore)
	}
	if got := svc.send(t, ledgerRead).want(t, http.StatusOK); !reflect.DeepEqual(got, ledgerBefore) {
		t.Errorf("ledger = %v, want it as before the refused requests, %v", got, ledgerBefore)
	}
	reconcile := call{method: http.MethodPost, path: "/wallets/" + w.id + "/reconciliation"}.as(internal)
	if got := svc.send(t, reconcile).want(t, http.StatusOK); got["consistent"] != true {
		t.Errorf("reconciliation = %v, want the wallet consistent with its ledger", got)
	}
}

func TestAuth_InternalReadsAnyTransaction(t *testing.T) {
	svc := startService(t, nil)
	internal := token(t, walletService, walletServiceSecret)
	w := svc.openWallet(t, internal)
	bets := map[string]placedBet{
		providerA: svc.placeBet(t, token(t, providerA, providerASecret), providerA, w),
		providerB: svc.placeBet(t, token(t, providerB, providerBSecret), providerB, w),
	}

	for provider, bet := range bets {
		t.Run(provider, func(t *testing.T) {
			for _, c := range []call{bet.byIDCall(internal), bet.byExternalCall(internal, provider)} {
				got := svc.send(t, c).want(t, http.StatusOK)

				if got["transactionId"] != bet.id || got["providerId"] != provider ||
					got["externalTransactionId"] != bet.call.externalID() || got["status"] != "PROCESSED" {
					t.Errorf("%s = %v, want %s's transaction %s, PROCESSED", c, got, provider, bet.id)
				}
			}
		})
	}
}
