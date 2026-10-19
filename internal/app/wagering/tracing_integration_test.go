//go:build integration

package wagering_test

import (
	"testing"

	"github.com/Brunohvc/backend-challenge-go/internal/app/wagering"
	"github.com/Brunohvc/backend-challenge-go/internal/domain/wager"
	"github.com/Brunohvc/backend-challenge-go/internal/platform/tracing/tracingtest"
)

func TestResume_TracesTheResumption(t *testing.T) {
	tr, rec := tracingtest.New(t)
	h := newHarness(t, withTracer(tr))
	w := openWallet(t, h.uow, "100.00")
	betIn := input(w, "BET", "25.00")
	refund := submit(t, h.svc, waitingFor(w, "REFUND", "25.00", betIn.ExternalTransactionID)).Transaction
	submit(t, h.svc, betIn)
	due := claimOne(t, h, refund)
	ctx, root := tracingtest.Root(t.Context(), tr)

	outcome, err := h.svc.Resume(ctx, due)
	root.End()

	if err != nil || outcome != wagering.ResumeProcessed {
		t.Fatalf("Resume() = %q, %v; want processed", outcome, err)
	}
	span := tracingtest.One(t, rec, "wagering.resume")
	if !tracingtest.ChildOf(span, tracingtest.One(t, rec, "test")) {
		t.Error("wagering.resume is not under the span of the caller")
	}
	want := map[string]string{
		"wagering.channel":        "resolver",
		"wagering.kind":           "REFUND",
		"wagering.status":         string(wager.StatusProcessed),
		"wagering.resume_outcome": string(wagering.ResumeProcessed),
		"wagering.provider_id":    refund.ProviderID,
		"wagering.transaction_id": refund.ID.String(),
		"wallet.id":               w.ID().String(),
	}
	attrs := tracingtest.Attributes(span)
	for key, value := range want {
		if attrs[key] != value {
			t.Errorf("%s = %q, want %q", key, attrs[key], value)
		}
	}
	requireNoAmount(t, span, "25.00", "100.00", "75.00")
	units := tracingtest.Named(rec, "uow.do")
	if len(units) != 1 || !tracingtest.ChildOf(units[0], span) {
		t.Errorf("%d units of work, want the one of the resumption under its span", len(units))
	}
}
