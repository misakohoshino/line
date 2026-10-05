package connector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/highesttt/matrix-line-messenger/pkg/e2ee"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// setReceiveStatusClock pins the receive-status clock; advance moves it.
func setReceiveStatusClock(t *testing.T, start time.Time) (advance func(time.Duration)) {
	t.Helper()
	old := receiveStatusNow
	now := start
	receiveStatusNow = func() time.Time { return now }
	t.Cleanup(func() { receiveStatusNow = old })
	return func(d time.Duration) { now = now.Add(d) }
}

func (h *divaControlHarness) getStatus(t *testing.T, token string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, h.srv.URL+"/diva/v1/status", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET status: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

func (h *divaControlHarness) status(t *testing.T) divaStatusResponse {
	t.Helper()
	code, data := h.getStatus(t, divaTestToken)
	if code != http.StatusOK {
		t.Fatalf("status code = %d body=%s", code, data)
	}
	var out divaStatusResponse
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("status is not JSON: %s", data)
	}
	return out
}

func strOrNil(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func TestDIVAStatusRequiresAuth(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	for name, token := range map[string]string{"none": "", "wrong": divaTestToken + "x", "prefix": divaTestToken[:10]} {
		code, data := h.getStatus(t, token)
		var got outboundResult
		_ = json.Unmarshal(data, &got)
		if code != http.StatusUnauthorized || got.Error == nil || got.Error.Code != outboundUnauthorized {
			t.Fatalf("%s: status=%d body=%s", name, code, data)
		}
		if strings.Contains(string(data), "logged_in") {
			t.Fatalf("%s: unauthorized response leaked status: %s", name, data)
		}
	}
	req, _ := http.NewRequest(http.MethodGet, h.srv.URL+"/diva/v1/status", nil)
	req.Header.Set("Authorization", "Basic "+divaTestToken)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Basic auth status = %d", resp.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodPost, h.srv.URL+"/diva/v1/status", nil)
	req.Header.Set("Authorization", "Bearer "+divaTestToken)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", resp.StatusCode)
	}
	if code, _ := h.getStatus(t, divaTestToken); code != http.StatusOK {
		t.Fatalf("valid token status = %d", code)
	}
}

func TestDIVAHealthContractUnchanged(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	h.env.lc.receiveStatus.recordSSEFailure()
	resp, err := http.Get(h.srv.URL + "/diva/v1/health")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(data) != "{\"ok\":true}\n" {
		t.Fatalf("health = %d %q", resp.StatusCode, data)
	}
}

func TestDIVAStatusConnectedWithRecentAuthProbe(t *testing.T) {
	start := time.Date(2026, 10, 5, 1, 2, 3, 0, time.UTC)
	advance := setReceiveStatusClock(t, start)
	h := newDIVAControlHarness(t, divaControlConfig{})
	lc := h.env.lc

	lc.sendBridgeState(status.BridgeState{StateEvent: status.StateConnected})
	advance(time.Second)
	lc.receiveStatus.recordAuthProbe(true)
	advance(time.Second)
	lc.receiveStatus.recordSSEEvent()
	advance(time.Second)

	code, raw := h.getStatus(t, divaTestToken)
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	for _, secret := range []string{sendTestSelf, "access-token"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("status leaked %q: %s", secret, raw)
		}
	}
	var got divaStatusResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Now != "2026-10-05T01:02:06Z" || got.LoginCount != 1 || len(got.Logins) != 1 {
		t.Fatalf("status = %s", raw)
	}
	l := got.Logins[0]
	if l.Index != 0 || !l.LoggedIn || l.SessionInvalidated {
		t.Fatalf("login = %+v", l)
	}
	if l.LastBridgeState == nil || l.LastBridgeState.StateEvent != "CONNECTED" || l.LastBridgeState.ErrorCode != nil ||
		l.LastBridgeState.At != "2026-10-05T01:02:03Z" {
		t.Fatalf("bridge state = %+v", l.LastBridgeState)
	}
	if strOrNil(l.LastAuthProbeOKAt) != "2026-10-05T01:02:04Z" || l.LastAuthProbeFailedAt != nil || l.ConsecutiveAuthProbeFailures != 0 {
		t.Fatalf("auth probe = %s", raw)
	}
	if strOrNil(l.LastSSEEventAt) != "2026-10-05T01:02:05Z" || l.ConsecutiveSSEFailures != 0 || l.SSEReconnecting || l.LastSSEFailureAt != nil {
		t.Fatalf("sse = %s", raw)
	}
}

func TestDIVAStatusEmptyWhenNothingRecorded(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	_, raw := h.getStatus(t, divaTestToken)
	for _, field := range []string{`"last_bridge_state":null`, `"last_sse_event_at":null`, `"last_auth_probe_ok_at":null`} {
		if !strings.Contains(string(raw), field) {
			t.Fatalf("status missing %s: %s", field, raw)
		}
	}
	none := newDIVAControlHarness(t, divaControlConfig{logins: func() []*bridgev2.UserLogin { return nil }})
	if _, raw := none.getStatus(t, divaTestToken); !strings.Contains(string(raw), `"login_count":0,"logins":[]`) {
		t.Fatalf("no-login status = %s", raw)
	}
}

func TestDIVAStatusShowsBadCredentialsCode(t *testing.T) {
	setReceiveStatusClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	h := newDIVAControlHarness(t, divaControlConfig{})
	lc := h.env.lc

	// Real connector call site: a missing E2EE key marks bad credentials.
	lc.markMissingE2EEKey(context.Background(), e2ee.ErrMissingOwnPrivateKey)
	l := h.status(t).Logins[0]
	if l.LastBridgeState == nil || l.LastBridgeState.StateEvent != "BAD_CREDENTIALS" ||
		strOrNil(l.LastBridgeState.ErrorCode) != "line-e2ee-key-missing" {
		t.Fatalf("bridge state = %+v", l.LastBridgeState)
	}

	// The human message (may carry a PIN or raw error) is never exposed.
	lc.sendBridgeState(status.BridgeState{
		StateEvent: status.StateBadCredentials,
		Error:      "line-login-failed",
		Message:    "login failed: secret-detail-123",
	})
	lc.invalidateAccessToken()
	code, raw := h.getStatus(t, divaTestToken)
	if code != http.StatusOK || strings.Contains(string(raw), "secret-detail-123") {
		t.Fatalf("status = %d %s", code, raw)
	}
	var got divaStatusResponse
	_ = json.Unmarshal(raw, &got)
	l = got.Logins[0]
	if l.LoggedIn || strOrNil(l.LastBridgeState.ErrorCode) != "line-login-failed" {
		t.Fatalf("login = %s", raw)
	}
}

func TestDIVAStatusSSEFailuresAndRecovery(t *testing.T) {
	setReceiveStatusClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	oldGetLastOpRevision := getLastOpRevisionWithClient
	oldListenSSE := listenSSEWithClient
	oldReconnectDelay := sseReconnectDelay
	t.Cleanup(func() {
		getLastOpRevisionWithClient = oldGetLastOpRevision
		listenSSEWithClient = oldListenSSE
		sseReconnectDelay = oldReconnectDelay
	})
	getLastOpRevisionWithClient = func(context.Context, *line.Client) (int64, error) { return 1, nil }
	sseReconnectDelay = time.Millisecond

	lc := &LineClient{
		AccessToken: "token",
		UserLogin:   &bridgev2.UserLogin{Bridge: &bridgev2.Bridge{Log: zerolog.New(io.Discard)}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts int
	var seen []lineReceiveStatusSnapshot
	listenSSEWithClient = func(_ *line.Client, _ context.Context, _ int64, handler func(eventType, data string)) error {
		attempts++
		seen = append(seen, lc.receiveStatus.snapshot())
		switch attempts {
		case 1, 2, 3:
			return errors.New("connection reset")
		case 4:
			// Connects with no events and closes: still a failed attempt.
			return io.EOF
		case 5:
			handler("ping", "{}")
			return io.EOF // normal close after events: not a failure
		default:
			cancel()
			return context.Canceled
		}
	}
	lc.wg.Add(1)
	lc.pollLoop(ctx)

	if attempts != 6 {
		t.Fatalf("attempts = %d", attempts)
	}
	for i, want := range []int{0, 1, 2, 3, 4, 0} {
		if seen[i].consecutiveSSEFailures != want {
			t.Fatalf("before attempt %d failures = %d, want %d", i+1, seen[i].consecutiveSSEFailures, want)
		}
	}
	snap := lc.receiveStatus.snapshot()
	if snap.lastSSEEventAt.IsZero() || snap.lastSSEFailureAt.IsZero() || snap.lastAuthProbeOKAt.IsZero() {
		t.Fatalf("snapshot = %+v", snap)
	}
}

func TestDIVAStatusAuthProbeFailureAndRecovery(t *testing.T) {
	advance := setReceiveStatusClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	oldGetLastOpRevision := getLastOpRevisionWithClient
	t.Cleanup(func() { getLastOpRevisionWithClient = oldGetLastOpRevision })

	probeErr := errors.New("network unreachable")
	getLastOpRevisionWithClient = func(context.Context, *line.Client) (int64, error) { return 0, probeErr }

	lc := &LineClient{
		AccessToken: "token",
		UserLogin:   &bridgev2.UserLogin{Bridge: &bridgev2.Bridge{Log: zerolog.New(io.Discard)}},
	}
	for i := 0; i < 2; i++ {
		if lc.handleReceiveAuthProbe(context.Background()) {
			t.Fatal("transient probe failure stopped the receive loop")
		}
		advance(time.Minute)
	}
	snap := lc.receiveStatus.snapshot()
	if snap.consecutiveAuthProbeFailures != 2 || snap.lastAuthProbeFailedAt.IsZero() || !snap.lastAuthProbeOKAt.IsZero() {
		t.Fatalf("after failures = %+v", snap)
	}

	probeErr = nil
	lc.handleReceiveAuthProbe(context.Background())
	snap = lc.receiveStatus.snapshot()
	if snap.consecutiveAuthProbeFailures != 0 || !snap.lastAuthProbeOKAt.After(snap.lastAuthProbeFailedAt) {
		t.Fatalf("after recovery = %+v", snap)
	}

	// A new CONNECTED state clears stale failure streaks.
	lc.receiveStatus.recordSSEFailure()
	lc.receiveStatus.recordAuthProbe(false)
	lc.sendBridgeState(status.BridgeState{StateEvent: status.StateConnected})
	snap = lc.receiveStatus.snapshot()
	if snap.consecutiveSSEFailures != 0 || snap.consecutiveAuthProbeFailures != 0 || snap.lastSSEFailureAt.IsZero() {
		t.Fatalf("after connected = %+v", snap)
	}
}

func TestDIVAStatusIsReadOnly(t *testing.T) {
	setReceiveStatusClock(t, time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	h := newDIVAControlHarness(t, divaControlConfig{
		send: func(context.Context, *LineClient, *lineOutboundRequest) (*lineOutboundResult, error) {
			t.Error("status triggered a LINE send")
			return nil, errors.New("unexpected")
		},
	})
	lc := h.env.lc
	lc.sendBridgeState(status.BridgeState{StateEvent: status.StateBadCredentials, Error: "line-token-expired"})
	lc.receiveStatus.recordSSEFailure()
	lc.receiveStatus.recordAuthProbe(false)
	before := lc.receiveStatus.snapshot()

	for i := 0; i < 3; i++ {
		h.status(t)
	}
	if after := lc.receiveStatus.snapshot(); after != before {
		t.Fatalf("status changed recorded state: %+v -> %+v", before, after)
	}
	if n := len(h.env.fake.snapshot()); n != 0 {
		t.Fatalf("LINE calls = %d", n)
	}
	if lc.getAccessToken() != "access-token" || lc.isSessionInvalidated() {
		t.Fatal("status changed session state")
	}
}

func TestDIVAStatusListsLoginsByOpaqueIndex(t *testing.T) {
	newLogin := func(id, token string) *bridgev2.UserLogin {
		lc := &LineClient{Mid: id, AccessToken: token}
		login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: networkid.UserLoginID(id)}, Client: lc}
		lc.UserLogin = login
		return login
	}
	b := newLogin(divaTestChen, "")
	a := newLogin(divaTestAmin, "token-a")
	h := newDIVAControlHarness(t, divaControlConfig{logins: func() []*bridgev2.UserLogin { return []*bridgev2.UserLogin{b, a} }})
	code, raw := h.getStatus(t, divaTestToken)
	if code != http.StatusOK || strings.Contains(string(raw), divaTestAmin) || strings.Contains(string(raw), divaTestChen) {
		t.Fatalf("status = %d %s", code, raw)
	}
	var got divaStatusResponse
	_ = json.Unmarshal(raw, &got)
	if got.LoginCount != 2 || got.Logins[0].Index != 0 || !got.Logins[0].LoggedIn || got.Logins[1].Index != 1 || got.Logins[1].LoggedIn {
		t.Fatalf("logins = %s", raw)
	}
}
