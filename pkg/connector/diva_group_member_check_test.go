package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDIVAClassifyMember(t *testing.T) {
	self := "uSelf"
	for _, tc := range []struct {
		name    string
		members divaGroupMembers
		want    string
	}{
		{"fresh member", divaGroupMembers{Fresh: []string{self, "uDriver", "uOther"}}, divaMemberIn},
		{"fresh list without driver", divaGroupMembers{Fresh: []string{self, "uOther"}}, divaMemberOut},
		{"self only is ambiguous", divaGroupMembers{Fresh: []string{self}}, divaMemberUnknown},
		{"fallback proves in", divaGroupMembers{Fresh: []string{self}, Fallback: []string{"uDriver"}}, divaMemberIn},
		{"fallback never proves out", divaGroupMembers{Fresh: []string{self}, Fallback: []string{"uOther"}}, divaMemberUnknown},
		{"empty", divaGroupMembers{}, divaMemberUnknown},
	} {
		if got := classifyDIVAMember(tc.members, self, "uDriver"); got != tc.want {
			t.Fatalf("%s: got %s want %s", tc.name, got, tc.want)
		}
	}
}

func TestDIVAGroupMemberCheckEndpoint(t *testing.T) {
	calls := 0
	var answer divaGroupMembers
	var answerErr error
	h := newDIVAControlHarness(t, divaControlConfig{
		groupMembers: func(ctx context.Context, lc *LineClient, chatID string) (divaGroupMembers, error) {
			calls++
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing lookup deadline")
			}
			if chatID != "cRebateGroup" {
				t.Fatalf("unexpected chat %s", chatID)
			}
			return answer, answerErr
		},
		send: func(context.Context, *LineClient, *lineOutboundRequest) (*lineOutboundResult, error) {
			t.Fatal("member check must never send")
			return nil, nil
		},
	})
	h.env.lc.Mid = "uSelf"
	post := func(method, body, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/diva/v1/group/member-check", strings.NewReader(body))
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		h.s.handler().ServeHTTP(w, r)
		return w
	}
	valid := `{"chat_id":"cRebateGroup","mid":"uDriver"}`
	for _, tc := range []struct {
		method, body, token string
		status              int
	}{
		{http.MethodPost, valid, "", 401},
		{http.MethodGet, "", divaTestToken, 405},
		{http.MethodPost, `{"chat_id":"cRebateGroup"}`, divaTestToken, 400},
		{http.MethodPost, `{"chat_id":"uPerson","mid":"uDriver"}`, divaTestToken, 400},
		{http.MethodPost, `{"chat_id":"rRoom","mid":"uDriver"}`, divaTestToken, 400},
		{http.MethodPost, `{"chat_id":"cRebateGroup","mid":"cNotUser"}`, divaTestToken, 400},
		{http.MethodPost, `{"chat_id":"cRebateGroup","mid":"uDriver","rpc":"leave"}`, divaTestToken, 400},
		{http.MethodPost, `{"chat_id":"cRebateGroup","mid":"uDriver"}{}`, divaTestToken, 400},
		{http.MethodPost, `{"chat_id":"cRebateGroup","mid":"uDriver","account_mid":"uMissing"}`, divaTestToken, 503},
	} {
		if w := post(tc.method, tc.body, tc.token); w.Code != tc.status {
			t.Fatalf("body=%s status=%d want %d (%s)", tc.body, w.Code, tc.status, w.Body.String())
		}
	}
	if calls != 0 {
		t.Fatalf("invalid requests reached LINE: %d", calls)
	}

	status := func() string {
		w := post(http.MethodPost, valid, divaTestToken)
		if w.Code != 200 {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		var data struct {
			OK     bool   `json:"ok"`
			Status string `json:"status"`
		}
		if json.Unmarshal(w.Body.Bytes(), &data) != nil || !data.OK {
			t.Fatalf("bad response %s", w.Body.String())
		}
		if strings.Contains(w.Body.String(), "uOther") {
			t.Fatal("member list must never be returned")
		}
		return data.Status
	}
	answer = divaGroupMembers{Fresh: []string{"uSelf", "uDriver", "uOther"}}
	if got := status(); got != divaMemberIn {
		t.Fatalf("in: %s", got)
	}
	answer = divaGroupMembers{Fresh: []string{"uSelf", "uOther"}}
	if got := status(); got != divaMemberOut {
		t.Fatalf("out: %s", got)
	}
	answer, answerErr = divaGroupMembers{}, errors.New("line down")
	if got := status(); got != divaMemberUnknown {
		t.Fatalf("error must be unknown: %s", got)
	}

	// Overload is bounded and answers immediately without another LINE call.
	answerErr = nil
	before := calls
	h.s.memberCheckSem <- struct{}{}
	h.s.memberCheckSem <- struct{}{}
	if w := post(http.MethodPost, valid, divaTestToken); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("busy status=%d", w.Code)
	}
	<-h.s.memberCheckSem
	<-h.s.memberCheckSem
	if calls != before {
		t.Fatal("busy request reached LINE")
	}
}
