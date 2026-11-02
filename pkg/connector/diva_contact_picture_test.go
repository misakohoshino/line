package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

func TestDIVAContactPictureShape(t *testing.T) {
	path := func(p *string) string {
		if p == nil {
			return "<nil>"
		}
		return *p
	}
	for _, tc := range []struct {
		name    string
		contact line.Contact
		found   bool
		path    string
	}{
		{"picture", line.Contact{Mid: "uStaff", DisplayName: "小美", PicturePath: "/0hABC-def_1.2"}, true, "/0hABC-def_1.2"},
		{"picture without slash", line.Contact{Mid: "uStaff", DisplayName: "小美", PicturePath: "0hABC"}, true, "/0hABC"},
		{"no picture", line.Contact{Mid: "uStaff", DisplayName: "小美"}, true, "<nil>"},
		{"weird path is no picture", line.Contact{Mid: "uStaff", DisplayName: "小美", PicturePath: "/a b?x=1"}, true, "<nil>"},
		{"getContact fallback (LINE knows nothing)", line.Contact{Mid: "uStaff", DisplayName: "uStaff"}, false, "<nil>"},
		{"empty", line.Contact{}, false, "<nil>"},
	} {
		found, got := divaContactPicture("uStaff", tc.contact)
		if found != tc.found || path(got) != tc.path {
			t.Fatalf("%s: got (%v, %s) want (%v, %s)", tc.name, found, path(got), tc.found, tc.path)
		}
	}
}

func TestDIVAContactPictureEndpoint(t *testing.T) {
	calls := 0
	var answer line.Contact
	var answerErr error
	h := newDIVAControlHarness(t, divaControlConfig{
		contactLookup: func(ctx context.Context, lc *LineClient, mid string) (line.Contact, error) {
			calls++
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing lookup deadline")
			}
			if mid != "uStaff" {
				t.Fatalf("unexpected mid %s", mid)
			}
			return answer, answerErr
		},
		send: func(context.Context, *LineClient, *lineOutboundRequest) (*lineOutboundResult, error) {
			t.Fatal("contact picture must never send")
			return nil, nil
		},
	})
	post := func(method, body, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/diva/v1/contact/picture", strings.NewReader(body))
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		h.s.handler().ServeHTTP(w, r)
		return w
	}
	valid := `{"mid":"uStaff"}`
	for _, tc := range []struct {
		method, body, token string
		status              int
	}{
		{http.MethodPost, valid, "", 401},
		{http.MethodPost, valid, "wrong", 401},
		{http.MethodGet, "", divaTestToken, 405},
		{http.MethodPost, `{}`, divaTestToken, 400},
		{http.MethodPost, `{"mid":"cGroup"}`, divaTestToken, 400},
		{http.MethodPost, `{"mid":"uStaff","chat_id":"cGroup"}`, divaTestToken, 400},
		{http.MethodPost, `{"mid":"uStaff"}{}`, divaTestToken, 400},
		{http.MethodPost, `{"mid":"uStaff","account_mid":"uMissing"}`, divaTestToken, 503},
	} {
		if w := post(tc.method, tc.body, tc.token); w.Code != tc.status {
			t.Fatalf("body=%s status=%d want %d (%s)", tc.body, w.Code, tc.status, w.Body.String())
		}
	}
	if calls != 0 {
		t.Fatalf("invalid requests reached LINE: %d", calls)
	}

	type result struct {
		OK          bool    `json:"ok"`
		Found       bool    `json:"found"`
		PicturePath *string `json:"picture_path"`
	}
	ask := func() result {
		w := post(http.MethodPost, valid, divaTestToken)
		if w.Code != 200 {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		var data result
		if json.Unmarshal(w.Body.Bytes(), &data) != nil || !data.OK {
			t.Fatalf("bad response %s", w.Body.String())
		}
		if strings.Contains(w.Body.String(), "小美") {
			t.Fatal("display name must not be returned")
		}
		return data
	}
	answer = line.Contact{Mid: "uStaff", DisplayName: "小美", PicturePath: "/0hPIC"}
	if got := ask(); !got.Found || got.PicturePath == nil || *got.PicturePath != "/0hPIC" {
		t.Fatalf("picture: %+v", got)
	}
	answer = line.Contact{Mid: "uStaff", DisplayName: "小美"}
	if got := ask(); !got.Found || got.PicturePath != nil {
		t.Fatalf("no picture: %+v", got)
	}
	answer, answerErr = line.Contact{}, errors.New("line down")
	if got := ask(); got.Found || got.PicturePath != nil {
		t.Fatalf("error must be not found: %+v", got)
	}

	// 忙的時候直接回 503，不再問 LINE。
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
