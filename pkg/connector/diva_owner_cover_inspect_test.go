package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

type fakeCoverInspector struct {
	*fakeProfileAPI
	calls int
}

func (f *fakeCoverInspector) inspectCover(_ context.Context, _ *LineClient, mid string) (line.ProfileCoverInspection, error) {
	if mid != divaProfileTestAccount { panic("cross-account inspection") }
	f.calls++
	return line.ProfileCoverInspection{HasCoverInfo: true, HasOID: true, HasSID: true, ParsedSelfCover: true}, nil
}

func TestDIVAOwnerCoverInspectionReadOnlyAuthAndExactAccount(t *testing.T) {
	base := &fakeProfileAPI{profiles: []*line.Profile{profileSelf("safe", "status", "photo")}}
	api := &fakeCoverInspector{fakeProfileAPI: base}
	h := profileHarness(t, base, divaProfileOperationPhoto)
	h.s.cfg.profileAPI = api
	path := "/diva/v1/owner/profile/cover/inspect?account_mid=" + divaProfileTestAccount
	for _, tc := range []struct {
		name, method, path, bearer string
		status int
	}{
		{"missing bearer", http.MethodGet, path, "", http.StatusUnauthorized},
		{"wrong bearer", http.MethodGet, path, "Bearer wrong", http.StatusUnauthorized},
		{"not get", http.MethodPost, path, "Bearer " + divaTestToken, http.StatusMethodNotAllowed},
		{"wrong query", http.MethodGet, path+"&extra=1", "Bearer " + divaTestToken, http.StatusBadRequest},
		{"valid self only", http.MethodGet, path, "Bearer " + divaTestToken, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.bearer != "" { req.Header.Set("Authorization", tc.bearer) }
			w := httptest.NewRecorder()
			h.s.handler().ServeHTTP(w, req)
			if w.Code != tc.status { t.Fatalf("status %d want %d", w.Code, tc.status) }
			if tc.status == http.StatusOK {
				var result struct {
					OK bool `json:"ok"`
					ReadOnly bool `json:"read_only"`
					Schema line.ProfileCoverInspection `json:"schema"`
				}
				if json.Unmarshal(w.Body.Bytes(), &result) != nil || !result.OK || !result.ReadOnly || !result.Schema.ParsedSelfCover {
					t.Fatal("missing safe inspection result")
				}
				for _, forbidden := range []string{divaProfileTestAccount, "channelAccessToken", "objectId", "main-token", "uploadProfile"} {
					if strings.Contains(w.Body.String(), forbidden) { t.Fatal("inspection returned forbidden profile value") }
				}
			}
		})
	}
	if api.calls != 1 { t.Fatalf("inspection calls %d", api.calls) }
	_, _, imageWrites := base.counts()
	if imageWrites != 0 { t.Fatal("read-only inspection invoked an image write") }
}
