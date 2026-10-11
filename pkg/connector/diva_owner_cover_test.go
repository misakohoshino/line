package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

type fakeCoverProfileAPI struct {
	*fakeProfileAPI
	coverMu      sync.Mutex
	coverReads   int
	coverWrites  int
	objects      []string
	coverErr     error
	coverReadErr error
	blockCover   chan struct{}
}

func (f *fakeCoverProfileAPI) readCover(ctx context.Context, _ *LineClient, mid string) (string, error) {
	f.coverMu.Lock()
	defer f.coverMu.Unlock()
	if mid != divaProfileTestAccount {
		panic("wrong self")
	}
	f.coverReads++
	if f.coverReads > 1 && f.coverReadErr != nil {
		return "", f.coverReadErr
	}
	return f.objects[min(f.coverReads-1, len(f.objects)-1)], nil
}
func (f *fakeCoverProfileAPI) uploadCover(ctx context.Context, _ *LineClient, mid string, jpeg []byte) (*line.ProfileImageUploadResult, error) {
	f.coverMu.Lock()
	f.coverWrites++
	f.coverMu.Unlock()
	if f.blockCover != nil {
		select {
		case <-f.blockCover:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.coverErr != nil {
		return nil, f.coverErr
	}
	return &line.ProfileImageUploadResult{ObjectID: "new-cover", Hash: "cover-hash"}, nil
}
func coverMetadata(id string) map[string]any {
	return map[string]any{"version": 1, "request_id": id, "account_mid": divaProfileTestAccount, "operation": divaProfileOperationCover, "mime_type": "image/png"}
}

func TestDIVAProfileCoverRequiresAssociationAndExactFreshSelfObject(t *testing.T) {
	for _, tc := range []struct {
		name     string
		after    string
		writeErr error
		readErr  error
		state    string
	}{
		{"matched", "new-cover", nil, nil, divaProfileStateConfirmed},
		{"changed but wrong object", "other-cover", nil, nil, divaProfileStateUnconfirmed},
		{"unchanged", "old-cover", nil, nil, divaProfileStateUnconfirmed},
		{"fresh read unavailable", "new-cover", nil, errors.New("read failed"), divaProfileStateUnconfirmed},
		{"association unknown", "new-cover", &line.ProfileWriteError{Outcome: line.ProfileWriteUnknown, Code: "PROFILE_COVER_ASSOCIATION_UNCONFIRMED"}, nil, divaProfileStateUnknown},
		{"association refused", "new-cover", &line.ProfileWriteError{Outcome: line.ProfileWriteRejected, Code: "PROFILE_COVER_ASSOCIATION_REJECTED"}, nil, divaProfileStateRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &fakeProfileAPI{profiles: []*line.Profile{profileSelf("名", "簽名", "photo")}}
			api := &fakeCoverProfileAPI{fakeProfileAPI: base, objects: []string{"old-cover", tc.after}, coverErr: tc.writeErr, coverReadErr: tc.readErr}
			h := profileHarness(t, base, divaProfileOperationCover)
			h.s.cfg.profileAPI = api
			id := newDIVARequestID()
			status, result := postProfilePhoto(t, h, coverMetadata(id), profilePNG(t), false)
			if status != 200 || result.State != tc.state || api.coverWrites != 1 {
				t.Fatalf("status=%d result=%+v writes=%d", status, result, api.coverWrites)
			}
			// Replay must return the stored result and not upload or associate again.
			_, repeated := postProfilePhoto(t, h, coverMetadata(id), profilePNG(t), false)
			if !repeated.Deduplicated || repeated.State != tc.state || api.coverWrites != 1 {
				t.Fatal("cover replay wrote again")
			}
			if _, _, photos := base.counts(); photos != 0 {
				t.Fatal("cover called photo adapter")
			}
		})
	}
}

func TestDIVAProfileProductionCoverOnlyWithExplicitOperationAllowList(t *testing.T) {
	base := &fakeProfileAPI{profiles: []*line.Profile{profileSelf("名", "簽名", "photo")}}
	// Normal production posture still blocks cover, even when the native
	// adapter supports it. No LINE call is allowed without explicit opt-in.
	h := profileHarness(t, base, divaProfileOperationName, divaProfileOperationPhoto)
	h.s.cfg.profileAPI = lineProfileAPI{}
	status, result := postProfilePhoto(t, h, coverMetadata(newDIVARequestID()), profilePNG(t), false)
	if status != http.StatusForbidden || result.Code != "PROFILE_OPERATION_DISABLED" || result.State != divaProfileStateNotSent {
		t.Fatalf("cover unexpectedly admitted with flag OFF: %d %+v", status, result)
	}
	// The bridge may advertise cover as eligible only when explicitly
	// configured. This does not write any image or claim LINE E2E success.
	h.s.cfg.profileOperations[divaProfileOperationCover] = true
	r := httptest.NewRequest(http.MethodGet, "/diva/v1/owner/profile/capabilities?account_mid="+divaProfileTestAccount, nil)
	r.Header.Set("Authorization", "Bearer "+divaTestToken)
	w := httptest.NewRecorder()
	h.s.handler().ServeHTTP(w, r)
	var data struct {
		Enabled []string          `json:"enabled_operations"`
		Blocked map[string]string `json:"blocked_operations"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &data)
	if len(data.Enabled) != 3 ||
		data.Enabled[0] != divaProfileOperationName ||
		data.Enabled[1] != divaProfileOperationPhoto ||
		data.Enabled[2] != divaProfileOperationCover ||
		len(data.Blocked) != 0 {
		t.Fatalf("explicit cover gate not reflected in capabilities: %s", w.Body.String())
	}
}

func TestDIVAProfileCoverAndPhotoShareSingleAccountLock(t *testing.T) {
	base := &fakeProfileAPI{profiles: []*line.Profile{profileSelf("名", "簽名", "photo")}}
	api := &fakeCoverProfileAPI{fakeProfileAPI: base, objects: []string{"old-cover", "new-cover"}, blockCover: make(chan struct{})}
	h := profileHarness(t, base, divaProfileOperationCover, divaProfileOperationPhoto)
	h.s.cfg.profileAPI = api
	h.s.cfg.profileWait = 0
	_, pending := postProfilePhoto(t, h, coverMetadata(newDIVARequestID()), profilePNG(t), false)
	if pending.State != divaProfileStatePending {
		t.Fatal("expected in-flight cover")
	}
	meta := coverMetadata(newDIVARequestID())
	meta["operation"] = divaProfileOperationPhoto
	status, result := postProfilePhoto(t, h, meta, profilePNG(t), false)
	close(api.blockCover)
	if status != 409 || result.Code != "PROFILE_ACCOUNT_BUSY" {
		t.Fatalf("competing photo admitted: %d %+v", status, result)
	}
}
