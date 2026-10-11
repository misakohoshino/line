package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/png"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

const divaProfileTestAccount = "UAccount_fake-90"

type fakeProfileAPI struct {
	mu       sync.Mutex
	profiles []*line.Profile // returned in order; the last one repeats
	reads    int
	writes   []string
	uploads  [][]byte
	writeErr error
	block    chan struct{}
	readErr  error
}

func (f *fakeProfileAPI) readSelf(ctx context.Context, _ *LineClient) (*line.Profile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := ctx.Deadline(); !ok {
		panic("profile read without deadline")
	}
	f.reads++
	if f.readErr != nil && f.reads > 1 {
		return nil, f.readErr
	}
	index := min(f.reads-1, len(f.profiles)-1)
	copyProfile := *f.profiles[index]
	return &copyProfile, nil
}

func (f *fakeProfileAPI) updateAttribute(ctx context.Context, _ *LineClient, attribute int, value string) error {
	if _, ok := ctx.Deadline(); !ok {
		panic("profile write without deadline")
	}
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, map[int]string{2: "name:", 16: "status:"}[attribute]+value)
	return f.writeErr
}

func (f *fakeProfileAPI) uploadImage(_ context.Context, _ *LineClient, selfMID string, jpeg []byte) (*line.ProfileImageUploadResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if selfMID != divaProfileTestAccount {
		panic("upload for another account")
	}
	f.uploads = append(f.uploads, jpeg)
	if f.writeErr != nil {
		return nil, f.writeErr
	}
	return &line.ProfileImageUploadResult{ObjectID: "oid", Hash: "hash"}, nil
}

func (f *fakeProfileAPI) counts() (int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads, len(f.writes), len(f.uploads)
}

func profileHarness(t *testing.T, api *fakeProfileAPI, operations ...string) *divaControlHarness {
	t.Helper()
	divaProfileReadbackDelay = time.Millisecond
	enabled := map[string]bool{}
	for _, operation := range operations {
		enabled[operation] = true
	}
	h := newDIVAControlHarness(t, divaControlConfig{profileOperations: enabled, profileAPI: api, profileWait: 2 * time.Second})
	h.env.lc.Mid = divaProfileTestAccount
	return h
}

func profileSelf(name, status, picture string) *line.Profile {
	return &line.Profile{Mid: divaProfileTestAccount, DisplayName: name, StatusMessage: status, PictureStatus: picture, PicturePath: "/" + picture}
}

func postProfile(t *testing.T, h *divaControlHarness, body map[string]any) (int, divaProfileResult) {
	t.Helper()
	raw, _ := json.Marshal(body)
	r := httptest.NewRequest(http.MethodPost, "/diva/v1/owner/profile/update", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+divaTestToken)
	w := httptest.NewRecorder()
	h.s.handler().ServeHTTP(w, r)
	var result divaProfileResult
	_ = json.Unmarshal(w.Body.Bytes(), &result)
	return w.Code, result
}

func profileBody(requestID, operation, value string) map[string]any {
	return map[string]any{"version": 1, "request_id": requestID, "account_mid": divaProfileTestAccount, "operation": operation, "value": value}
}

func TestDIVAProfileOperationsAllowListFailsClosed(t *testing.T) {
	for raw, want := range map[string]int{"": 0, " profile_name , profile_status ": 2, "profile_photo": 1} {
		got, err := parseDIVAProfileOperations(raw)
		if err != nil || len(got) != want {
			t.Fatalf("%q: %v %v", raw, got, err)
		}
	}
	for _, raw := range []string{"profile_name,leave_group", "PROFILE_NAME", "profile_name,,", "*"} {
		if got, err := parseDIVAProfileOperations(raw); err == nil || len(got) != 0 {
			t.Fatalf("%q must disable everything: %v", raw, got)
		}
	}
}

func TestDIVAProfileUpdateConfirmedOnlyAfterReadback(t *testing.T) {
	api := &fakeProfileAPI{profiles: []*line.Profile{profileSelf("舊名", "舊簽名", "p1"), profileSelf("新名😀", "舊簽名", "p1")}}
	h := profileHarness(t, api, divaProfileOperationName)
	var logs bytes.Buffer
	h.s.cfg.log = zerolog.New(&logs)
	status, result := postProfile(t, h, profileBody(newDIVARequestID(), divaProfileOperationName, "新名😀"))
	reads, writes, _ := api.counts()
	if status != 200 || result.State != divaProfileStateConfirmed || result.Write != divaProfileWriteAccepted ||
		result.Readback != divaProfileReadbackMatch || reads != 2 || writes != 1 || api.writes[0] != "name:新名😀" {
		t.Fatalf("status=%d result=%+v reads=%d writes=%v", status, result, reads, api.writes)
	}
	if strings.Contains(logs.String(), "新名") || strings.Contains(logs.String(), divaProfileTestAccount) {
		t.Fatalf("log leaked value or account: %s", logs.String())
	}
}

func TestDIVAProfileOutcomeMatrix(t *testing.T) {
	unknown := &line.ProfileWriteError{Outcome: line.ProfileWriteUnknown, Code: "PROFILE_TRANSPORT_UNKNOWN"}
	rejected := &line.ProfileWriteError{Outcome: line.ProfileWriteRejected, Code: "PROFILE_LINE_REJECTED"}
	notSent := &line.ProfileWriteError{Outcome: line.ProfileWriteNotSent, Code: "PROFILE_REQUEST_SIGN_FAILED"}
	for _, tc := range []struct {
		name     string
		after    *line.Profile
		writeErr error
		readErr  error
		state    string
		reads    int
	}{
		{"unknown write but LINE shows value", profileSelf("新名", "", ""), unknown, nil, divaProfileStateConfirmed, 2},
		{"unknown write and old value", profileSelf("舊名", "", ""), unknown, nil, divaProfileStateUnknown, 1 + divaProfileReadbackTries},
		{"accepted but old value", profileSelf("舊名", "", ""), nil, nil, divaProfileStateUnconfirmed, 1 + divaProfileReadbackTries},
		{"accepted but read back fails", profileSelf("新名", "", ""), nil, errors.New("read failed"), divaProfileStateUnconfirmed, 1 + divaProfileReadbackTries},
		{"read back from another account", &line.Profile{Mid: "UOther", DisplayName: "新名"}, nil, nil, divaProfileStateUnconfirmed, 1 + divaProfileReadbackTries},
		{"rejected", profileSelf("新名", "", ""), rejected, nil, divaProfileStateRejected, 1},
		{"not sent", profileSelf("新名", "", ""), notSent, nil, divaProfileStateNotSent, 1},
	} {
		api := &fakeProfileAPI{profiles: []*line.Profile{profileSelf("舊名", "", ""), tc.after}, writeErr: tc.writeErr, readErr: tc.readErr}
		h := profileHarness(t, api, divaProfileOperationName)
		_, result := postProfile(t, h, profileBody(newDIVARequestID(), divaProfileOperationName, "新名"))
		reads, writes, _ := api.counts()
		if result.State != tc.state || reads != tc.reads || writes != 1 {
			t.Fatalf("%s: result=%+v reads=%d writes=%d", tc.name, result, reads, writes)
		}
	}
}

func TestDIVAProfileUnchangedAndSelfVerification(t *testing.T) {
	api := &fakeProfileAPI{profiles: []*line.Profile{profileSelf("同名", "同簽名", "p")}}
	h := profileHarness(t, api, divaProfileOperationName, divaProfileOperationStatus)
	for _, operation := range []string{divaProfileOperationName, divaProfileOperationStatus} {
		value := map[string]string{divaProfileOperationName: "同名", divaProfileOperationStatus: "同簽名"}[operation]
		_, result := postProfile(t, h, profileBody(newDIVARequestID(), operation, value))
		if result.State != divaProfileStateUnchanged {
			t.Fatalf("%s: %+v", operation, result)
		}
	}
	if _, writes, _ := api.counts(); writes != 0 {
		t.Fatal("unchanged value was written")
	}
	other := &fakeProfileAPI{profiles: []*line.Profile{{Mid: "UOther", DisplayName: "x"}}}
	h = profileHarness(t, other, divaProfileOperationName)
	_, result := postProfile(t, h, profileBody(newDIVARequestID(), divaProfileOperationName, "新名"))
	if _, writes, _ := other.counts(); result.State != divaProfileStateNotSent || result.Code != "PROFILE_SELF_VERIFY_FAILED" || writes != 0 {
		t.Fatalf("self verification did not stop write: %+v", result)
	}
}

func TestDIVAProfileRequestValidationAndAuthorization(t *testing.T) {
	api := &fakeProfileAPI{profiles: []*line.Profile{profileSelf("舊名", "", "")}}
	h := profileHarness(t, api, divaProfileOperationName)
	base := func() map[string]any { return profileBody(newDIVARequestID(), divaProfileOperationName, "新名") }
	cases := []struct {
		name   string
		mutate func(map[string]any)
		status int
	}{
		{"disabled status", func(b map[string]any) { b["operation"] = divaProfileOperationStatus }, 403},
		{"photo on text route", func(b map[string]any) { b["operation"] = divaProfileOperationPhoto }, 400},
		{"arbitrary attribute", func(b map[string]any) { b["attribute"] = 4 }, 400},
		{"arbitrary target", func(b map[string]any) { b["target_mid"] = "UOther" }, 400},
		{"missing account", func(b map[string]any) { delete(b, "account_mid") }, 400},
		{"other account", func(b map[string]any) { b["account_mid"] = "UOther" }, 503},
		{"bad request id", func(b map[string]any) { b["request_id"] = "1" }, 400},
		{"bad version", func(b map[string]any) { b["version"] = 2 }, 400},
		{"empty value", func(b map[string]any) { b["value"] = "" }, 400},
		{"surrounding space", func(b map[string]any) { b["value"] = " 新名" }, 400},
		{"too long", func(b map[string]any) { b["value"] = strings.Repeat("名", 21) }, 400},
		{"control char", func(b map[string]any) { b["value"] = "新\u0007名" }, 400},
	}
	for _, tc := range cases {
		body := base()
		tc.mutate(body)
		if status, _ := postProfile(t, h, body); status != tc.status {
			t.Fatalf("%s: status=%d", tc.name, status)
		}
	}
	raw, _ := json.Marshal(base())
	for _, token := range []string{"", "wrong-token-wrong-token-wrong-token"} {
		r := httptest.NewRequest(http.MethodPost, "/diva/v1/owner/profile/update", bytes.NewReader(raw))
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		h.s.handler().ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatalf("token %q: %d", token, w.Code)
		}
	}
	if reads, writes, _ := api.counts(); reads != 0 || writes != 0 {
		t.Fatalf("rejected requests reached LINE: reads=%d writes=%d", reads, writes)
	}
}

func TestDIVAProfileRequestIDDedupeAndAccountSingleFlight(t *testing.T) {
	api := &fakeProfileAPI{profiles: []*line.Profile{profileSelf("舊名", "", ""), profileSelf("新名", "", "")}, block: make(chan struct{})}
	h := profileHarness(t, api, divaProfileOperationName)
	h.s.cfg.profileWait = 30 * time.Millisecond
	requestID := newDIVARequestID()
	status, first := postProfile(t, h, profileBody(requestID, divaProfileOperationName, "新名"))
	if status != 200 || first.State != divaProfileStatePending {
		t.Fatalf("first wait should be pending: %d %+v", status, first)
	}
	if status, busy := postProfile(t, h, profileBody(newDIVARequestID(), divaProfileOperationName, "第三名")); status != 409 || busy.Code != "PROFILE_ACCOUNT_BUSY" {
		t.Fatalf("second write while in flight: %d %+v", status, busy)
	}
	if status, reused := postProfile(t, h, profileBody(requestID, divaProfileOperationName, "別的名")); status != 409 || reused.Code != "PROFILE_REQUEST_ID_REUSED" {
		t.Fatalf("request_id reuse: %d %+v", status, reused)
	}
	resultURL := "/diva/v1/owner/profile/result?account_mid=" + divaProfileTestAccount + "&request_id=" + requestID
	get := func(path string) (int, divaProfileResult) {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("Authorization", "Bearer "+divaTestToken)
		w := httptest.NewRecorder()
		h.s.handler().ServeHTTP(w, r)
		var result divaProfileResult
		_ = json.Unmarshal(w.Body.Bytes(), &result)
		return w.Code, result
	}
	if status, pending := get(resultURL); status != 200 || pending.State != divaProfileStatePending {
		t.Fatalf("result while in flight: %d %+v", status, pending)
	}
	close(api.block)
	h.s.cfg.profileWait = 2 * time.Second
	status, again := postProfile(t, h, profileBody(requestID, divaProfileOperationName, "新名"))
	if status != 200 || again.State != divaProfileStateConfirmed || !again.Deduplicated {
		t.Fatalf("same request must return the stored result: %d %+v", status, again)
	}
	if status, stored := get(resultURL); status != 200 || stored.State != divaProfileStateConfirmed {
		t.Fatalf("stored result: %d %+v", status, stored)
	}
	if status, _ := get("/diva/v1/owner/profile/result?account_mid=UOther&request_id=" + requestID); status != 404 {
		t.Fatal("another account could read the result")
	}
	if _, writes, _ := api.counts(); writes != 1 {
		t.Fatalf("writes=%d, want exactly one", writes)
	}
}

func profilePNG(t *testing.T) []byte {
	img := image.NewNRGBA(image.Rect(0, 0, 32, 32))
	img.Set(1, 1, color.NRGBA{R: 255, A: 128})
	var out bytes.Buffer
	if err := png.Encode(&out, img); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func postProfilePhoto(t *testing.T, h *divaControlHarness, metadata map[string]any, media []byte, extra bool) (int, divaProfileResult) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	metaPart, _ := writer.CreateFormField("metadata")
	_ = json.NewEncoder(metaPart).Encode(metadata)
	mediaPart, _ := writer.CreateFormFile("media", "x")
	_, _ = mediaPart.Write(media)
	if extra {
		extraPart, _ := writer.CreateFormField("url")
		_, _ = extraPart.Write([]byte("https://example.invalid/a.jpg"))
	}
	_ = writer.Close()
	r := httptest.NewRequest(http.MethodPost, "/diva/v1/owner/profile/photo", &body)
	r.Header.Set("Content-Type", writer.FormDataContentType())
	r.Header.Set("Authorization", "Bearer "+divaTestToken)
	w := httptest.NewRecorder()
	h.s.handler().ServeHTTP(w, r)
	var result divaProfileResult
	_ = json.Unmarshal(w.Body.Bytes(), &result)
	return w.Code, result
}

func TestDIVAProfilePhotoNormalizesAndRequiresPictureChange(t *testing.T) {
	meta := func(mime string) map[string]any {
		return map[string]any{"version": 1, "request_id": newDIVARequestID(), "account_mid": divaProfileTestAccount, "operation": divaProfileOperationPhoto, "mime_type": mime}
	}
	for _, tc := range []struct {
		after *line.Profile
		state string
	}{
		{profileSelf("名", "", "p2"), divaProfileStateConfirmed},
		{profileSelf("名", "", "p1"), divaProfileStateUnconfirmed},
	} {
		api := &fakeProfileAPI{profiles: []*line.Profile{profileSelf("名", "", "p1"), tc.after}}
		h := profileHarness(t, api, divaProfileOperationPhoto)
		status, result := postProfilePhoto(t, h, meta("image/png"), profilePNG(t), false)
		_, _, uploads := api.counts()
		if status != 200 || result.State != tc.state || uploads != 1 || !bytes.HasPrefix(api.uploads[0], []byte{0xff, 0xd8, 0xff}) {
			t.Fatalf("status=%d result=%+v uploads=%d", status, result, uploads)
		}
	}
	api := &fakeProfileAPI{profiles: []*line.Profile{profileSelf("名", "", "p1")}}
	h := profileHarness(t, api, divaProfileOperationPhoto)
	for name, tc := range map[string]struct {
		meta   map[string]any
		media  []byte
		extra  bool
		status int
	}{
		"fake mime":         {meta("image/jpeg"), profilePNG(t), false, 400},
		"gif mime":          {meta("image/gif"), profilePNG(t), false, 400},
		"corrupt":           {meta("image/png"), profilePNG(t)[:40], false, 400},
		"text":              {meta("image/png"), []byte("hello"), false, 400},
		"extra url part":    {meta("image/png"), profilePNG(t), true, 400},
		"wrong operation":   {func() map[string]any { m := meta("image/png"); m["operation"] = "profile_name"; return m }(), profilePNG(t), false, 400},
		"unknown field url": {func() map[string]any { m := meta("image/png"); m["url"] = "https://x"; return m }(), profilePNG(t), false, 400},
	} {
		if status, _ := postProfilePhoto(t, h, tc.meta, tc.media, tc.extra); status != tc.status {
			t.Fatalf("%s: status=%d", name, status)
		}
	}
	disabled := profileHarness(t, api, divaProfileOperationName)
	if status, result := postProfilePhoto(t, disabled, meta("image/png"), profilePNG(t), false); status != 403 || result.Code != "PROFILE_OPERATION_DISABLED" {
		t.Fatalf("disabled photo: %d %+v", status, result)
	}
	if reads, _, uploads := api.counts(); reads != 0 || uploads != 0 {
		t.Fatalf("invalid photo reached LINE: reads=%d uploads=%d", reads, uploads)
	}
}

func TestDIVAProfileCapabilitiesAndSelfHashesOnly(t *testing.T) {
	api := &fakeProfileAPI{profiles: []*line.Profile{profileSelf("秘密名字", "秘密簽名", "p1")}}
	h := profileHarness(t, api, divaProfileOperationStatus, divaProfileOperationName)
	get := func(path string) (int, string) {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("Authorization", "Bearer "+divaTestToken)
		w := httptest.NewRecorder()
		h.s.handler().ServeHTTP(w, r)
		return w.Code, w.Body.String()
	}
	status, body := get("/diva/v1/owner/profile/capabilities?account_mid=" + divaProfileTestAccount)
	if status != 200 || !strings.Contains(body, `"enabled_operations":["profile_name","profile_status"]`) || !strings.Contains(body, `"account_connected":true`) {
		t.Fatalf("capabilities: %d %s", status, body)
	}
	if reads, _, _ := api.counts(); reads != 0 {
		t.Fatal("capabilities called LINE")
	}
	status, body = get("/diva/v1/owner/profile/self?account_mid=" + divaProfileTestAccount)
	if status != 200 || strings.Contains(body, "秘密") || !strings.Contains(body, "display_name_sha256") {
		t.Fatalf("self: %d %s", status, body)
	}
	if status, _ := get("/diva/v1/owner/profile/self?account_mid=" + divaProfileTestAccount + "&mid=UOther"); status != 400 {
		t.Fatal("extra query accepted")
	}
	off := profileHarness(t, api)
	r := httptest.NewRequest(http.MethodGet, "/diva/v1/owner/profile/capabilities?account_mid="+divaProfileTestAccount, nil)
	r.Header.Set("Authorization", "Bearer "+divaTestToken)
	w := httptest.NewRecorder()
	off.s.handler().ServeHTTP(w, r)
	if !strings.Contains(w.Body.String(), `"enabled_operations":[]`) {
		t.Fatalf("default must be all off: %s", w.Body.String())
	}
}
