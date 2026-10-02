package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"testing"
	"time"
)

func divaImageMetadata(requestID, chatID string) map[string]any {
	return map[string]any{
		"version":      1,
		"request_id":   requestID,
		"target":       map[string]any{"chat_id": chatID, "account_mid": nil},
		"message_type": "image",
		"file_name":    "dispatch.png",
		"mime_type":    "image/png",
	}
}

func (h *divaControlHarness) postMedia(t *testing.T, token string, metadata map[string]any, media []byte) (int, outboundResult) {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	metaPart, err := mw.CreateFormField("metadata")
	if err != nil {
		t.Fatal(err)
	}
	metaBytes, _ := json.Marshal(metadata)
	if _, err = metaPart.Write(metaBytes); err != nil {
		t.Fatal(err)
	}
	mediaPart, err := mw.CreateFormFile("media", "upload.png")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = mediaPart.Write(media); err != nil {
		t.Fatal(err)
	}
	if err = mw.Close(); err != nil {
		t.Fatal(err)
	}

	req, _ := http.NewRequest(http.MethodPost, h.srv.URL+"/diva/v1/send-media", bytes.NewReader(body.Bytes()))
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /send-media: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var out outboundResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("response is not JSON (%d): %s", resp.StatusCode, data)
	}
	return resp.StatusCode, out
}

func TestDIVAControlSendsImageMedia(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	id := newDIVARequestID()
	data := testPNG(t)

	status, got := h.postMedia(t, divaTestToken, divaImageMetadata(id, sendTestGroup), data)
	requireDIVAOK(t, status, got)
	if got.RequestID != id || got.Deduplicated {
		t.Fatalf("result = %+v", got)
	}
	sent := h.env.fake.sentMessages(t)
	if len(sent) != 1 {
		t.Fatalf("sendMessage calls = %d, want 1", len(sent))
	}
	if name := sent[0].Msg.ContentMetadata["FILE_NAME"]; name != "dispatch.png" {
		t.Fatalf("FILE_NAME = %q", name)
	}
	if typ := sent[0].Msg.ContentType; typ != int(ContentImage) {
		t.Fatalf("content type = %d, want image", typ)
	}
}

func TestDIVAControlImageMediaDedupeFingerprintsBytes(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	id := newDIVARequestID()
	data := testPNG(t)
	meta := divaImageMetadata(id, sendTestGroup)

	status, first := h.postMedia(t, divaTestToken, meta, data)
	requireDIVAOK(t, status, first)

	status, second := h.postMedia(t, divaTestToken, meta, data)
	requireDIVAOK(t, status, second)
	if !second.Deduplicated {
		t.Fatalf("second result was not deduplicated: %+v", second)
	}

	changed := append(append([]byte(nil), data...), 0)
	status, third := h.postMedia(t, divaTestToken, meta, changed)
	requireDIVAError(t, status, third, http.StatusConflict, outboundInvalidRequest, deliveryNotSent)
	if n := len(h.env.fake.sentMessages(t)); n != 1 {
		t.Fatalf("sendMessage calls = %d, want 1", n)
	}
}

func TestDIVAControlImageMediaValidation(t *testing.T) {
	data := testPNG(t)
	tests := []struct {
		name       string
		cfg        divaControlConfig
		token      string
		mutate     func(map[string]any)
		media      []byte
		wantStatus int
		wantCode   outboundErrorCode
	}{
		{
			name: "unauthorized", token: "wrong",
			wantStatus: http.StatusUnauthorized, wantCode: outboundUnauthorized,
		},
		{
			name: "direct user target", token: divaTestToken,
			mutate:     func(m map[string]any) {
				m["target"] = map[string]any{"chat_id": sendTestPeer, "account_mid": nil}
			},
			wantStatus: http.StatusBadRequest, wantCode: outboundInvalidRequest,
		},
		{
			name: "video not enabled", token: divaTestToken,
			mutate:     func(m map[string]any) { m["message_type"] = "video" },
			wantStatus: http.StatusBadRequest, wantCode: outboundUnsupportedMessageType,
		},
		{
			name: "mime mismatch", token: divaTestToken,
			mutate:     func(m map[string]any) { m["mime_type"] = "image/jpeg" },
			wantStatus: http.StatusBadRequest, wantCode: outboundInvalidRequest,
		},
		{
			name: "not an image", token: divaTestToken, media: []byte("not an image"),
			wantStatus: http.StatusBadRequest, wantCode: outboundInvalidRequest,
		},
		{
			name: "over media limit", token: divaTestToken,
			cfg:        divaControlConfig{mediaMaxBytes: int64(len(data) - 1)},
			wantStatus: http.StatusRequestEntityTooLarge, wantCode: outboundInvalidRequest,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newDIVAControlHarness(t, tc.cfg)
			meta := divaImageMetadata(newDIVARequestID(), sendTestGroup)
			if tc.mutate != nil {
				tc.mutate(meta)
			}
			body := tc.media
			if body == nil {
				body = data
			}
			status, got := h.postMedia(t, tc.token, meta, body)
			requireDIVAError(t, status, got, tc.wantStatus, tc.wantCode, deliveryNotSent)
			if n := len(h.env.fake.sentMessages(t)); n != 0 {
				t.Fatalf("sendMessage calls = %d", n)
			}
		})
	}
}

func TestDIVAControlMediaDefaults(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	if h.s.cfg.mediaMaxBytes != divaControlDefaultMediaMaxBytes {
		t.Fatalf("media max bytes = %d", h.s.cfg.mediaMaxBytes)
	}
	if cap(h.s.mediaSem) != divaControlMediaMaxConcurrent {
		t.Fatalf("media concurrency = %d", cap(h.s.mediaSem))
	}
	if h.s.mediaPending != 0 {
		t.Fatalf("media pending = %d", h.s.mediaPending)
	}
}

func TestDIVAControlRejectsTooManyPendingMediaSends(t *testing.T) {
	started := make(chan struct{}, divaControlMediaMaxPending)
	release := make(chan struct{})
	h := newDIVAControlHarness(t, divaControlConfig{
		maxConcurrent:      divaControlMediaMaxPending,
		mediaMaxConcurrent: divaControlMediaMaxPending,
		send: func(ctx context.Context, lc *LineClient, req *lineOutboundRequest) (*lineOutboundResult, error) {
			started <- struct{}{}
			select {
			case <-release:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	})
	defer close(release)

	data := testPNG(t)
	for i := 0; i < divaControlMediaMaxPending; i++ {
		metaMap := divaImageMetadata(newDIVARequestID(), fmt.Sprintf("cmedia%030d", i))
		metaJSON, _ := json.Marshal(metaMap)
		var raw divaSendMediaMetadata
		if err := json.Unmarshal(metaJSON, &raw); err != nil {
			t.Fatal(err)
		}
		job, reqErr := parseDIVASendMediaMetadata(raw, data, "upload.png")
		if reqErr != nil {
			t.Fatalf("parse media job: %v", reqErr)
		}
		go h.s.submit(context.Background(), job)
	}
	for i := 0; i < divaControlMediaMaxPending; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("media send did not start")
		}
	}

	metaMap := divaImageMetadata(newDIVARequestID(), "coverflow000000000000000000000000000")
	metaJSON, _ := json.Marshal(metaMap)
	var raw divaSendMediaMetadata
	if err := json.Unmarshal(metaJSON, &raw); err != nil {
		t.Fatal(err)
	}
	job, reqErr := parseDIVASendMediaMetadata(raw, data, "upload.png")
	if reqErr != nil {
		t.Fatalf("parse overflow media job: %v", reqErr)
	}
	got, status := h.s.submit(t.Context(), job)
	requireDIVAError(t, status, got, http.StatusServiceUnavailable, outboundInternal, deliveryNotSent)
}

func TestDIVAControlSendMediaRejectsWrongMethod(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	req, _ := http.NewRequest(http.MethodGet, h.srv.URL+"/diva/v1/send-media", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
