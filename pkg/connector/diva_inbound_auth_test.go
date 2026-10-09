package connector

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

const ownerTestToken = "test-only-owner-inbound-token-0000000000"

// Independent receiver verifies the final bytes; no LINE requests are sent.
func verifyOwnerTestRequest(t *testing.T, r *http.Request) []byte {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Error(err)
		return nil
	}
	stamp := r.Header.Get("X-DIVA-Inbound-Timestamp")
	epoch, err := strconv.ParseInt(stamp, 10, 64)
	if err != nil || time.Since(time.Unix(epoch, 0)) > time.Minute {
		t.Errorf("invalid timestamp %q", stamp)
	}
	digest := sha256.Sum256(body)
	message := fmt.Sprintf("diva-owner-inbound-v1\nPOST\n/line/inbound\n%s\n%s\n%x", stamp, r.Header.Get("Content-Type"), digest)
	mac := hmac.New(sha256.New, []byte(ownerTestToken))
	_, _ = mac.Write([]byte(message))
	if !hmac.Equal([]byte(r.Header.Get("X-DIVA-Inbound-Signature")), []byte(hex.EncodeToString(mac.Sum(nil)))) {
		t.Error("forwarding signature does not bind final body")
	}
	return body
}

func TestDIVAInboundSignatureGolden(t *testing.T) {
	body := []byte(`{"version":2}`)
	req := httptest.NewRequest(http.MethodPost, "http://worker/line/inbound", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	signDIVAInboundAt(req, body, ownerTestToken, 1800000000)
	if got := req.Header.Get("X-DIVA-Inbound-Signature"); got != "26d11cca85a6ec447560ab384d54723cee43905e206ef309164a4296d7a965d8" {
		t.Fatalf("signature %s", got)
	}
	for _, token := range []string{"", "short"} {
		unsigned := httptest.NewRequest(http.MethodPost, "/line/inbound", nil)
		signDIVAInboundAt(unsigned, body, token, 1800000000)
		if unsigned.Header.Get("X-DIVA-Inbound-Signature") != "" {
			t.Fatal("short token signed")
		}
	}
}

func TestDIVAInboundSignsFinalJSONMultipartAndRecovery(t *testing.T) {
	t.Setenv("DIVA_CONTROL_TOKEN", ownerTestToken)
	received := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := verifyOwnerTestRequest(t, r)
		if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") && !bytes.Contains(body, []byte("binary-test")) {
			t.Error("signed multipart body missing media")
		}
		received <- r.Header.Get("Content-Type")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	t.Setenv("DIVA_WEBHOOK_URL", srv.URL+"/line/inbound")
	lc := newDIVAV2TestClient(io.Discard)
	for _, payload := range []string{`{"content":{"type":"text"}}`, `{"content":{"type":"call"}}`, `{"origin":"backfill"}`} {
		data := []byte(payload)
		lc.forwardDIVAInbound(func() ([]byte, error) { return data, nil }, nil, "cgroup", "1", false)
	}
	lc.forwardDIVAInbound(func() ([]byte, error) { return []byte(`{"content":{"type":"image"}}`), nil },
		func(context.Context) (*divaInboundMedia, error) {
			return &divaInboundMedia{Data: []byte("binary-test"), MimeType: "image/png", FileName: "image.png"}, nil
		}, "cgroup", "2", false)
	if reply, err := postDIVARawRecovery(context.Background(), srv.URL+"/line/inbound", []byte(`{"origin":"backfill"}`)); err != nil || reply {
		t.Fatalf("recovery reply=%v err=%v", reply, err)
	}
	multipart := 0
	for range 5 {
		select {
		case contentType := <-received:
			if strings.HasPrefix(contentType, "multipart/form-data") {
				multipart++
			}
		case <-time.After(3 * time.Second):
			t.Fatal("forward request missing")
		}
	}
	if multipart != 1 {
		t.Fatalf("multipart requests=%d", multipart)
	}
}

func TestDIVAInboundMissingTokenStillForwards(t *testing.T) {
	t.Setenv("DIVA_CONTROL_TOKEN", "")
	received := make(chan bool, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Get("X-DIVA-Inbound-Signature") == ""
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	t.Setenv("DIVA_WEBHOOK_URL", srv.URL+"/line/inbound")
	newDIVAV2TestClient(io.Discard).forwardDIVAInbound(func() ([]byte, error) {
		return []byte(`{"content":{"type":"call"}}`), nil
	}, nil, "cgroup", "1", false)
	select {
	case unsigned := <-received:
		if !unsigned {
			t.Fatal("missing credential signed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing token stopped forwarding")
	}
}
