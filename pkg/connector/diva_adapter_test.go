package connector

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

func TestDIVAV1ForwardDecision(t *testing.T) {
	cases := []struct {
		name             string
		msg              line.Message
		text             string
		decryptionFailed bool
		want             string
	}{
		{
			name: "plain text is forwarded",
			msg:  line.Message{ContentType: int(ContentText)},
			text: "123456654",
			want: "",
		},
		{
			name: "text with inline sticon metadata is still user text",
			msg:  line.Message{ContentType: int(ContentText), ContentMetadata: map[string]string{"STICON_OWNERSHIP": "[]"}},
			text: "hi (emoji)",
			want: "",
		},
		{
			name: "E2EE image keyMaterial is not forwarded as text",
			msg:  line.Message{ContentType: int(ContentImage)},
			text: `{"keyMaterial":"c2VjcmV0"}`,
			want: divaSkipNotText,
		},
		{
			name: "sticker is not text",
			msg:  line.Message{ContentType: int(ContentSticker)},
			text: "",
			want: divaSkipNotText,
		},
		{
			name: "call notice wrapped in text content type",
			msg:  line.Message{ContentType: int(ContentText), ContentMetadata: map[string]string{"ORGCONTP": "CALL"}},
			text: "call",
			want: divaSkipWrappedNotice,
		},
		{
			name: "device contact vCard wrapped in text content type",
			msg:  line.Message{ContentType: int(ContentText), ContentMetadata: map[string]string{"ORGCONTP": "CONTACT"}},
			text: "BEGIN:VCARD",
			want: divaSkipWrappedNotice,
		},
		{
			name:             "decryption failure has no text",
			msg:              line.Message{ContentType: int(ContentText)},
			text:             "",
			decryptionFailed: true,
			want:             divaSkipDecryptionFailed,
		},
		{
			name: "whitespace only text",
			msg:  line.Message{ContentType: int(ContentText)},
			text: "  \n",
			want: divaSkipEmptyText,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := divaV1ForwardDecision(&tc.msg, tc.text, tc.decryptionFailed); got != tc.want {
				t.Fatalf("divaV1ForwardDecision() = %q, want %q", got, tc.want)
			}
		})
	}
}

func newDIVATestClient(logOut io.Writer) *LineClient {
	return &LineClient{UserLogin: &bridgev2.UserLogin{
		Bridge: &bridgev2.Bridge{Log: zerolog.New(logOut).Level(zerolog.DebugLevel)},
	}}
}

func startDIVATestWorker(t *testing.T) <-chan []byte {
	t.Helper()
	received := make(chan []byte, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- body
		// No reply_text: the test must not reach the LINE send path.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("DIVA_WEBHOOK_URL", srv.URL+"/line/inbound")
	return received
}

// The payload Server A receives must stay the legacy v1 shape until PR 1.
func TestHandleDIVAInboundForwardsGroupTextAsV1(t *testing.T) {
	received := startDIVATestWorker(t)
	logs := &divaSyncBuffer{}
	lc := newDIVATestClient(logs)

	msg := &line.Message{
		ID:          "600000000000000001",
		From:        "usender",
		To:          "cgroup",
		ToType:      int(ToGroup),
		ContentType: int(ContentText),
	}
	lc.handleDIVAInbound(msg, "cgroup", "123456654", false, int(OpReceiveMessage), divaOriginLive)

	select {
	case body := <-received:
		var got map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("worker received invalid JSON: %v", err)
		}
		want := map[string]any{
			"text":       "123456654",
			"group_id":   "cgroup",
			"sender_id":  "usender",
			"message_id": "600000000000000001",
		}
		if len(got) != len(want) {
			t.Fatalf("payload keys = %v, want exactly %v", got, want)
		}
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("payload[%q] = %v, want %v", k, got[k], v)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("group text message was not forwarded to the DIVA worker")
	}

	// Wait for the async delivery log too, so it is covered by the leak check.
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(logs.String(), "DIVA adapter delivered inbound event") {
		if time.Now().After(deadline) {
			t.Fatalf("delivery was not logged: %s", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if strings.Contains(logs.String(), "123456654") {
		t.Fatalf("[DIVA_RX] log leaked message text: %s", logs.String())
	}
}

func TestHandleDIVAInboundV2ForwardsDirectCallButNotDirectText(t *testing.T) {
	received := startDIVATestWorker(t)
	t.Setenv("DIVA_CONTRACT_VERSION", "2")
	lc := newDIVAV2TestClient(io.Discard)

	lc.handleDIVAInbound(&line.Message{
		ID: "call-1", From: "usender", To: "ume", ToType: int(ToUser), ContentType: int(ContentText),
		ContentMetadata: map[string]string{"MESSAGE_TARGET": "u1234567"},
	}, "usender", "", false, int(OpReceiveMessage), divaOriginLive)
	lc.handleDIVAInbound(&line.Message{
		ID: "text-1", From: "usender", To: "ume", ToType: int(ToUser), ContentType: int(ContentText),
		ContentMetadata: map[string]string{"e2eeVersion": "2"},
	}, "usender", "ordinary DM", false, int(OpReceiveMessage), divaOriginLive)

	select {
	case body := <-received:
		var got map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("worker received invalid JSON: %v", err)
		}
		content, _ := got["content"].(map[string]any)
		chat, _ := got["chat"].(map[string]any)
		metadata, _ := got["metadata"].(map[string]any)
		if content["type"] != "call" || chat["type"] != "direct" || chat["id"] != "usender" {
			t.Fatalf("direct call payload = %v", got)
		}
		if _, leaked := metadata["MESSAGE_TARGET"]; leaked {
			t.Fatalf("MESSAGE_TARGET routing metadata leaked to worker: %v", metadata)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("direct call was not forwarded to the DIVA worker")
	}

	select {
	case body := <-received:
		t.Fatalf("ordinary direct message leaked to DIVA worker: %s", body)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestHandleDIVAInboundWithholdsMediaAndDMs(t *testing.T) {
	received := startDIVATestWorker(t)
	logs := &divaSyncBuffer{}
	lc := newDIVATestClient(logs)

	const keyMaterial = `{"keyMaterial":"c2VjcmV0LWtleS1tYXRlcmlhbA=="}`
	lc.handleDIVAInbound(&line.Message{
		ID: "1", From: "usender", To: "cgroup", ToType: int(ToGroup), ContentType: int(ContentImage),
	}, "cgroup", keyMaterial, false, int(OpReceiveMessage), divaOriginLive)
	lc.handleDIVAInbound(&line.Message{
		ID: "2", From: "usender", To: "ume", ToType: int(ToUser), ContentType: int(ContentText),
	}, "usender", "123456654", false, int(OpReceiveMessage), divaOriginLive)

	select {
	case body := <-received:
		t.Fatalf("unexpected forward to DIVA worker: %s", body)
	case <-time.After(300 * time.Millisecond):
	}

	if strings.Contains(logs.String(), "keyMaterial") || strings.Contains(logs.String(), "c2VjcmV0") {
		t.Fatalf("[DIVA_RX] log leaked keyMaterial: %s", logs.String())
	}
	if !strings.Contains(logs.String(), `"skip_reason":"not_text"`) {
		t.Fatalf("expected a not_text debug record, got: %s", logs.String())
	}
}
