package connector

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

const divaTestAccount = "uaccount0000000000000000000000000"

func newDIVAV2TestClient(logOut io.Writer) *LineClient {
	return &LineClient{
		Mid: divaTestAccount,
		UserLogin: &bridgev2.UserLogin{
			Bridge: &bridgev2.Bridge{Log: zerolog.New(logOut).Level(zerolog.DebugLevel)},
		},
		contactCache: map[string]cachedContact{
			"usender": {Contact: line.Contact{Mid: "usender", DisplayName: "阿明"}, cachedAt: time.Now()},
		},
	}
}

// assertDIVAGolden compares the encoded v2 event with the expected JSON,
// ignoring key order but not missing, extra or null fields.
func assertDIVAGolden(t *testing.T, event *divaV2Event, golden string) {
	t.Helper()
	gotRaw, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal v2 event: %v", err)
	}
	var got, want any
	if err := json.Unmarshal(gotRaw, &got); err != nil {
		t.Fatalf("unmarshal v2 event: %v", err)
	}
	if err := json.Unmarshal([]byte(golden), &want); err != nil {
		t.Fatalf("golden JSON is invalid: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("v2 event mismatch\n got: %s\nwant: %s", gotRaw, golden)
	}
}

func TestDIVAV2GoldenText(t *testing.T) {
	lc := newDIVAV2TestClient(io.Discard)
	msg := &line.Message{
		ID: "600000000000000001", From: "usender", To: "cgroup", ToType: int(ToGroup),
		CreatedTime: "1727654321000", ContentType: int(ContentText),
		Chunks:          []string{"a", "b", "c", "d", "e"},
		ContentMetadata: map[string]string{"e2eeVersion": "2"},
	}
	event := lc.buildDIVAV2Event(msg, "cgroup", "5分 白1234", false, int(OpReceiveMessage), divaOriginLive)
	assertDIVAGolden(t, event, `{
		"version": 2,
		"event_id": "line:600000000000000001",
		"event_type": "message",
		"origin": "live",
		"account": {"mid": "uaccount0000000000000000000000000"},
		"chat": {"id": "cgroup", "type": "group"},
		"sender": {"mid": "usender", "is_from_me": false, "display_name": "阿明"},
		"message": {
			"id": "600000000000000001",
			"content_type": 0,
			"created_at": 1727654321000,
			"decryption_failed": false,
			"e2ee": true
		},
		"content": {"type": "text", "text": "5分 白1234"},
		"relations": {"reply_to": null, "mentions": [], "mention_all": false},
		"metadata": {}
	}`)
}

func TestDIVAV2GoldenImageDescriptorHasNoKeyMaterial(t *testing.T) {
	lc := newDIVAV2TestClient(io.Discard)
	const secret = "c2VjcmV0LWtleS1tYXRlcmlhbA=="
	msg := &line.Message{
		ID: "600000000000000002", From: "ustranger", To: "rroom", ToType: int(ToRoom),
		CreatedTime: "1727654322000", ContentType: int(ContentImage),
		Chunks: []string{"chunk-1", "chunk-2", "chunk-3", "chunk-4", "chunk-5"},
		ContentMetadata: map[string]string{
			"e2eeVersion": "2",
			"FILE_NAME":   "car.jpg",
			"FILE_SIZE":   "20480",
			"OID":         "oid-value",
			"SID":         "emi",
			"ENC_KM":      secret,
		},
	}
	// For E2EE media the decrypted body is the keyMaterial JSON.
	decrypted := `{"keyMaterial":"` + secret + `"}`
	event := lc.buildDIVAV2Event(msg, "rroom", decrypted, false, int(OpReceiveMessage), divaOriginLive)
	assertDIVAGolden(t, event, `{
		"version": 2,
		"event_id": "line:600000000000000002",
		"event_type": "message",
		"origin": "live",
		"account": {"mid": "uaccount0000000000000000000000000"},
		"chat": {"id": "rroom", "type": "room"},
		"sender": {"mid": "ustranger", "is_from_me": false, "display_name": null},
		"message": {
			"id": "600000000000000002",
			"content_type": 1,
			"created_at": 1727654322000,
			"decryption_failed": false,
			"e2ee": true
		},
		"content": {"type": "image", "file_name": "car.jpg", "file_size": 20480},
		"relations": {"reply_to": null, "mentions": [], "mention_all": false},
		"metadata": {}
	}`)

	raw, _ := json.Marshal(event)
	for _, forbidden := range []string{secret, "keyMaterial", "ENC_KM", "chunk-1", "oid-value"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("v2 image event leaked %q: %s", forbidden, raw)
		}
	}
}

func TestDIVAV2GoldenReply(t *testing.T) {
	lc := newDIVAV2TestClient(io.Discard)
	msg := &line.Message{
		ID: "600000000000000003", From: "usender", To: "cgroup", ToType: int(ToGroup),
		CreatedTime: "1727654323000", ContentType: int(ContentText),
		RelatedMessageID: "599999999999999999", MessageRelationType: 3, RelatedMessageServiceCode: 1,
	}
	event := lc.buildDIVAV2Event(msg, "cgroup", "好", false, int(OpReceiveMessage), divaOriginLive)
	assertDIVAGolden(t, event, `{
		"version": 2,
		"event_id": "line:600000000000000003",
		"event_type": "message",
		"origin": "live",
		"account": {"mid": "uaccount0000000000000000000000000"},
		"chat": {"id": "cgroup", "type": "group"},
		"sender": {"mid": "usender", "is_from_me": false, "display_name": "阿明"},
		"message": {
			"id": "600000000000000003",
			"content_type": 0,
			"created_at": 1727654323000,
			"decryption_failed": false,
			"e2ee": false
		},
		"content": {"type": "text", "text": "好"},
		"relations": {"reply_to": {"message_id": "599999999999999999"}, "mentions": [], "mention_all": false},
		"metadata": {}
	}`)
}

func TestDIVAV2GoldenMention(t *testing.T) {
	lc := newDIVAV2TestClient(io.Discard)
	// "@阿明 @All 這張你出": LINE offsets are UTF-16 code units.
	msg := &line.Message{
		ID: "600000000000000004", From: divaTestAccount, To: "cgroup", ToType: int(ToGroup),
		CreatedTime: "1727654324000", ContentType: int(ContentText),
		ContentMetadata: map[string]string{
			"MENTION": `{"MENTIONEES":[{"S":"0","E":"3","M":"udriver"},{"S":"4","E":"8","A":"1"},{"S":"x","E":"9","M":"uother"}]}`,
		},
	}
	event := lc.buildDIVAV2Event(msg, "cgroup", "@阿明 @All 這張你出", false, int(OpSendMessage), divaOriginLive)
	assertDIVAGolden(t, event, `{
		"version": 2,
		"event_id": "line:600000000000000004",
		"event_type": "message",
		"origin": "live",
		"account": {"mid": "uaccount0000000000000000000000000"},
		"chat": {"id": "cgroup", "type": "group"},
		"sender": {"mid": "uaccount0000000000000000000000000", "is_from_me": true, "display_name": null},
		"message": {
			"id": "600000000000000004",
			"content_type": 0,
			"created_at": 1727654324000,
			"decryption_failed": false,
			"e2ee": false
		},
		"content": {"type": "text", "text": "@阿明 @All 這張你出"},
		"relations": {
			"reply_to": null,
			"mentions": [
				{"mid": "udriver", "start": 0, "end": 3},
				{"mid": "uother", "start": null, "end": 9}
			],
			"mention_all": true
		},
		"metadata": {}
	}`)
}

func TestDIVAV2GoldenDecryptionFailed(t *testing.T) {
	lc := newDIVAV2TestClient(io.Discard)
	msg := &line.Message{
		ID: "600000000000000005", From: "usender", To: "cgroup", ToType: int(ToGroup),
		CreatedTime: "1727654325000", ContentType: int(ContentText),
		Chunks: []string{"a", "b", "c", "d", "e"},
	}
	event := lc.buildDIVAV2Event(msg, "cgroup", "", true, int(OpReceiveMessage), divaOriginLive)
	assertDIVAGolden(t, event, `{
		"version": 2,
		"event_id": "line:600000000000000005",
		"event_type": "message",
		"origin": "live",
		"account": {"mid": "uaccount0000000000000000000000000"},
		"chat": {"id": "cgroup", "type": "group"},
		"sender": {"mid": "usender", "is_from_me": false, "display_name": "阿明"},
		"message": {
			"id": "600000000000000005",
			"content_type": 0,
			"created_at": 1727654325000,
			"decryption_failed": true,
			"e2ee": true
		},
		"content": {"type": "text", "text": null},
		"relations": {"reply_to": null, "mentions": [], "mention_all": false},
		"metadata": {}
	}`)
}

func TestDIVAV2GoldenBackfillSticker(t *testing.T) {
	lc := newDIVAV2TestClient(io.Discard)
	msg := &line.Message{
		ID: "600000000000000006", From: "usender", To: "cgroup", ToType: int(ToGroup),
		CreatedTime: "1727650000000", ContentType: int(ContentSticker),
		ContentMetadata: map[string]string{"STKID": "52002734", "STKPKGID": "11537", "STKVER": "1", "STKTXT": "[OK]"},
	}
	event := lc.buildDIVAV2Event(msg, "cgroup", "", false, int(OpReceiveMessage), divaOriginBackfill)
	assertDIVAGolden(t, event, `{
		"version": 2,
		"event_id": "line:600000000000000006",
		"event_type": "message",
		"origin": "backfill",
		"account": {"mid": "uaccount0000000000000000000000000"},
		"chat": {"id": "cgroup", "type": "group"},
		"sender": {"mid": "usender", "is_from_me": false, "display_name": "阿明"},
		"message": {
			"id": "600000000000000006",
			"content_type": 7,
			"created_at": 1727650000000,
			"decryption_failed": false,
			"e2ee": false
		},
		"content": {"type": "sticker", "package_id": "11537", "sticker_id": "52002734"},
		"relations": {"reply_to": null, "mentions": [], "mention_all": false},
		"metadata": {"STKID": "52002734", "STKPKGID": "11537"}
	}`)
}

// LINE-wrapped notices and unknown types are never presented as text.
func TestDIVAV2WrappedNoticesAreUnsupported(t *testing.T) {
	cases := []line.Message{
		{ContentType: int(ContentText), ContentMetadata: map[string]string{"ORGCONTP": "CALL"}},
		{ContentType: int(ContentText), ContentMetadata: map[string]string{"ORGCONTP": "CONTACT", "vCard": "BEGIN:VCARD"}},
		{ContentType: int(ContentPostNotification)},
		{ContentType: int(ContentFlex)},
	}
	for _, msg := range cases {
		content := divaV2ContentFor(&msg, "BEGIN:VCARD", false)
		if content != (divaV2UnsupportedContent{Type: "unsupported"}) {
			t.Fatalf("content for %+v = %#v, want unsupported", msg, content)
		}
	}
}

func TestDIVAContractVersionFromEnv(t *testing.T) {
	lc := newDIVAV2TestClient(io.Discard)
	for value, want := range map[string]int{"": 1, "1": 1, " 2 ": 2, "3": 1, "v2": 1} {
		t.Setenv("DIVA_CONTRACT_VERSION", value)
		if got := lc.divaContractVersion(); got != want {
			t.Fatalf("DIVA_CONTRACT_VERSION=%q -> %d, want %d", value, got, want)
		}
	}
}

type divaSyncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *divaSyncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *divaSyncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startDIVAReplyingWorker is a worker that always asks for reply_text, as a
// misbehaving Server A might for a backfilled 123456654.
func startDIVAReplyingWorker(t *testing.T, contractVersion string) <-chan []byte {
	t.Helper()
	received := make(chan []byte, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"reply_text":"789"}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("DIVA_WEBHOOK_URL", srv.URL+"/line/inbound")
	t.Setenv("DIVA_CONTRACT_VERSION", contractVersion)
	return received
}

func TestHandleDIVAInboundV1DoesNotForwardBackfill(t *testing.T) {
	received := startDIVAReplyingWorker(t, "")
	lc := newDIVAV2TestClient(io.Discard)
	lc.handleDIVAInbound(&line.Message{
		ID: "7", From: "usender", To: "cgroup", ToType: int(ToGroup), ContentType: int(ContentText),
	}, "cgroup", "123456654", false, int(OpReceiveMessage), divaOriginBackfill)

	select {
	case body := <-received:
		t.Fatalf("v1 must not forward backfill, got: %s", body)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestHandleDIVAInboundV2ForwardsBackfillWithoutReplying(t *testing.T) {
	received := startDIVAReplyingWorker(t, "2")
	logs := &divaSyncBuffer{}
	lc := newDIVAV2TestClient(logs)
	lc.handleDIVAInbound(&line.Message{
		ID: "8", From: "usender", To: "cgroup", ToType: int(ToGroup), ContentType: int(ContentText),
	}, "cgroup", "123456654", false, int(OpReceiveMessage), divaOriginBackfill)

	select {
	case body := <-received:
		var event map[string]any
		if err := json.Unmarshal(body, &event); err != nil {
			t.Fatalf("invalid JSON: %v", err)
		}
		if event["version"] != float64(2) || event["origin"] != "backfill" {
			t.Fatalf("want v2 backfill event, got: %s", body)
		}
		for _, legacy := range []string{"text", "group_id", "sender_id", "message_id"} {
			if _, ok := event[legacy]; ok {
				t.Fatalf("v2 event must not carry legacy field %q: %s", legacy, body)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("v2 backfill event was not forwarded")
	}

	// The worker asked for a reply. Reaching sendDIVAText here would panic on
	// the empty test client; the adapter must refuse and log instead.
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(logs.String(), "backfilled event, not sending") {
		if time.Now().After(deadline) {
			t.Fatalf("expected backfill reply to be refused, logs: %s", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if strings.Contains(logs.String(), "123456654") {
		t.Fatalf("log leaked message text: %s", logs.String())
	}
}
