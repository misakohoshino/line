package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

const (
	divaTestToken  = "test-token-0123456789abcdefghijklmnopqrstuvwxyz"
	divaTestAmin   = "uamin000000000000000000000000000a"
	divaTestChen   = "uchen000000000000000000000000000c"
	divaTestGroupB = "cgroupb00000000000000000000000000"
)

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

type divaControlHarness struct {
	env *sendTestEnv
	s   *divaControlServer
	srv *httptest.Server
}

func newDIVAControlHarness(t *testing.T, cfg divaControlConfig) *divaControlHarness {
	t.Helper()
	env := newSendTestEnv(t, false)
	env.lc.UserLogin.Client = env.lc
	cfg.token = divaTestToken
	if cfg.logins == nil {
		cfg.logins = func() []*bridgev2.UserLogin { return []*bridgev2.UserLogin{env.lc.UserLogin} }
	}
	cfg.log = zerolog.New(io.Discard)
	s := newDIVAControlServer(cfg)
	srv := httptest.NewServer(s.handler())
	t.Cleanup(func() {
		srv.Close()
		s.stop(5 * time.Second)
	})
	return &divaControlHarness{env: env, s: s, srv: srv}
}

var divaUUIDCounter atomic.Int64

func newDIVARequestID() string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", divaUUIDCounter.Add(1))
}

func divaTextBody(requestID, chatID, text string) map[string]any {
	return map[string]any{
		"version":      1,
		"request_id":   requestID,
		"target":       map[string]any{"chat_id": chatID, "account_mid": nil},
		"message_type": "text",
		"content":      map[string]any{"text": text},
	}
}

func (h *divaControlHarness) postRaw(t *testing.T, token string, body []byte) (int, outboundResult) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, h.srv.URL+"/diva/v1/send", bytes.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	var out outboundResult
	data, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("response is not JSON (%d): %s", resp.StatusCode, data)
	}
	return resp.StatusCode, out
}

func (h *divaControlHarness) post(t *testing.T, body map[string]any) (int, outboundResult) {
	t.Helper()
	raw, _ := json.Marshal(body)
	return h.postRaw(t, divaTestToken, raw)
}

func requireDIVAError(t *testing.T, status int, got outboundResult, wantStatus int, code outboundErrorCode, delivery outboundDelivery) {
	t.Helper()
	if status != wantStatus || got.OK || got.Error == nil || got.Error.Code != code || got.Error.Delivery != delivery {
		t.Fatalf("status=%d result=%+v error=%+v, want %d %s %s", status, got, got.Error, wantStatus, code, delivery)
	}
}

func requireDIVAOK(t *testing.T, status int, got outboundResult) {
	t.Helper()
	if status != http.StatusOK || !got.OK || got.Message == nil {
		t.Fatalf("status=%d result=%+v error=%+v", status, got, got.Error)
	}
}

// ---------------------------------------------------------------------------
// listener and auth
// ---------------------------------------------------------------------------

func TestDIVAControlListenerNeedsToken(t *testing.T) {
	br := &bridgev2.Bridge{Log: zerolog.New(io.Discard)}
	t.Setenv("DIVA_CONTROL_LISTEN", "127.0.0.1:0")

	t.Setenv("DIVA_CONTROL_TOKEN", "")
	if s := startDIVAControl(br); s != nil {
		t.Fatal("listener started without DIVA_CONTROL_TOKEN")
	}
	t.Setenv("DIVA_CONTROL_TOKEN", "short-token")
	if s := startDIVAControl(br); s != nil {
		t.Fatal("listener started with a token shorter than the minimum")
	}

	t.Setenv("DIVA_CONTROL_TOKEN", divaTestToken)
	s := startDIVAControl(br)
	if s == nil {
		t.Fatal("listener did not start with a valid token")
	}
	defer s.stop(time.Second)
	resp, err := http.Get("http://" + s.addr + "/diva/v1/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("health: %v %v", resp, err)
	}
	resp.Body.Close()
	resp, err = http.Post("http://"+s.addr+"/diva/v1/send", "application/json", strings.NewReader("{}"))
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated send: %v %v", resp, err)
	}
	resp.Body.Close()
	if s.httpServer.ReadHeaderTimeout == 0 || s.httpServer.ReadTimeout == 0 {
		t.Fatal("server timeouts are not set")
	}

	s.stop(time.Second)
	if _, err := http.Get("http://" + s.addr + "/diva/v1/health"); err == nil {
		t.Fatal("listener still accepts connections after stop")
	}
}

func TestDIVAControlRejectsBadAuth(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	body, _ := json.Marshal(divaTextBody(newDIVARequestID(), sendTestGroup, "hi"))
	for name, token := range map[string]string{"none": "", "wrong": divaTestToken + "x", "prefix": divaTestToken[:10]} {
		status, got := h.postRaw(t, token, body)
		if status != http.StatusUnauthorized || got.Error == nil || got.Error.Code != outboundUnauthorized {
			t.Fatalf("%s: status=%d error=%+v", name, status, got.Error)
		}
	}
	req, _ := http.NewRequest(http.MethodPost, h.srv.URL+"/diva/v1/send", bytes.NewReader(body))
	req.Header.Set("Authorization", "Basic "+divaTestToken)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Basic auth status = %d", resp.StatusCode)
	}
	if n := len(h.env.fake.snapshot()); n != 0 {
		t.Fatalf("LINE calls = %d", n)
	}
}

func TestDIVAControlRejectsBadRequests(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	id := newDIVARequestID()
	withField := func(mutate func(map[string]any)) []byte {
		b := divaTextBody(id, sendTestGroup, "hi")
		mutate(b)
		raw, _ := json.Marshal(b)
		return raw
	}
	cases := []struct {
		name   string
		body   []byte
		status int
		code   outboundErrorCode
	}{
		{"invalid JSON", []byte(`{"version":1,`), 400, outboundInvalidRequest},
		{"unknown field", withField(func(b map[string]any) { b["extra"] = 1 }), 400, outboundInvalidRequest},
		{"trailing data", append(withField(func(map[string]any) {}), []byte(` {}`)...), 400, outboundInvalidRequest},
		{"missing version", withField(func(b map[string]any) { delete(b, "version") }), 400, outboundInvalidRequest},
		{"unsupported version", withField(func(b map[string]any) { b["version"] = 2 }), 400, outboundInvalidRequest},
		{"missing request_id", withField(func(b map[string]any) { delete(b, "request_id") }), 400, outboundInvalidRequest},
		{"request_id not UUID", withField(func(b map[string]any) { b["request_id"] = "abc" }), 400, outboundInvalidRequest},
		{"bad chat_id", withField(func(b map[string]any) { b["target"] = map[string]any{"chat_id": "group-1"} }), 400, outboundInvalidRequest},
		{"direct user chat_id", withField(func(b map[string]any) { b["target"] = map[string]any{"chat_id": sendTestPeer} }), 400, outboundInvalidRequest},
		{"bad account_mid", withField(func(b map[string]any) {
			b["target"] = map[string]any{"chat_id": sendTestGroup, "account_mid": "nope"}
		}), 400, outboundInvalidRequest},
		{"unsupported message type", withField(func(b map[string]any) {
			b["message_type"] = "image"
			b["content"] = map[string]any{"url": "x"}
		}), 400, outboundUnsupportedMessageType},
		{"empty text", withField(func(b map[string]any) { b["content"] = map[string]any{"text": "  "} }), 400, outboundInvalidRequest},
		{"bad reply id", withField(func(b map[string]any) {
			b["relations"] = map[string]any{"reply_to": map[string]any{"message_id": "local-1"}}
		}), 400, outboundInvalidRequest},
		{"bad reply_fallback", withField(func(b map[string]any) { b["options"] = map[string]any{"reply_fallback": "maybe"} }), 400, outboundInvalidRequest},
		{"oversized body", bytes.Repeat([]byte("x"), divaControlMaxBodyBytes+1), 413, outboundInvalidRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, got := h.postRaw(t, divaTestToken, tc.body)
			requireDIVAError(t, status, got, tc.status, tc.code, deliveryNotSent)
		})
	}
	if n := len(h.env.fake.snapshot()); n != 0 {
		t.Fatalf("LINE calls after rejected requests = %d", n)
	}
	resp, _ := http.Get(h.srv.URL + "/diva/v1/send")
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /send status = %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// text, mention, reply
// ---------------------------------------------------------------------------

func TestDIVAControlSendsText(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	id := newDIVARequestID()
	status, got := h.post(t, divaTextBody(id, sendTestGroup, "5分 白1234"))
	requireDIVAOK(t, status, got)
	if got.RequestID != id || got.Deduplicated || got.Message.ID != "srv-1" || got.Message.ChatID != sendTestGroup ||
		got.Message.E2EE || got.Message.CreatedAt == 0 || len(got.Fallbacks) != 0 {
		t.Fatalf("result = %+v message = %+v", got, got.Message)
	}
	sent := h.env.fake.sentMessages(t)
	if len(sent) != 1 || sent[0].Msg.Text != "5分 白1234" || sent[0].Msg.To != sendTestGroup || sent[0].Msg.RelatedMessageID != "" {
		t.Fatalf("sent = %+v", sent)
	}
	if _, ok := sent[0].Msg.ContentMetadata["MENTION"]; ok {
		t.Fatal("unexpected MENTION metadata")
	}
}

// The endpoint refuses direct (u...) targets but still sends to rooms, and the
// shared send core keeps its direct-chat path.
func TestDIVAControlTargetsGroupsAndRoomsOnly(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})

	status, got := h.post(t, divaTextBody(newDIVARequestID(), sendTestPeer, "hi"))
	requireDIVAError(t, status, got, http.StatusBadRequest, outboundInvalidRequest, deliveryNotSent)
	if !strings.Contains(got.Error.Detail, "direct user targets are not allowed") {
		t.Fatalf("detail = %q", got.Error.Detail)
	}
	if n := len(h.env.fake.snapshot()); n != 0 {
		t.Fatalf("LINE calls after direct target = %d", n)
	}

	const room = "rroom0000000000000000000000000000"
	status, got = h.post(t, divaTextBody(newDIVARequestID(), room, "hi room"))
	requireDIVAOK(t, status, got)

	// Production inbound chat_id is copied from LINE msg.To. Real groups can
	// start with uppercase C and are not guaranteed to match the old fixed
	// 33-char test shape. The control endpoint must pass that opaque ID through.
	const realWorldGroup = "CCLwvHR9QU3qworV2HYGB3rDhkiIZVsboNB3Wo7qyZfc"
	status, got = h.post(t, divaTextBody(newDIVARequestID(), realWorldGroup, "hi prod group"))
	requireDIVAOK(t, status, got)

	sent := h.env.fake.sentMessages(t)
	if len(sent) != 2 ||
		sent[0].Msg.To != room || sent[0].Msg.Text != "hi room" ||
		sent[1].Msg.To != realWorldGroup || sent[1].Msg.Text != "hi prod group" {
		t.Fatalf("sent = %+v", sent)
	}

	res, err := h.env.lc.sendLineOutbound(context.Background(), &lineOutboundRequest{ChatMID: sendTestPeer, ContentType: ContentText, Text: "dm"})
	if err != nil || res == nil || res.Sent == nil {
		t.Fatalf("shared core direct send: res=%+v err=%v", res, err)
	}
}

func TestDIVAControlMentions(t *testing.T) {
	cases := []struct {
		name       string
		text       string
		mentions   []map[string]any
		mentionAll bool
		want       string
	}{
		{
			name:     "Chinese name after an emoji uses UTF-16 offsets",
			text:     "🙂 @阿明 這張你出",
			mentions: []map[string]any{{"mid": divaTestAmin, "name": "阿明"}},
			want:     `{"MENTIONEES":[{"S":"3","E":"6","M":"` + divaTestAmin + `"}]}`,
		},
		{
			name:     "two mentions are ordered by position",
			text:     "@小陳 @阿明 出車",
			mentions: []map[string]any{{"mid": divaTestAmin, "name": "阿明"}, {"mid": divaTestChen, "name": "小陳"}},
			want:     `{"MENTIONEES":[{"S":"0","E":"3","M":"` + divaTestChen + `"},{"S":"4","E":"7","M":"` + divaTestAmin + `"}]}`,
		},
		{
			name:     "longer name is not taken by a prefix name",
			text:     "@阿明 @阿明哥",
			mentions: []map[string]any{{"mid": divaTestAmin, "name": "阿明"}, {"mid": divaTestChen, "name": "阿明哥"}},
			want:     `{"MENTIONEES":[{"S":"0","E":"3","M":"` + divaTestAmin + `"},{"S":"4","E":"8","M":"` + divaTestChen + `"}]}`,
		},
		{
			name:       "mention_all",
			text:       "@All 集合 @阿明",
			mentions:   []map[string]any{{"mid": divaTestAmin, "name": "阿明"}},
			mentionAll: true,
			want:       `{"MENTIONEES":[{"S":"0","E":"4","A":"1"},{"S":"8","E":"11","M":"` + divaTestAmin + `"}]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newDIVAControlHarness(t, divaControlConfig{})
			body := divaTextBody(newDIVARequestID(), sendTestGroup, tc.text)
			body["relations"] = map[string]any{"mentions": tc.mentions, "mention_all": tc.mentionAll}
			status, got := h.post(t, body)
			requireDIVAOK(t, status, got)
			msg := h.env.fake.sentMessages(t)[0].Msg
			if msg.ContentMetadata["MENTION"] != tc.want || msg.Text != tc.text {
				t.Fatalf("MENTION = %s\nwant      %s", msg.ContentMetadata["MENTION"], tc.want)
			}
		})
	}
}

func TestDIVAControlMentionNotInTextIsRejected(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	for _, rel := range []map[string]any{
		{"mentions": []map[string]any{{"mid": divaTestAmin, "name": "阿明"}}},
		{"mention_all": true},
		{"mentions": []map[string]any{{"mid": "not-a-mid", "name": "阿明"}}},
	} {
		body := divaTextBody(newDIVARequestID(), sendTestGroup, "這張你出")
		body["relations"] = rel
		status, got := h.post(t, body)
		requireDIVAError(t, status, got, 400, outboundInvalidRequest, deliveryNotSent)
	}
	if n := len(h.env.fake.snapshot()); n != 0 {
		t.Fatalf("LINE calls = %d", n)
	}
}

func TestDIVAControlReply(t *testing.T) {
	t.Run("reply", func(t *testing.T) {
		h := newDIVAControlHarness(t, divaControlConfig{})
		body := divaTextBody(newDIVARequestID(), sendTestGroup, "收到")
		body["relations"] = map[string]any{"reply_to": map[string]any{"message_id": "600000000000000123"}}
		status, got := h.post(t, body)
		requireDIVAOK(t, status, got)
		msg := h.env.fake.sentMessages(t)[0].Msg
		if msg.RelatedMessageID != "600000000000000123" || msg.MessageRelationType != 3 || msg.RelatedMessageServiceCode != 1 {
			t.Fatalf("reply relation = %+v", msg)
		}
	})
	t.Run("reply and mention together", func(t *testing.T) {
		h := newDIVAControlHarness(t, divaControlConfig{})
		body := divaTextBody(newDIVARequestID(), sendTestGroup, "@阿明 這張你出")
		body["relations"] = map[string]any{
			"reply_to": map[string]any{"message_id": "600000000000000123"},
			"mentions": []map[string]any{{"mid": divaTestAmin, "name": "阿明"}},
		}
		status, got := h.post(t, body)
		requireDIVAOK(t, status, got)
		msg := h.env.fake.sentMessages(t)[0].Msg
		if msg.RelatedMessageID != "600000000000000123" || !strings.Contains(msg.ContentMetadata["MENTION"], divaTestAmin) {
			t.Fatalf("reply+mention = %+v", msg)
		}
	})
	t.Run("target missing, default fail", func(t *testing.T) {
		h := newDIVAControlHarness(t, divaControlConfig{})
		h.env.fake.failNextSend(400, talkExceptionNotFound)
		body := divaTextBody(newDIVARequestID(), sendTestGroup, "收到")
		body["relations"] = map[string]any{"reply_to": map[string]any{"message_id": "600000000000000123"}}
		status, got := h.post(t, body)
		requireDIVAError(t, status, got, 200, outboundReplyTargetNotFound, deliveryNotSent)
		if n := len(h.env.fake.sentMessages(t)); n != 1 {
			t.Fatalf("sendMessage calls = %d, want 1 (no resend without the quote)", n)
		}
	})
	t.Run("target missing, send_without_reply", func(t *testing.T) {
		h := newDIVAControlHarness(t, divaControlConfig{})
		h.env.fake.failNextSend(400, talkExceptionNotFound)
		body := divaTextBody(newDIVARequestID(), sendTestGroup, "收到")
		body["relations"] = map[string]any{"reply_to": map[string]any{"message_id": "600000000000000123"}}
		body["options"] = map[string]any{"reply_fallback": "send_without_reply"}
		status, got := h.post(t, body)
		requireDIVAOK(t, status, got)
		if fmt.Sprint(got.Fallbacks) != "[reply_relation_dropped]" {
			t.Fatalf("fallbacks = %v", got.Fallbacks)
		}
		sent := h.env.fake.sentMessages(t)
		if len(sent) != 2 || sent[1].Msg.RelatedMessageID != "" {
			t.Fatalf("sent = %+v", sent)
		}
	})
}

// ---------------------------------------------------------------------------
// idempotency and timeout
// ---------------------------------------------------------------------------

// gateSends blocks every sendMessage until release is closed and counts them.
func gateSends(t *testing.T, h *divaControlHarness) (release func(), count *atomic.Int32) {
	t.Helper()
	gate := make(chan struct{})
	count = &atomic.Int32{}
	var once sync.Once
	h.env.hookTransport(t, func(req *http.Request) (*http.Response, error) {
		if isSendMessage(req) {
			count.Add(1)
			<-gate
		}
		return nil, nil
	})
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return release, count
}

func TestDIVAControlCompletedRequestIsNotResent(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	body := divaTextBody(newDIVARequestID(), sendTestGroup, "只送一次")
	_, first := h.post(t, body)
	status, second := h.post(t, body)
	requireDIVAOK(t, status, second)
	if !second.Deduplicated || first.Deduplicated || second.Message.ID != first.Message.ID {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	if n := len(h.env.fake.sentMessages(t)); n != 1 {
		t.Fatalf("sendMessage calls = %d", n)
	}

	changed := divaTextBody(body["request_id"].(string), sendTestGroup, "不同內容")
	status, conflict := h.post(t, changed)
	requireDIVAError(t, status, conflict, http.StatusConflict, outboundInvalidRequest, deliveryNotSent)
}

// The endpoint cannot cancel a LINE send. A slow send answers TIMEOUT /
// unknown, keeps running, and later retries with the same request_id get the
// real result without a second send.
func TestDIVAControlTimeoutKeepsSendingAndRetryGetsResult(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	release, count := gateSends(t, h)
	body := divaTextBody(newDIVARequestID(), sendTestGroup, "慢慢送")
	body["options"] = map[string]any{"timeout_ms": 150}

	status, first := h.post(t, body)
	requireDIVAError(t, status, first, 200, outboundTimeout, deliveryUnknown)
	if first.Deduplicated || !first.Error.Retryable {
		t.Fatalf("first = %+v", first.Error)
	}

	status, second := h.post(t, body)
	requireDIVAError(t, status, second, 200, outboundTimeout, deliveryUnknown)
	if !second.Deduplicated {
		t.Fatal("in-flight retry was not marked deduplicated")
	}
	if n := count.Load(); n != 1 {
		t.Fatalf("sendMessage calls while in flight = %d, want 1", n)
	}

	release()
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, third := h.post(t, body)
		if third.OK {
			requireDIVAOK(t, status, third)
			if !third.Deduplicated || third.Message.ID != "srv-1" {
				t.Fatalf("cached result = %+v", third)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("send never completed: %+v", third.Error)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := count.Load(); n != 1 {
		t.Fatalf("sendMessage calls = %d, want exactly 1", n)
	}
}

func TestDIVAControlConcurrentSameRequestIDSendsOnce(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	release, count := gateSends(t, h)
	body := divaTextBody(newDIVARequestID(), sendTestGroup, "二十次同一單")
	body["options"] = map[string]any{"timeout_ms": 5000}

	const n = 20
	results := make(chan outboundResult, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, got := h.post(t, body)
			results <- got
		}()
	}
	// Let all 20 arrive while the only send is held.
	time.Sleep(300 * time.Millisecond)
	release()
	wg.Wait()
	close(results)

	fresh := 0
	for got := range results {
		if !got.OK || got.Message.ID != "srv-1" {
			t.Fatalf("result = %+v %+v", got, got.Error)
		}
		if !got.Deduplicated {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("non-deduplicated responses = %d, want 1", fresh)
	}
	if got := count.Load(); got != 1 {
		t.Fatalf("sendMessage calls = %d, want 1", got)
	}
}

func TestDIVAControlDifferentRequestIDsEachSend(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	for i := 0; i < 3; i++ {
		status, got := h.post(t, divaTextBody(newDIVARequestID(), sendTestGroup, fmt.Sprintf("單 %d", i)))
		requireDIVAOK(t, status, got)
		if got.Deduplicated {
			t.Fatal("new request_id marked deduplicated")
		}
	}
	if n := len(h.env.fake.sentMessages(t)); n != 3 {
		t.Fatalf("sendMessage calls = %d, want 3", n)
	}
}

// A not_sent failure frees the request_id so the same request can be retried.
func TestDIVAControlNotSentFailureCanBeRetried(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	h.env.fake.failNextSend(400, `{"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","message":"TalkException","code":10,"reason":"not a member","parameterMap":null}}`)
	body := divaTextBody(newDIVARequestID(), sendTestGroup, "再試")
	status, first := h.post(t, body)
	requireDIVAError(t, status, first, 200, outboundTargetNotFound, deliveryNotSent)
	status, second := h.post(t, body)
	requireDIVAOK(t, status, second)
	if second.Deduplicated {
		t.Fatal("retry after not_sent was served from cache")
	}
}

func TestDIVAControlCompletedResultsExpire(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{resultTTL: 50 * time.Millisecond})
	body := divaTextBody(newDIVARequestID(), sendTestGroup, "過期")
	h.post(t, body)
	time.Sleep(80 * time.Millisecond)
	_, got := h.post(t, body)
	if got.Deduplicated {
		t.Fatal("expired request_id was still deduplicated")
	}
	h.s.mu.Lock()
	entries := len(h.s.requests)
	h.s.mu.Unlock()
	if entries != 1 {
		t.Fatalf("cache entries = %d, want 1 after sweep", entries)
	}
}

// ---------------------------------------------------------------------------
// ordering and concurrency
// ---------------------------------------------------------------------------

func waitForPending(t *testing.T, s *divaControlServer, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		s.mu.Lock()
		got := s.pending
		s.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending = %d, want %d", got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDIVAControlSameChatKeepsCallOrder(t *testing.T) {
	var mu sync.Mutex
	var order []string
	gate := make(chan struct{})
	h := newDIVAControlHarness(t, divaControlConfig{
		send: func(ctx context.Context, lc *LineClient, req *lineOutboundRequest) (*lineOutboundResult, error) {
			if req.Text == "A" {
				<-gate
			}
			mu.Lock()
			order = append(order, req.Text)
			mu.Unlock()
			return &lineOutboundResult{Sent: &line.Message{ID: "srv-" + req.Text}, SentPlaintext: true, SentAt: time.Now()}, nil
		},
	})
	var wg sync.WaitGroup
	for i, text := range []string{"A", "B", "C", "D"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := divaTextBody(newDIVARequestID(), sendTestGroup, text)
			body["options"] = map[string]any{"timeout_ms": 5000}
			h.post(t, body)
		}()
		waitForPending(t, h.s, i+1)
	}
	close(gate)
	wg.Wait()
	if strings.Join(order, "") != "ABCD" {
		t.Fatalf("send order = %v, want A B C D", order)
	}
	h.s.mu.Lock()
	lanes := len(h.s.lanes)
	h.s.mu.Unlock()
	if lanes != 0 {
		t.Fatalf("lanes left after all sends finished = %d", lanes)
	}
}

func TestDIVAControlSameChatOrderThroughSendCore(t *testing.T) {
	h := newDIVAControlHarness(t, divaControlConfig{})
	h.env.hookTransport(t, func(req *http.Request) (*http.Response, error) {
		if isSendMessage(req) {
			time.Sleep(30 * time.Millisecond)
		}
		return nil, nil
	})
	var wg sync.WaitGroup
	for i, text := range []string{"第一", "第二", "第三"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.post(t, divaTextBody(newDIVARequestID(), sendTestGroup, text))
		}()
		waitForPending(t, h.s, i+1)
	}
	wg.Wait()
	var got []string
	for _, s := range h.env.fake.sentMessages(t) {
		got = append(got, s.Msg.Text)
	}
	if strings.Join(got, ",") != "第一,第二,第三" {
		t.Fatalf("LINE order = %v", got)
	}
}

func TestDIVAControlDifferentChatsRunInParallel(t *testing.T) {
	groupBStarted := make(chan struct{})
	h := newDIVAControlHarness(t, divaControlConfig{
		send: func(ctx context.Context, lc *LineClient, req *lineOutboundRequest) (*lineOutboundResult, error) {
			if req.ChatMID == divaTestGroupB {
				close(groupBStarted)
			} else {
				select {
				case <-groupBStarted:
				case <-time.After(3 * time.Second):
					return nil, fmt.Errorf("chat B never started while chat A was sending")
				}
			}
			return &lineOutboundResult{Sent: &line.Message{ID: "srv"}, SentPlaintext: true, SentAt: time.Now()}, nil
		},
	})
	results := make(chan outboundResult, 2)
	go func() { _, got := h.post(t, divaTextBody(newDIVARequestID(), sendTestGroup, "A")); results <- got }()
	waitForPending(t, h.s, 1)
	go func() { _, got := h.post(t, divaTextBody(newDIVARequestID(), divaTestGroupB, "B")); results <- got }()
	for i := 0; i < 2; i++ {
		if got := <-results; !got.OK {
			t.Fatalf("result = %+v", got.Error)
		}
	}
}

func TestDIVAControlGlobalConcurrencyLimit(t *testing.T) {
	var active, peak atomic.Int32
	h := newDIVAControlHarness(t, divaControlConfig{
		maxConcurrent: 2,
		send: func(ctx context.Context, lc *LineClient, req *lineOutboundRequest) (*lineOutboundResult, error) {
			n := active.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(40 * time.Millisecond)
			active.Add(-1)
			return &lineOutboundResult{Sent: &line.Message{ID: "srv"}, SentPlaintext: true, SentAt: time.Now()}, nil
		},
	})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			chat := fmt.Sprintf("c%032d", i)
			body := divaTextBody(newDIVARequestID(), chat, "hi")
			body["options"] = map[string]any{"timeout_ms": 5000}
			if _, got := h.post(t, body); !got.OK {
				t.Errorf("result = %+v", got.Error)
			}
		}()
	}
	wg.Wait()
	if p := peak.Load(); p != 2 {
		t.Fatalf("peak concurrent sends = %d, want 2", p)
	}
}

// ---------------------------------------------------------------------------
// login selection
// ---------------------------------------------------------------------------

func divaTestLogin(mid string, loggedIn bool) *bridgev2.UserLogin {
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: networkid.UserLoginID(mid)}}
	lc := &LineClient{Mid: mid, UserLogin: login}
	if loggedIn {
		lc.AccessToken = "token"
	}
	login.Client = lc
	return login
}

func TestSelectDIVALogin(t *testing.T) {
	a := divaTestLogin("ua000000000000000000000000000000a", true)
	b := divaTestLogin("ub000000000000000000000000000000b", true)
	off := divaTestLogin("uc000000000000000000000000000000c", false)

	if lc, err := selectDIVALogin([]*bridgev2.UserLogin{a}, ""); err != nil || lc.Mid != a.Client.(*LineClient).Mid {
		t.Fatalf("single login: %v %v", lc, err)
	}
	if _, err := selectDIVALogin(nil, ""); err == nil || err.code != outboundNoActiveLogin {
		t.Fatalf("no login: %v", err)
	}
	if _, err := selectDIVALogin([]*bridgev2.UserLogin{a, b}, ""); err == nil || err.code != outboundNoActiveLogin {
		t.Fatalf("two logins without account_mid: %v", err)
	}
	if lc, err := selectDIVALogin([]*bridgev2.UserLogin{a, b}, "ub000000000000000000000000000000b"); err != nil || lc.Mid != "ub000000000000000000000000000000b" {
		t.Fatalf("account_mid: %v %v", lc, err)
	}
	if _, err := selectDIVALogin([]*bridgev2.UserLogin{a, b}, "ud000000000000000000000000000000d"); err == nil || err.code != outboundNoActiveLogin {
		t.Fatalf("unknown account_mid: %v", err)
	}
	if _, err := selectDIVALogin([]*bridgev2.UserLogin{off}, ""); err == nil || err.code != outboundLineSessionInvalid {
		t.Fatalf("only login disconnected: %v", err)
	}
	if lc, err := selectDIVALogin([]*bridgev2.UserLogin{a, off}, ""); err != nil || lc.Mid != a.Client.(*LineClient).Mid {
		t.Fatalf("one connected among two: %v %v", lc, err)
	}
}

func TestDIVAControlNoActiveLoginIsNotCached(t *testing.T) {
	var logins []*bridgev2.UserLogin
	var mu sync.Mutex
	h := newDIVAControlHarness(t, divaControlConfig{logins: func() []*bridgev2.UserLogin {
		mu.Lock()
		defer mu.Unlock()
		return logins
	}})
	body := divaTextBody(newDIVARequestID(), sendTestGroup, "hi")
	status, got := h.post(t, body)
	requireDIVAError(t, status, got, http.StatusServiceUnavailable, outboundNoActiveLogin, deliveryNotSent)

	mu.Lock()
	logins = []*bridgev2.UserLogin{h.env.lc.UserLogin}
	mu.Unlock()
	status, got = h.post(t, body)
	requireDIVAOK(t, status, got)
}

// ---------------------------------------------------------------------------
// deployment contract
// ---------------------------------------------------------------------------

func TestComposeKeepsDIVAControlInternal(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "ports:" {
			t.Fatal("docker-compose.yml publishes ports; the DIVA control endpoint must stay on the internal network (use expose)")
		}
	}
	if !strings.Contains(text, `- "8090"`) || !strings.Contains(text, "DIVA_CONTROL_TOKEN: ${DIVA_CONTROL_TOKEN:-}") {
		t.Fatal("docker-compose.yml does not expose the DIVA control endpoint internally")
	}
}
