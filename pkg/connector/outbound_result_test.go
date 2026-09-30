package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/highesttt/matrix-line-messenger/pkg/e2ee"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// hookTransport lets a test fail selected requests before they reach the
// fake LINE server. hook returns (nil, nil) to pass the request through.
type hookTransport struct {
	next http.RoundTripper
	hook func(req *http.Request) (*http.Response, error)
}

func (h *hookTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if resp, err := h.hook(req); resp != nil || err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return resp, err
	}
	return h.next.RoundTrip(req)
}

func (env *sendTestEnv) hookTransport(t *testing.T, hook func(req *http.Request) (*http.Response, error)) {
	t.Helper()
	env.hookTransportWithTimeout(t, 0, hook)
}

// hookTransportWithTimeout also sets the HTTP client timeout, which is how a
// LINE call times out in production (rpcClientTimeout): SendMessage does not
// take the caller's context.
func (env *sendTestEnv) hookTransportWithTimeout(t *testing.T, timeout time.Duration, hook func(req *http.Request) (*http.Response, error)) {
	t.Helper()
	transport := &hookTransport{next: env.fake, hook: hook}
	newLineAPIClient = func(token string) *line.Client {
		client := line.NewClient(token)
		client.HTTPClient = &http.Client{Transport: transport, Timeout: timeout}
		client.OBSClient = &http.Client{Transport: transport, Timeout: timeout}
		return client
	}
}

func isSendMessage(req *http.Request) bool {
	return strings.HasSuffix(req.URL.Path, "/sendMessage")
}

func (env *sendTestEnv) sendText(t *testing.T, ctx context.Context, req *lineOutboundRequest) outboundResult {
	t.Helper()
	if req.ContentType == 0 && req.Text == "" {
		req.Text = "hello"
	}
	if req.ChatMID == "" {
		req.ChatMID = sendTestGroup
	}
	sent, err := env.lc.sendLineOutbound(ctx, req)
	return env.lc.buildOutboundResult(req.ChatMID, sent, err)
}

func assertOutboundError(t *testing.T, got outboundResult, code outboundErrorCode, delivery outboundDelivery, retryable bool) {
	t.Helper()
	if got.OK || got.Error == nil {
		t.Fatalf("result = %+v, want error %s", got, code)
	}
	if got.Error.Code != code || got.Error.Delivery != delivery || got.Error.Retryable != retryable {
		t.Fatalf("error = %+v, want code=%s delivery=%s retryable=%v", *got.Error, code, delivery, retryable)
	}
	if got.Error.Detail == "" {
		t.Fatal("error detail is empty")
	}
}

// ---------------------------------------------------------------------------
// success, result model and fallbacks
// ---------------------------------------------------------------------------

func TestOutboundResultSuccessJSONShape(t *testing.T) {
	env := newSendTestEnv(t, true)
	got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText, Text: "hello"})
	if !got.OK || got.Error != nil || got.Message == nil {
		t.Fatalf("result = %+v", got)
	}
	msg := env.fake.sentMessages(t)[0].Msg
	wantCreated, _ := msg.CreatedTime.Int64()
	if got.Message.ID != "srv-1" || got.Message.ChatID != sendTestGroup || !got.Message.E2EE || got.Message.CreatedAt != wantCreated {
		t.Fatalf("message = %+v (want created_at %d)", *got.Message, wantCreated)
	}
	raw, _ := json.Marshal(got)
	var decoded map[string]any
	_ = json.Unmarshal(raw, &decoded)
	if decoded["fallbacks"] == nil || decoded["deduplicated"] != false || decoded["ok"] != true {
		t.Fatalf("JSON = %s", raw)
	}
	if _, ok := decoded["error"]; ok {
		t.Fatalf("success JSON carries an error: %s", raw)
	}
	for _, key := range []string{"id", "chat_id", "created_at", "e2ee"} {
		if _, ok := decoded["message"].(map[string]any)[key]; !ok {
			t.Fatalf("message JSON missing %q: %s", key, raw)
		}
	}
}

func TestOutboundResultFallbackFlagsFollowCoreResult(t *testing.T) {
	t.Run("E2EE text has no fallback", func(t *testing.T) {
		env := newSendTestEnv(t, true)
		got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText})
		if len(got.Fallbacks) != 0 || !got.Message.E2EE {
			t.Fatalf("result = %+v", got)
		}
	})
	t.Run("E2EE failure falls back to plaintext", func(t *testing.T) {
		env := newSendTestEnv(t, true)
		env.crypto.failGroup = 2
		got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText})
		if fmt.Sprint(got.Fallbacks) != "[plaintext_no_e2ee]" || got.Message.E2EE {
			t.Fatalf("result = %+v", got)
		}
	})
	t.Run("account without E2EE is not a fallback", func(t *testing.T) {
		env := newSendTestEnv(t, false)
		got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText})
		if len(got.Fallbacks) != 0 || got.Message.E2EE {
			t.Fatalf("result = %+v", got)
		}
	})
	t.Run("reply dropped", func(t *testing.T) {
		env := newSendTestEnv(t, false)
		env.fake.failNextSend(400, talkExceptionNotFound)
		got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText, ReplyToID: "600000000000000123", ReplyFallback: true})
		if !got.OK || fmt.Sprint(got.Fallbacks) != "[reply_relation_dropped]" {
			t.Fatalf("result = %+v", got)
		}
	})
	t.Run("zip wrapped", func(t *testing.T) {
		env := newSendTestEnv(t, false)
		env.fake.failNextSend(200, extensionRejectsFile)
		got := env.sendText(t, t.Context(), &lineOutboundRequest{
			ContentType: ContentFile,
			Media:       &outboundMedia{Data: []byte("x"), FileName: "a.xyz"},
		})
		if !got.OK || fmt.Sprint(got.Fallbacks) != "[zip_wrapped]" {
			t.Fatalf("result = %+v", got)
		}
	})
}

// ---------------------------------------------------------------------------
// errors through the shared core
// ---------------------------------------------------------------------------

func TestOutboundErrorReplyTargetMissingWithoutFallback(t *testing.T) {
	env := newSendTestEnv(t, false)
	env.fake.failNextSend(400, talkExceptionNotFound)
	got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText, ReplyToID: "600000000000000123"})
	assertOutboundError(t, got, outboundReplyTargetNotFound, deliveryNotSent, false)
	if got.Message != nil {
		t.Fatalf("failed send carries a message: %+v", got.Message)
	}
}

// Without a reply relation, LINE's code 5 "not found" does not say what was
// missing, so it is not guessed as TARGET_NOT_FOUND.
func TestOutboundErrorNotFoundWithoutReplyIsNotGuessed(t *testing.T) {
	env := newSendTestEnv(t, false)
	env.fake.failNextSend(400, talkExceptionNotFound)
	got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText})
	assertOutboundError(t, got, outboundInternal, deliveryNotSent, false)
}

func TestOutboundErrorNotAMember(t *testing.T) {
	env := newSendTestEnv(t, false)
	env.fake.failNextSend(400, `{"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","message":"TalkException","code":10,"reason":"not a member","parameterMap":null}}`)
	got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText})
	assertOutboundError(t, got, outboundTargetNotFound, deliveryNotSent, false)
}

func TestOutboundErrorBlocked(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.lc.blockedUsers[sendTestPeer] = true
	got := env.sendText(t, t.Context(), &lineOutboundRequest{ChatMID: sendTestPeer, ContentType: ContentText})
	assertOutboundError(t, got, outboundBlocked, deliveryNotSent, false)
}

func TestOutboundErrorOwnE2EEKeyMissing(t *testing.T) {
	t.Run("group key fetch targets a missing private key", func(t *testing.T) {
		env := newSendTestEnv(t, true)
		env.crypto.fetchErr = fmt.Errorf("unwrap: %w", e2ee.ErrMissingOwnPrivateKey)
		got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText})
		assertOutboundError(t, got, outboundE2EEKeyUnavailable, deliveryNotSent, false)
	})
	t.Run("own key not loaded for 1:1", func(t *testing.T) {
		env := newSendTestEnv(t, true)
		e2eeMyKeyIDs = func(*e2ee.Manager) (int, int, error) { return 0, 0, errors.New("my key not loaded") }
		got := env.sendText(t, t.Context(), &lineOutboundRequest{ChatMID: sendTestPeer, ContentType: ContentText})
		assertOutboundError(t, got, outboundE2EEKeyUnavailable, deliveryNotSent, false)
		if n := len(env.fake.sentMessages(t)); n != 0 {
			t.Fatalf("sendMessage calls = %d", n)
		}
	})
	t.Run("group key not registered and registration failed", func(t *testing.T) {
		env := newSendTestEnv(t, true)
		env.fake.failNextSend(400, talkExceptionNoGroupK)
		lineAutoRegisterGroupKey = func(*LineClient, context.Context, string) error { return errors.New("register failed") }
		got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText})
		assertOutboundError(t, got, outboundE2EEKeyUnavailable, deliveryNotSent, false)
	})
}

func TestOutboundErrorSessionInvalid(t *testing.T) {
	env := newSendTestEnv(t, false)
	oldRecover := recoverLineToken
	t.Cleanup(func() { recoverLineToken = oldRecover })
	recoverLineToken = func(*LineClient, context.Context) error { return errors.New("refresh failed") }
	env.fake.failNextSend(400, `{"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","code":119,"reason":"Access token refresh required"}}`)
	got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText})
	assertOutboundError(t, got, outboundLineSessionInvalid, deliveryNotSent, false)
}

func TestOutboundErrorNetworkOnSendIsUnknown(t *testing.T) {
	env := newSendTestEnv(t, false)
	env.hookTransport(t, func(req *http.Request) (*http.Response, error) {
		if isSendMessage(req) {
			return nil, errors.New("connection reset by peer")
		}
		return nil, nil
	})
	got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText})
	assertOutboundError(t, got, outboundLineTransient, deliveryUnknown, true)
}

func TestOutboundErrorServerErrorOnSendIsUnknown(t *testing.T) {
	env := newSendTestEnv(t, false)
	env.fake.failNextSend(502, "bad gateway")
	got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText})
	assertOutboundError(t, got, outboundLineTransient, deliveryUnknown, true)
}

func TestOutboundErrorTimeoutOnSendIsUnknown(t *testing.T) {
	env := newSendTestEnv(t, false)
	env.hookTransportWithTimeout(t, 200*time.Millisecond, func(req *http.Request) (*http.Response, error) {
		if isSendMessage(req) {
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-time.After(5 * time.Second):
				t.Error("HTTP client timeout did not cancel the request")
				return nil, errors.New("test transport gave up")
			}
		}
		return nil, nil
	})
	got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText})
	if !strings.Contains(got.Error.Detail, "sendMessage") || !strings.Contains(got.Error.Detail, "deadline exceeded") {
		t.Fatalf("detail = %q, want the sendMessage deadline", got.Error.Detail)
	}
	assertOutboundError(t, got, outboundTimeout, deliveryUnknown, true)
}

func TestOutboundErrorUploadBeforeSendIsNotSent(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.hookTransport(t, func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == "obs.line-apps.com" {
			return nil, errors.New("connection reset by peer")
		}
		return nil, nil
	})
	got := env.sendText(t, t.Context(), &lineOutboundRequest{
		ContentType: ContentImage,
		Media:       &outboundMedia{Data: testPNG(t), MimeType: "image/png"},
	})
	assertOutboundError(t, got, outboundUploadFailed, deliveryNotSent, true)
	if n := len(env.fake.sentMessages(t)); n != 0 {
		t.Fatalf("sendMessage calls = %d", n)
	}
}

func TestOutboundErrorUploadAfterSendIsSent(t *testing.T) {
	env := newSendTestEnv(t, false)
	env.hookTransport(t, func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/r/talk/m/srv-1" {
			return fakeHTTPResponse(req, 500, "obs down", nil), nil
		}
		return nil, nil
	})
	got := env.sendText(t, t.Context(), &lineOutboundRequest{
		ContentType: ContentImage,
		Media:       &outboundMedia{Data: testPNG(t), MimeType: "image/png"},
	})
	assertOutboundError(t, got, outboundUploadFailed, deliverySent, true)
	if got.Message == nil || got.Message.ID != "srv-1" || got.Message.E2EE {
		t.Fatalf("sent message missing from result: %+v", got.Message)
	}
}

func TestOutboundErrorInvalidAndUnsupported(t *testing.T) {
	env := newSendTestEnv(t, false)
	got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentImage})
	assertOutboundError(t, got, outboundInvalidRequest, deliveryNotSent, false)

	got = env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentSticker, Text: "x"})
	assertOutboundError(t, got, outboundUnsupportedMessageType, deliveryNotSent, false)

	err := env.lc.sendDIVAReplyText(t.Context(), " ", "789")
	if c := classifyOutboundError(err); c.Code != outboundInvalidRequest || c.Delivery != deliveryNotSent {
		t.Fatalf("DIVA empty chat = %+v", c)
	}
	if n := len(env.fake.snapshot()); n != 0 {
		t.Fatalf("LINE calls = %d", n)
	}
}

// ---------------------------------------------------------------------------
// classification of errors produced elsewhere in the connector
// ---------------------------------------------------------------------------

func TestClassifyOutboundErrorTable(t *testing.T) {
	sendAttempt := func(err error) error { return markOutboundFailure(failSendAttempt, err) }
	tests := []struct {
		name      string
		err       error
		code      outboundErrorCode
		delivery  outboundDelivery
		retryable bool
	}{
		{"session invalidated", errLineSessionInvalidated, outboundLineSessionInvalid, deliveryNotSent, false},
		{"client superseded", fmt.Errorf("x: %w", errLineClientSuperseded), outboundLineSessionInvalid, deliveryNotSent, false},
		{"logged out on send", sendAttempt(errors.New("V3_TOKEN_CLIENT_LOGGED_OUT")), outboundLineSessionInvalid, deliveryNotSent, false},
		{"recovery failed", sendAttempt(fmt.Errorf("failed to recover token after LINE auth error (%w): %w",
			errors.New(`API error 400: {"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","message":"TalkException","code":83,"reason":"invalid sender key","parameterMap":null}}`),
			errors.New("login failed"))), outboundLineSessionInvalid, deliveryNotSent, false},
		{"reconnect notice status", lineGroupE2EEReconnectRequiredError(fmt.Errorf("x: %w", e2ee.ErrMissingOwnPrivateKey)), outboundE2EEKeyUnavailable, deliveryNotSent, false},
		{"group key not loaded", fmt.Errorf("failed to encrypt zipped file message: %w", e2ee.ErrGroupKeyNotLoaded), outboundE2EEKeyUnavailable, deliveryNotSent, false},
		{"canceled on send", sendAttempt(fmt.Errorf("request failed: %w", context.Canceled)), outboundLineTransient, deliveryUnknown, true},
		{"deadline before send", fmt.Errorf("failed to determine flow: %w", context.DeadlineExceeded), outboundTimeout, deliveryNotSent, true},
		{"sendMessage code != 0", sendAttempt(errors.New("sendMessage failed: something")), outboundInternal, deliveryNotSent, false},
		{"unknown", errors.New("boom"), outboundInternal, deliveryNotSent, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyOutboundError(tt.err)
			if got.Code != tt.code || got.Delivery != tt.delivery || got.Retryable != tt.retryable {
				t.Fatalf("classify = %+v, want %s/%s/%v", got, tt.code, tt.delivery, tt.retryable)
			}
		})
	}
}

// The markers must not change what Matrix sees.
func TestOutboundFailureKeepsErrorText(t *testing.T) {
	inner := errors.New(`API error 400: {"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","code":5,"reason":"not found"}}`)
	marked := markOutboundFailure(failSendAttempt, inner)
	if marked.Error() != inner.Error() || !errors.Is(marked, inner) || !line.IsTalkExceptionNotFound(marked) {
		t.Fatalf("marker changed the error: %v", marked)
	}
	if markOutboundFailure(failUpload, nil) != nil {
		t.Fatal("marking nil must stay nil")
	}
	long := classifyOutboundError(errors.New(strings.Repeat("x", 2000)))
	if len(long.Detail) != outboundErrorDetailMax {
		t.Fatalf("detail length = %d", len(long.Detail))
	}
}

func TestOutboundErrorCodesAreTheAgreedTaxonomy(t *testing.T) {
	codes := []outboundErrorCode{
		outboundInvalidRequest, outboundUnauthorized, outboundUnsupportedMessageType, outboundNoActiveLogin,
		outboundLineSessionInvalid, outboundTargetNotFound, outboundBlocked, outboundE2EEKeyUnavailable,
		outboundReplyTargetNotFound, outboundUploadFailed, outboundLineTransient, outboundTimeout, outboundInternal,
	}
	want := "INVALID_REQUEST UNAUTHORIZED UNSUPPORTED_MESSAGE_TYPE NO_ACTIVE_LOGIN LINE_SESSION_INVALID " +
		"TARGET_NOT_FOUND BLOCKED E2EE_KEY_UNAVAILABLE REPLY_TARGET_NOT_FOUND UPLOAD_FAILED LINE_TRANSIENT TIMEOUT INTERNAL"
	var got []string
	for _, c := range codes {
		got = append(got, string(c))
	}
	if strings.Join(got, " ") != want {
		t.Fatalf("codes = %v", got)
	}
}
