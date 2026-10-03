package connector

// Group E2EE → plaintext fallback. One decision (fallBackToPlain in
// sendLineOutbound) covers text and media; these tests pin when it may and may
// not fire, and how the DIVA result reports it.

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

const talkExceptionMemberOff = `{"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","code":98,"reason":"member settings off"}}`

func requirePlaintextFallbackResult(t *testing.T, got outboundResult) {
	t.Helper()
	if !got.OK || got.Error != nil || got.Message == nil {
		t.Fatalf("result = %+v, want a successful send", got)
	}
	if got.Message.E2EE {
		t.Fatalf("message = %+v, want e2ee=false", *got.Message)
	}
	if !slices.Contains(got.Fallbacks, fallbackPlaintextNoE2EE) {
		t.Fatalf("fallbacks = %v, want %s", got.Fallbacks, fallbackPlaintextNoE2EE)
	}
}

// Production Gate 1: a cached group key let the bridge encrypt, but LINE
// rejected the E2EE message with code 98 because a member has Letter Sealing
// off. The same text must go out once more in plaintext.
func TestSendCoreMemberSettingsOffResendsGroupTextInPlaintext(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.fake.failNextSend(400, talkExceptionMemberOff)

	got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText, Text: "789"})
	requirePlaintextFallbackResult(t, got)

	sent := env.fake.sentMessages(t)
	if len(sent) != 2 {
		t.Fatalf("sendMessage calls = %d, want 2 (E2EE rejected, then plaintext)", len(sent))
	}
	first, second := sent[0].Msg, sent[1].Msg
	if len(first.Chunks) == 0 || first.ContentMetadata["e2eeVersion"] != "2" {
		t.Fatalf("first attempt = %+v, want E2EE", first)
	}
	if second.Text != "789" || len(second.Chunks) != 0 || second.ContentMetadata["e2eeVersion"] != "" ||
		second.To != sendTestGroup || second.ContentType != int(ContentText) {
		t.Fatalf("plaintext resend = %+v", second)
	}
	assertDistinctSeqs(t, sent)
	assertTracked(t, env.lc, sent[0].ReqSeq, sent[1].ReqSeq)
	if !env.lc.isGroupNoE2EE(sendTestGroup) {
		t.Fatal("group was not cached as no-E2EE after code 98")
	}

	// The cached decision sends the next message in plaintext directly.
	cryptoCalls := len(env.crypto.snapshot())
	got = env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText, Text: "again"})
	requirePlaintextFallbackResult(t, got)
	if n := len(env.fake.sentMessages(t)); n != 3 {
		t.Fatalf("sendMessage calls = %d, want 3", n)
	}
	if len(env.crypto.snapshot()) != cryptoCalls {
		t.Fatalf("no-E2EE cache ignored: crypto calls %d -> %d", cryptoCalls, len(env.crypto.snapshot()))
	}
}

// The pre-send path (no usable group key) uses the same decision and reports
// the same result shape.
func TestSendCoreGroupKeyUnavailableReportsPlaintextFallback(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.crypto.failGroup = 2

	got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText, Text: "789"})
	requirePlaintextFallbackResult(t, got)

	sent := env.fake.sentMessages(t)
	if len(sent) != 1 || sent[0].Msg.Text != "789" || len(sent[0].Msg.Chunks) != 0 {
		t.Fatalf("sent = %+v, want one plaintext message", sent)
	}
	if !env.lc.isGroupNoE2EE(sendTestGroup) {
		t.Fatal("group was not cached as no-E2EE")
	}
}

// Failures that do not prove the chat cannot use E2EE must never downgrade to
// plaintext or trigger a resend; their existing classification stays intact.
func TestSendCoreNonMemberSettingsErrorsDoNotFallBackToPlaintext(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T, env *sendTestEnv, attempts *int)
		code      outboundErrorCode
		delivery  outboundDelivery
		retryable bool
	}{
		{
			name: "session invalid",
			setup: func(t *testing.T, env *sendTestEnv, attempts *int) {
				oldRecover := recoverLineToken
				t.Cleanup(func() { recoverLineToken = oldRecover })
				recoverLineToken = func(*LineClient, context.Context) error { return errors.New("refresh failed") }
				env.fake.failNextSend(400, `{"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","code":119,"reason":"Access token refresh required"}}`)
			},
			code: outboundLineSessionInvalid, delivery: deliveryNotSent,
		},
		// Unknown targets are refused as TARGET_NOT_FOUND by the known-chat
		// gate before the send core runs. If LINE itself answers code 5 on
		// sendMessage, it keeps its existing (unguessed) classification.
		{
			name: "code 5 not found on send",
			setup: func(t *testing.T, env *sendTestEnv, attempts *int) {
				env.fake.failNextSend(400, talkExceptionNotFound)
			},
			code: outboundInternal, delivery: deliveryNotSent,
		},
		{
			name: "server error is unknown",
			setup: func(t *testing.T, env *sendTestEnv, attempts *int) {
				env.fake.failNextSend(502, "bad gateway")
			},
			code: outboundLineTransient, delivery: deliveryUnknown, retryable: true,
		},
		{
			name: "network error is unknown",
			setup: func(t *testing.T, env *sendTestEnv, attempts *int) {
				env.hookTransport(t, func(req *http.Request) (*http.Response, error) {
					if isSendMessage(req) {
						*attempts++
						return nil, errors.New("connection reset by peer")
					}
					return nil, nil
				})
			},
			code: outboundLineTransient, delivery: deliveryUnknown, retryable: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newSendTestEnv(t, true)
			// Sends the transport hook short-circuits never reach the fake.
			hookAttempts := 0
			tt.setup(t, env, &hookAttempts)

			got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText, Text: "789"})
			assertOutboundError(t, got, tt.code, tt.delivery, tt.retryable)
			if n := len(env.fake.sentMessages(t)) + hookAttempts; n != 1 {
				t.Fatalf("sendMessage calls = %d, want 1 (no plaintext resend)", n)
			}
			if env.lc.isGroupNoE2EE(sendTestGroup) {
				t.Fatal("group must not be cached as no-E2EE")
			}
		})
	}
}

func TestSendCoreMemberSettingsOffFallbackNeedsExactSignal(t *testing.T) {
	t.Run("other code 98 reason", func(t *testing.T) {
		env := newSendTestEnv(t, true)
		env.fake.failNextSend(400, `{"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","code":98,"reason":"different reason"}}`)
		got := env.sendText(t, t.Context(), &lineOutboundRequest{ContentType: ContentText, Text: "789"})
		if got.OK {
			t.Fatalf("result = %+v, want failure", got)
		}
		if n := len(env.fake.sentMessages(t)); n != 1 || env.lc.isGroupNoE2EE(sendTestGroup) {
			t.Fatalf("sendMessage calls = %d, noE2EE=%v; want no fallback", n, env.lc.isGroupNoE2EE(sendTestGroup))
		}
	})

	// Already plaintext: there is nothing to downgrade, so no second send.
	t.Run("plaintext send rejected", func(t *testing.T) {
		env := newSendTestEnv(t, false)
		env.fake.failNextSend(400, talkExceptionMemberOff)
		_, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
			ChatMID: sendTestGroup, ContentType: ContentText, Text: "789",
		})
		if err == nil || !line.IsMemberSettingsOffError(err) {
			t.Fatalf("err = %v, want the code 98 error", err)
		}
		if n := len(env.fake.sentMessages(t)); n != 1 {
			t.Fatalf("sendMessage calls = %d, want 1", n)
		}
	})
}

// A second code 98 on the plaintext resend is returned as-is: the fallback runs
// at most once per send.
func TestSendCoreMemberSettingsOffResendsAtMostOnce(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.fake.failNextSend(400, talkExceptionMemberOff)
	env.fake.failNextSend(400, talkExceptionMemberOff)

	_, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
		ChatMID: sendTestGroup, ContentType: ContentText, Text: "789",
	})
	if err == nil || !line.IsMemberSettingsOffError(err) {
		t.Fatalf("err = %v, want the code 98 error from the plaintext resend", err)
	}
	if n := len(env.fake.sentMessages(t)); n != 2 {
		t.Fatalf("sendMessage calls = %d, want 2", n)
	}
}
