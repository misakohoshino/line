package connector

// Tests for the shared send core API itself (sendLineOutbound), as used by
// callers other than the Matrix adapter. The Matrix and DIVA behaviour is
// pinned by the characterization tests in send_core_test.go.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

func TestSendLineOutboundResultForPlainText(t *testing.T) {
	env := newSendTestEnv(t, false)
	result, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
		ChatMID: sendTestGroup, ContentType: ContentText, Text: "hello",
	})
	if err != nil {
		t.Fatalf("sendLineOutbound: %v", err)
	}
	if result.Sent == nil || result.Sent.ID != "srv-1" || !result.SentPlaintext || result.ReplyDropped || result.ZipWrapped || result.SentAt.IsZero() {
		t.Fatalf("result = %+v", result)
	}
}

func TestSendLineOutboundResultForE2EEText(t *testing.T) {
	env := newSendTestEnv(t, true)
	result, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
		ChatMID: sendTestGroup, ContentType: ContentText, Text: "hello",
	})
	if err != nil {
		t.Fatalf("sendLineOutbound: %v", err)
	}
	if result.SentPlaintext {
		t.Fatalf("E2EE send reported as plaintext: %+v", result)
	}
}

func TestSendLineOutboundReplyFallbackIsTheCallersChoice(t *testing.T) {
	t.Run("fallback on", func(t *testing.T) {
		env := newSendTestEnv(t, false)
		env.fake.failNextSend(400, talkExceptionNotFound)
		result, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
			ChatMID: sendTestGroup, ContentType: ContentText, Text: "收到",
			ReplyToID: "600000000000000123", ReplyFallback: true,
		})
		if err != nil {
			t.Fatalf("sendLineOutbound: %v", err)
		}
		if !result.ReplyDropped || len(env.fake.sentMessages(t)) != 2 {
			t.Fatalf("result = %+v, sends = %d", result, len(env.fake.sentMessages(t)))
		}
	})
	t.Run("fallback off", func(t *testing.T) {
		env := newSendTestEnv(t, false)
		env.fake.failNextSend(400, talkExceptionNotFound)
		result, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
			ChatMID: sendTestGroup, ContentType: ContentText, Text: "收到",
			ReplyToID: "600000000000000123",
		})
		if err == nil || !line.IsTalkExceptionNotFound(err) || result != nil {
			t.Fatalf("result = %+v, err = %v; want reply-target-not-found error", result, err)
		}
		if n := len(env.fake.sentMessages(t)); n != 1 {
			t.Fatalf("sendMessage calls = %d, want 1 (no resend without the reply)", n)
		}
	})
}

func TestSendLineOutboundReportsZipWrapping(t *testing.T) {
	env := newSendTestEnv(t, false)
	env.fake.failNextSend(200, extensionRejectsFile)
	result, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
		ChatMID: sendTestGroup, ContentType: ContentFile,
		Media: &outboundMedia{Data: []byte("file bytes"), FileName: "a.xyz"},
	})
	if err != nil {
		t.Fatalf("sendLineOutbound: %v", err)
	}
	if !result.ZipWrapped || result.Sent.ID != "srv-1" {
		t.Fatalf("result = %+v", result)
	}
}

// A non-Matrix caller passes media bytes directly.
func TestSendLineOutboundMediaFromBytes(t *testing.T) {
	env := newSendTestEnv(t, false)
	data := testPNG(t)
	result, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
		ChatMID: sendTestGroup, ContentType: ContentImage,
		Media: &outboundMedia{Data: data, MimeType: "image/png"},
	})
	if err != nil {
		t.Fatalf("sendLineOutbound: %v", err)
	}
	calls := env.fake.snapshot()
	if len(calls) < 3 || calls[2].Method != "OBS" || !bytes.Equal(calls[2].Body, data) {
		t.Fatalf("image bytes were not uploaded after send: %q", env.fake.methods())
	}
	if name := env.fake.sentMessages(t)[0].Msg.ContentMetadata["FILE_NAME"]; name != "image.png" {
		t.Fatalf("default FILE_NAME = %q", name)
	}
	if !result.SentPlaintext {
		t.Fatalf("result = %+v", result)
	}
}

func TestSendLineOutboundMediaErrors(t *testing.T) {
	env := newSendTestEnv(t, false)
	if _, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
		ChatMID: sendTestGroup, ContentType: ContentImage,
	}); err == nil {
		t.Fatal("image without media did not fail")
	}

	loadErr := errors.New("download failed")
	_, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
		ChatMID: sendTestGroup, ContentType: ContentVideo,
		Media: &outboundMedia{
			Load:     func(context.Context) ([]byte, error) { return nil, loadErr },
			Duration: 1,
		},
	})
	if !errors.Is(err, loadErr) {
		t.Fatalf("error = %v, want the loader error unchanged", err)
	}
	if calls := env.fake.methods(); len(calls) != 0 {
		t.Fatalf("LINE was called for a failed media load: %q", calls)
	}
}

func TestSendLineOutboundConcurrentSendsUseDistinctTrackedReqSeqs(t *testing.T) {
	env := newSendTestEnv(t, false)
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
				ChatMID: sendTestGroup, ContentType: ContentText, Text: fmt.Sprintf("m%d", i),
			})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("sendLineOutbound: %v", err)
		}
	}
	sent := env.fake.sentMessages(t)
	if len(sent) != n {
		t.Fatalf("sendMessage calls = %d, want %d", len(sent), n)
	}
	assertDistinctSeqs(t, sent)
	for _, s := range sent {
		assertTracked(t, env.lc, s.ReqSeq)
	}
}

func TestHandleMatrixMessageUnsupportedTypeSendsNothing(t *testing.T) {
	env := newSendTestEnv(t, false)
	_, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, &event.MessageEventContent{
		MsgType: event.MsgEmote, Body: "waves",
	}))
	if err == nil || err.Error() != "message type m.emote not implemented" {
		t.Fatalf("error = %v", err)
	}
	if n := len(env.fake.sentMessages(t)); n != 0 {
		t.Fatalf("sendMessage calls = %d", n)
	}
}
