package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
	"github.com/highesttt/matrix-line-messenger/pkg/ltsm"
)

const (
	defaultDIVAWebhookTimeout = 2 * time.Second
	defaultDIVASendTimeout    = 10 * time.Second
)

var divaHTTPClient = &http.Client{Timeout: defaultDIVAWebhookTimeout}

type divaInboundEvent struct {
	Text      string `json:"text"`
	GroupID   string `json:"group_id"`
	SenderID  string `json:"sender_id"`
	MessageID string `json:"message_id"`
}

type divaInboundDecision struct {
	OK        bool   `json:"ok"`
	ReplyText string `json:"reply_text"`
}

// Reasons a group/room message is withheld from the legacy (v1) DIVA inbound
// contract. v1 only carries text, so anything that is not a genuine user text
// message must not be forwarded as if it were one.
const (
	divaSkipNotText          = "not_text"
	divaSkipWrappedNotice    = "wrapped_notice"
	divaSkipDecryptionFailed = "decryption_failed"
	divaSkipEmptyText        = "empty_text"
)

// divaV1ForwardDecision decides whether an inbound LINE message may be sent to
// the DIVA worker under the v1 contract, which only has a "text" field.
//
//   - Non-text content types are withheld: for E2EE media the decrypted body is
//     the media keyMaterial JSON, not user text.
//   - Text-typed messages carrying ORGCONTP (call, device contact, and post
//     notifications) are LINE-generated notices, not user text.
//   - Messages that failed to decrypt have no text; v1 cannot express that.
//
// It returns an empty reason when the message should be forwarded.
func divaV1ForwardDecision(msg *line.Message, text string, decryptionFailed bool) string {
	switch {
	case ContentType(msg.ContentType) != ContentText:
		return divaSkipNotText
	case msg.ContentMetadata["ORGCONTP"] != "":
		return divaSkipWrappedNotice
	case decryptionFailed:
		return divaSkipDecryptionFailed
	case strings.TrimSpace(text) == "":
		return divaSkipEmptyText
	}
	return ""
}

// handleDIVAInbound is the single DIVA hook in the LINE receive path. It only
// considers group and room messages, logs a content-free debug record, and
// forwards genuine text messages to the DIVA worker. It is a no-op for the
// worker unless DIVA_WEBHOOK_URL is set.
func (lc *LineClient) handleDIVAInbound(msg *line.Message, chatMID, unwrappedText string, decryptionFailed bool) {
	if ToType(msg.ToType) != ToRoom && ToType(msg.ToType) != ToGroup {
		return
	}

	skipReason := divaV1ForwardDecision(msg, unwrappedText, decryptionFailed)

	// Never log message text or decrypted payloads here: for media the
	// decrypted body contains the media keyMaterial.
	lc.UserLogin.Bridge.Log.Debug().
		Str("diva_event", "DIVA_RX").
		Str("group_id", chatMID).
		Str("sender_id", msg.From).
		Str("message_id", msg.ID).
		Int("content_type", msg.ContentType).
		Bool("decryption_failed", decryptionFailed).
		Bool("forward", skipReason == "").
		Str("skip_reason", skipReason).
		Msg("[DIVA_RX]")

	if skipReason != "" {
		return
	}
	lc.forwardDIVAInbound(unwrappedText, chatMID, msg.From, msg.ID)
}

// forwardDIVAInbound forwards a decrypted LINE group message to the local
// DIVA worker. It is intentionally asynchronous: a DIVA worker outage must
// never block the LINE receive loop. If the worker returns reply_text, Go sends
// that text back to the same LINE chat.
func (lc *LineClient) forwardDIVAInbound(text, groupID, senderID, messageID string) {
	endpoint := strings.TrimSpace(os.Getenv("DIVA_WEBHOOK_URL"))
	if endpoint == "" {
		return
	}

	payload, err := json.Marshal(divaInboundEvent{
		Text:      text,
		GroupID:   groupID,
		SenderID:  senderID,
		MessageID: messageID,
	})
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("DIVA adapter failed to encode inbound event")
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultDIVAWebhookTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("DIVA adapter failed to build request")
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := divaHTTPClient.Do(req)
		if err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).Str("message_id", messageID).Msg("DIVA adapter delivery failed")
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lc.UserLogin.Bridge.Log.Warn().Int("status_code", resp.StatusCode).Str("message_id", messageID).Msg("DIVA adapter worker rejected event")
			return
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		if err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).Str("message_id", messageID).Msg("DIVA adapter failed to read worker response")
			return
		}

		var decision divaInboundDecision
		if len(body) > 0 {
			if err := json.Unmarshal(body, &decision); err != nil {
				lc.UserLogin.Bridge.Log.Warn().Err(err).Str("message_id", messageID).Msg("DIVA adapter worker returned invalid JSON")
				return
			}
		}

		lc.UserLogin.Bridge.Log.Debug().Str("message_id", messageID).Msg("DIVA adapter delivered inbound event")

		replyText := strings.TrimSpace(decision.ReplyText)
		if replyText == "" {
			return
		}

		sendCtx, sendCancel := context.WithTimeout(context.Background(), defaultDIVASendTimeout)
		defer sendCancel()
		if err := lc.sendDIVAText(sendCtx, groupID, replyText); err != nil {
			lc.UserLogin.Bridge.Log.Error().Err(err).
				Str("group_id", groupID).
				Str("message_id", messageID).
				Str("reply_text", replyText).
				Msg("DIVA auto-reply send failed")
			return
		}
		lc.UserLogin.Bridge.Log.Info().
			Str("group_id", groupID).
			Str("message_id", messageID).
			Str("reply_text", replyText).
			Msg("DIVA auto-reply sent")
	}()
}

// sendDIVAText sends one text message to a LINE chat using the same group E2EE
// behavior as the normal Matrix->LINE send path. This is deliberately narrow:
// text only, for the Server B command-path smoke test.
func (lc *LineClient) sendDIVAText(ctx context.Context, groupID, text string) error {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" || strings.TrimSpace(text) == "" {
		return errors.New("DIVA send requires group_id and text")
	}

	client := lc.newClient()
	plainText := lc.E2EE == nil || lc.isGroupNoE2EE(groupID)
	contentMetadata := map[string]string{}
	var chunks []string

	if !plainText {
		contentMetadata["e2eeVersion"] = "2"
		if errFetch := lc.fetchAndUnwrapGroupKey(ctx, groupID, 0); errFetch != nil {
			if errors.Is(errFetch, ltsm.ErrAbort) {
				return errFetch
			}
			if errFetch = lineGroupE2EEFetchFailureError(errFetch); errFetch != nil {
				return errFetch
			}
		}

		var err error
		chunks, err = lc.E2EE.EncryptGroupMessage(groupID, lc.midOrFallback(), text)
		if err != nil {
			if errors.Is(err, ltsm.ErrAbort) {
				return err
			}
			if errFetch := lc.fetchAndUnwrapGroupKey(ctx, groupID, 0); errFetch == nil {
				chunks, err = lc.E2EE.EncryptGroupMessage(groupID, lc.midOrFallback(), text)
			} else if errors.Is(errFetch, ltsm.ErrAbort) {
				return errFetch
			} else if errFetch = lineGroupE2EEFetchFailureError(errFetch); errFetch != nil {
				return errFetch
			}
			if err != nil {
				lc.markGroupNoE2EE(groupID)
				plainText = true
				chunks = nil
				delete(contentMetadata, "e2eeVersion")
			}
		}
	}

	buildMessage := func() (*line.Message, int) {
		now := time.Now().UnixMilli()
		msg := &line.Message{
			ID:              "local-diva-" + strconv.FormatInt(now, 10),
			From:            lc.midOrFallback(),
			To:              groupID,
			ToType:          int(guessToType(groupID)),
			SessionID:       0,
			CreatedTime:     json.Number(strconv.FormatInt(now, 10)),
			ContentType:     int(ContentText),
			HasContent:      false,
			ContentMetadata: contentMetadata,
		}
		if plainText {
			msg.Text = text
		} else {
			msg.Chunks = chunks
		}
		return msg, int(now % 1_000_000_000)
	}

	lineMsg, reqSeq := buildMessage()
	lc.trackReqSeq(reqSeq)
	var sentMsg *line.Message
	var err error
	client, sentMsg, err = callLineResultUsing(lc, ctx, client, func(client *line.Client) (*line.Message, error) {
		return client.SendMessage(int64(reqSeq), lineMsg)
	})
	_ = sentMsg

	if err != nil && !plainText && line.IsGroupKeyNotRegisteredError(err) {
		if regErr := lc.autoRegisterGroupKey(ctx, groupID); regErr != nil {
			return regErr
		}
		if fetchErr := lc.fetchAndUnwrapGroupKey(ctx, groupID, 0); fetchErr != nil {
			return fetchErr
		}
		chunks, err = lc.E2EE.EncryptGroupMessage(groupID, lc.midOrFallback(), text)
		if err != nil {
			return err
		}
		lineMsg, reqSeq = buildMessage()
		lineMsg.Chunks = chunks
		lineMsg.Text = ""
		lc.trackReqSeq(reqSeq)
		client, sentMsg, err = callLineResultUsing(lc, ctx, client, func(client *line.Client) (*line.Message, error) {
			return client.SendMessage(int64(reqSeq), lineMsg)
		})
		_ = sentMsg
	}

	return err
}
