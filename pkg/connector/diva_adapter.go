package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"
)

const defaultDIVAWebhookTimeout = 2 * time.Second

var divaHTTPClient = &http.Client{Timeout: defaultDIVAWebhookTimeout}

type divaInboundEvent struct {
	Text      string `json:"text"`
	GroupID   string `json:"group_id"`
	SenderID  string `json:"sender_id"`
	MessageID string `json:"message_id"`
}

// forwardDIVAInbound forwards a decrypted LINE group message to the local
// DIVA worker. It is intentionally best-effort and asynchronous: a DIVA worker
// outage must never block or break the LINE bridge itself.
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

		lc.UserLogin.Bridge.Log.Debug().Str("message_id", messageID).Msg("DIVA adapter delivered inbound event")
	}()
}
