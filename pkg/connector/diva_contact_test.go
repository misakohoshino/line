package connector

import (
	"encoding/json"
	"testing"
)

// Contact cards (好友資訊／名片) on /diva/v1/send — ServerB docs/26 §1F, AI_EXC #539:
// DC "加拒收／加問題" makes Server A drop the driver's card into the reject group,
// then type "🪪🪪🪪 <region> <拒收／問題>". The card carries the driver MID as
// Server A sees it; contact cards are not end-to-end encrypted on LINE.

const contactTestMID = "UUhbDoj4ND_fake-driver-mid-000000000000000"

func divaContactBody(requestID string, content map[string]any) []byte {
	raw, _ := json.Marshal(map[string]any{
		"version":      1,
		"request_id":   requestID,
		"target":       map[string]any{"chat_id": "CW7bSGKIZ9tILQ2InjdeU6S2cNM3BDQTw1uKh5YCiULw", "account_mid": nil},
		"message_type": "contact",
		"content":      content,
	})
	return raw
}

func TestDIVAContactSendParsing(t *testing.T) {
	id := newDIVARequestID()
	_, job, err := parseDIVASendRequest(divaContactBody(id, map[string]any{"mid": contactTestMID, "display_name": "  馬  "}))
	if err != nil {
		t.Fatalf("contact card rejected: %v", err)
	}
	if job.req.ContentType != ContentContact || job.req.ContactMID != contactTestMID || job.req.ContactName != "馬" {
		t.Fatalf("contact job = %+v", job.req)
	}
	if job.req.Text != "" || job.req.Media != nil || len(job.req.MentionMeta) != 0 {
		t.Fatalf("contact job carries text/media/mentions: %+v", job.req)
	}
	_, other, _ := parseDIVASendRequest(divaContactBody(id, map[string]any{"mid": contactTestMID, "display_name": "綸"}))
	if other.fingerprint == job.fingerprint {
		t.Fatal("different cards must not share a fingerprint (request_id reuse would return the wrong result)")
	}

	bad := []struct {
		name string
		body []byte
	}{
		{"no mid", divaContactBody(id, map[string]any{"display_name": "馬"})},
		{"mid not a user", divaContactBody(id, map[string]any{"mid": "Cgroup000", "display_name": "馬"})},
		{"unknown content field", divaContactBody(id, map[string]any{"mid": contactTestMID, "text": "x"})},
		{"name too long", divaContactBody(id, map[string]any{"mid": contactTestMID, "display_name": string(make([]rune, 101))})},
	}
	for _, c := range bad {
		if _, _, err := parseDIVASendRequest(c.body); err == nil || err.code != outboundInvalidRequest {
			t.Fatalf("%s: want invalid_request, got %v", c.name, err)
		}
	}
	var withMention map[string]any
	_ = json.Unmarshal(divaContactBody(id, map[string]any{"mid": contactTestMID}), &withMention)
	withMention["relations"] = map[string]any{"mentions": []any{map[string]any{"mid": contactTestMID, "name": "馬"}}}
	raw, _ := json.Marshal(withMention)
	if _, _, err := parseDIVASendRequest(raw); err == nil || err.code != outboundInvalidRequest {
		t.Fatalf("mentions on a contact card: want invalid_request, got %v", err)
	}
}

func TestSendCoreContactCardIsPlainInE2EEGroup(t *testing.T) {
	env := newSendTestEnv(t, true)
	_, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
		ChatMID: sendTestGroup, ContentType: ContentContact, ContactMID: contactTestMID, ContactName: "馬",
	})
	if err != nil {
		t.Fatalf("sendLineOutbound contact: %v", err)
	}
	assertMethods(t, env.fake, "sendMessage")
	assertCrypto(t, env.crypto) // 名片不加密：不抓群組金鑰、不加密
	msg := env.fake.sentMessages(t)[0].Msg
	if msg.ContentType != int(ContentContact) || msg.HasContent || len(msg.Chunks) != 0 || msg.Text != "" {
		t.Fatalf("contact message = %+v", msg)
	}
	if msg.ContentMetadata["mid"] != contactTestMID || msg.ContentMetadata["displayName"] != "馬" {
		t.Fatalf("contact metadata = %v", msg.ContentMetadata)
	}
	if _, ok := msg.ContentMetadata["e2eeVersion"]; ok {
		t.Fatalf("contact card must not be marked E2EE: %v", msg.ContentMetadata)
	}
}
