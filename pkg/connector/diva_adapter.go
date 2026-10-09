package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/highesttt/matrix-line-messenger/pkg/connector/handlers"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

const (
	defaultDIVAWebhookTimeout      = 2 * time.Second
	defaultDIVAMediaWebhookTimeout = 2 * time.Minute
	defaultDIVASendTimeout         = 10 * time.Second
	defaultDIVAInboundMediaMax     = 32 << 20
	divaInboundMediaMaxConcurrent  = 2
	divaEncryptedMediaOverhead     = 32
)

var (
	divaHTTPClient      = &http.Client{Timeout: defaultDIVAWebhookTimeout}
	divaMediaHTTPClient = &http.Client{Timeout: defaultDIVAMediaWebhookTimeout}
	divaInboundMediaSem = make(chan struct{}, divaInboundMediaMaxConcurrent)
)

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

type divaInboundMedia struct {
	Data     []byte
	MimeType string
	FileName string
}

type divaInboundMediaLoader func(context.Context) (*divaInboundMedia, error)

var divaPrepareInboundMedia = func(lc *LineClient, ctx context.Context, kind handlers.MediaKind, msg line.Message, decryptedBody string) (*divaInboundMedia, error) {
	return lc.prepareDIVAInboundMedia(ctx, kind, msg, decryptedBody)
}

func divaInboundMediaMaxBytes() int64 {
	raw := strings.TrimSpace(os.Getenv("DIVA_MEDIA_MAX_BYTES"))
	if raw == "" {
		return defaultDIVAInboundMediaMax
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || parsed <= 0 {
		return defaultDIVAInboundMediaMax
	}
	return parsed
}

func cloneDIVAMediaMessage(msg *line.Message) line.Message {
	copyMsg := line.Message{ID: msg.ID}
	if len(msg.ContentMetadata) > 0 {
		copyMsg.ContentMetadata = make(map[string]string, len(msg.ContentMetadata))
		for key, value := range msg.ContentMetadata {
			copyMsg.ContentMetadata[key] = value
		}
	}
	return copyMsg
}

func divaMediaKindForContentType(contentType ContentType) (handlers.MediaKind, bool) {
	switch contentType {
	case ContentImage:
		return handlers.MediaKindImage, true
	case ContentVideo:
		return handlers.MediaKindVideo, true
	case ContentAudio:
		return handlers.MediaKindAudio, true
	case ContentFile:
		return handlers.MediaKindFile, true
	default:
		return "", false
	}
}

func divaMediaFileName(kind handlers.MediaKind, msg line.Message, decryptedBody, mimeType string) (string, error) {
	name := strings.TrimSpace(msg.ContentMetadata["FILE_NAME"])
	switch kind {
	case handlers.MediaKindImage:
		if name != "" {
			return name, nil
		}
		switch mimeType {
		case "image/png":
			return "image.png", nil
		case "image/gif":
			return "image.gif", nil
		default:
			return "image.jpg", nil
		}
	case handlers.MediaKindVideo:
		if name == "" && strings.Contains(decryptedBody, "fileName") {
			var payload struct {
				FileName string `json:"fileName"`
			}
			if err := json.Unmarshal([]byte(decryptedBody), &payload); err == nil {
				name = strings.TrimSpace(payload.FileName)
			}
		}
		if name == "" {
			name = "video.mp4"
		}
		return name, nil
	case handlers.MediaKindAudio:
		return "audio.m4a", nil
	case handlers.MediaKindFile:
		if strings.Contains(decryptedBody, "fileName") {
			var payload struct {
				FileName string `json:"fileName"`
			}
			if err := json.Unmarshal([]byte(decryptedBody), &payload); err != nil {
				return "", fmt.Errorf("failed to parse file payload JSON: %w", err)
			}
			if bodyName := strings.TrimSpace(payload.FileName); bodyName != "" {
				name = bodyName
			}
		}
		if name == "" {
			name = "file.bin"
		}
		return name, nil
	default:
		return "", fmt.Errorf("unsupported DIVA inbound media kind %q", kind)
	}
}

func divaMediaMimeType(kind handlers.MediaKind, media []byte, fileName string) (string, error) {
	switch kind {
	case handlers.MediaKindImage:
		detected := http.DetectContentType(media)
		switch detected {
		case "image/jpeg", "image/png", "image/gif":
			return detected, nil
		default:
			return "", fmt.Errorf("LINE image has unsupported media type %q", detected)
		}
	case handlers.MediaKindVideo:
		if strings.HasSuffix(strings.ToLower(fileName), ".webm") {
			return "video/webm", nil
		}
		return "video/mp4", nil
	case handlers.MediaKindAudio:
		return "audio/mp4", nil
	case handlers.MediaKindFile:
		if strings.HasSuffix(strings.ToLower(fileName), ".pdf") {
			return "application/pdf", nil
		}
		return "application/octet-stream", nil
	default:
		return "", fmt.Errorf("unsupported DIVA inbound media kind %q", kind)
	}
}

func (lc *LineClient) prepareDIVAInboundMedia(ctx context.Context, kind handlers.MediaKind, msg line.Message, decryptedBody string) (*divaInboundMedia, error) {
	maxBytes := divaInboundMediaMaxBytes()
	if rawSize := strings.TrimSpace(msg.ContentMetadata["FILE_SIZE"]); rawSize != "" {
		if size, err := strconv.ParseInt(rawSize, 10, 64); err == nil && size > maxBytes+divaEncryptedMediaOverhead {
			return nil, fmt.Errorf("LINE %s metadata size %d exceeds DIVA media limit %d", kind, size, maxBytes)
		}
	}

	h := lc.newMessageHandler()
	fetched, err := h.FetchMedia(ctx, kind, msg, decryptedBody)
	if err != nil {
		return nil, err
	}
	if fetched == nil {
		return nil, fmt.Errorf("LINE %s has no fetchable media source", kind)
	}
	if int64(len(fetched.Data)) > maxBytes+divaEncryptedMediaOverhead {
		return nil, fmt.Errorf("LINE %s download size %d exceeds DIVA media limit %d", kind, len(fetched.Data), maxBytes)
	}
	mediaData, err := h.DecryptFetchedMedia(fetched.Data, decryptedBody, msg.ContentMetadata, kind)
	if err != nil {
		return nil, err
	}
	if len(mediaData) == 0 {
		return nil, fmt.Errorf("LINE %s decrypted to empty media", kind)
	}
	if int64(len(mediaData)) > maxBytes {
		return nil, fmt.Errorf("LINE %s size %d exceeds DIVA media limit %d", kind, len(mediaData), maxBytes)
	}
	preliminaryName, err := divaMediaFileName(kind, msg, decryptedBody, "")
	if err != nil {
		return nil, err
	}
	mimeType, err := divaMediaMimeType(kind, mediaData, preliminaryName)
	if err != nil {
		return nil, err
	}
	fileName, err := divaMediaFileName(kind, msg, decryptedBody, mimeType)
	if err != nil {
		return nil, err
	}
	return &divaInboundMedia{Data: mediaData, MimeType: mimeType, FileName: fileName}, nil
}

func buildDIVAInboundMultipart(metadata []byte, media *divaInboundMedia) ([]byte, string, error) {
	if media == nil || len(media.Data) == 0 {
		return nil, "", errors.New("DIVA inbound media is empty")
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	metadataHeader := make(textproto.MIMEHeader)
	metadataHeader.Set("Content-Disposition", `form-data; name="metadata"`)
	metadataHeader.Set("Content-Type", "application/json; charset=utf-8")
	metadataPart, err := writer.CreatePart(metadataHeader)
	if err != nil {
		return nil, "", err
	}
	if _, err = metadataPart.Write(metadata); err != nil {
		return nil, "", err
	}

	mediaHeader := make(textproto.MIMEHeader)
	fileName := strings.TrimSpace(media.FileName)
	if fileName == "" {
		fileName = "media.bin"
	}
	mediaHeader.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{
		"name": "media", "filename": fileName,
	}))
	mediaHeader.Set("Content-Type", media.MimeType)
	mediaPart, err := writer.CreatePart(mediaHeader)
	if err != nil {
		return nil, "", err
	}
	if _, err = mediaPart.Write(media.Data); err != nil {
		return nil, "", err
	}
	if err = writer.Close(); err != nil {
		return nil, "", err
	}
	return body.Bytes(), writer.FormDataContentType(), nil
}

// Reasons a group/room message is withheld from the legacy (v1) DIVA inbound
// contract. v1 only carries text, so anything that is not a genuine user text
// message must not be forwarded as if it were one.
const (
	divaSkipNotText          = "not_text"
	divaSkipWrappedNotice    = "wrapped_notice"
	divaSkipDecryptionFailed = "decryption_failed"
	divaSkipEmptyText        = "empty_text"
	divaSkipBackfill         = "backfill"
)

var divaContractWarnOnce sync.Once

// divaContractVersion reads DIVA_CONTRACT_VERSION. Unset or "1" selects the
// legacy v1 payload, "2" selects the v2 envelope. Any other value falls back
// to v1, so rolling back is always just unsetting or resetting the variable.
func (lc *LineClient) divaContractVersion() int {
	raw := strings.TrimSpace(os.Getenv("DIVA_CONTRACT_VERSION"))
	switch raw {
	case "", "1":
		return 1
	case "2":
		return divaContractV2
	}
	divaContractWarnOnce.Do(func() {
		lc.UserLogin.Bridge.Log.Warn().
			Str("value", raw).
			Msg("Unknown DIVA_CONTRACT_VERSION, using the v1 inbound contract")
	})
	return 1
}

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

// handleDIVAInbound is the single DIVA hook in the LINE receive path. Normal
// DIVA traffic stays group/room-only. LINE-1D-C additionally admits direct
// ORGCONTP=CALL summaries so Server A can tell a caller that this account does
// not accept calls; ordinary direct messages remain outside DIVA.
//
// Under v1 it forwards genuine live text only; backfill cannot be labelled in
// v1, so it is not forwarded at all. Under v2 every admitted bridgeable message
// is forwarded with origin, is_from_me and decryption_failed so Server A can
// tell them apart. It is a no-op for the worker unless DIVA_WEBHOOK_URL is set.
func divaIsDirectCall(msg *line.Message, opType int) bool {
	if msg == nil || ToType(msg.ToType) != ToUser || opType != int(OpReceiveMessage) {
		return false
	}
	if msg.ContentMetadata["ORGCONTP"] == "CALL" {
		return true
	}
	// Production handset evidence: current LINE Chrome/SSE emits a direct call
	// notice as ContentType=0 with exactly one MESSAGE_TARGET metadata field.
	// A normal encrypted direct text on the same account instead carries only
	// e2eeVersion, so ordinary direct messages remain outside the DIVA gate.
	return ContentType(msg.ContentType) == ContentText &&
		len(msg.ContentMetadata) == 1 &&
		msg.ContentMetadata["MESSAGE_TARGET"] != ""
}

// handleDIVAGroupOperation forwards the normalized live membership events used
// by LINE-1D-D. v1 has no generic event envelope, so it intentionally remains
// a no-op unless contract v2 is enabled. Group events can never auto-reply.
func (lc *LineClient) handleDIVAGroupOperation(op line.Operation) {
	if lc.divaContractVersion() != divaContractV2 {
		return
	}
	event, ok := lc.buildDIVAV2LiveGroupEvent(op)
	if !ok {
		lc.UserLogin.Bridge.Log.Debug().
			Int("op_type", op.Type).
			Str("revision", op.Revision.String()).
			Str("param1", op.Param1).
			Str("param2", op.Param2).
			Str("param3", op.Param3).
			Msg("[DIVA_GROUP_EVENT] unsupported live membership operation shape")
		return
	}

	if !lc.admitDIVAGroupEvent(event, op.Revision.String()) {
		return
	}

	actorMID := ""
	if event.GroupEvent.Actor != nil {
		actorMID = event.GroupEvent.Actor.Mid
	}
	lc.UserLogin.Bridge.Log.Debug().
		Str("diva_event", "DIVA_GROUP_EVENT").
		Str("event_id", event.EventID).
		Str("group_id", event.Chat.ID).
		Str("group_event_type", event.GroupEvent.Type).
		Str("member_id", event.GroupEvent.Member.Mid).
		Str("actor_id", actorMID).
		Msg("[DIVA_GROUP_EVENT]")

	encode := func() ([]byte, error) { return json.Marshal(event) }
	lc.forwardDIVAInbound(encode, nil, event.Chat.ID, event.EventID, false)
}

// divaGroupLeaveWindow bounds how long a live member_removed (op 133) claims
// the trailing member_left (op 61) for the same chat member. When another
// member kicks someone, LINE sends op 133 (remover known) and then op 61 (the
// generic "no longer in the chat" notice) for the same logical removal. A
// genuine self leave only sends op 61.
const divaGroupLeaveWindow = 10 * time.Second

// divaGroupEventNow is the wall clock for leave markers. It is a variable only
// so tests can move time without sleeping; production always uses time.Now.
var divaGroupEventNow = time.Now

type divaGroupLeaveKey struct {
	account string
	chat    string
	member  string
}

// divaGroupLeaveMarker remembers the last forwarded leave-type event for one
// chat member. It is memory-only: a restart in the middle of a kick only means
// DIVA receives both events, exactly as before this normalization existed.
type divaGroupLeaveMarker struct {
	eventType  string
	revision   string
	receivedAt time.Time
}

// admitDIVAGroupEvent decides whether a normalized live group event is
// forwarded to DIVA. It only affects the DIVA copy of the operation; the
// caller's Matrix/Beeper membership handling always runs unchanged.
//
//   - member_removed: forwarded and remembered.
//   - member_left right after member_removed for the same account/chat/member:
//     the trailing op 61 of a kick, suppressed so DIVA sees one member_removed.
//   - member_left otherwise: a genuine leave, forwarded and remembered.
//   - member_removed right after member_left: unexpected reverse order. Both
//     stay forwarded (no reordering queue); only a warning is logged.
//   - member_joined: forwarded and clears the marker, so a quick rejoin and a
//     later leave are never mistaken for a kick's trailing op.
func (lc *LineClient) admitDIVAGroupEvent(event *divaV2GroupEvent, revision string) bool {
	key := divaGroupLeaveKey{
		account: event.Account.Mid,
		chat:    event.Chat.ID,
		member:  event.GroupEvent.Member.Mid,
	}
	now := divaGroupEventNow()

	lc.divaGroupLeaveMu.Lock()
	for k, marker := range lc.divaGroupLeaves {
		if now.Sub(marker.receivedAt) >= divaGroupLeaveWindow {
			delete(lc.divaGroupLeaves, k)
		}
	}
	previous, hasPrevious := lc.divaGroupLeaves[key]
	admit := true
	switch event.GroupEvent.Type {
	case divaGroupEventMemberJoined:
		delete(lc.divaGroupLeaves, key)
	case divaGroupEventMemberRemoved, divaGroupEventMemberLeft:
		if event.GroupEvent.Type == divaGroupEventMemberLeft && hasPrevious &&
			previous.eventType == divaGroupEventMemberRemoved {
			admit = false
			delete(lc.divaGroupLeaves, key)
			break
		}
		if lc.divaGroupLeaves == nil {
			lc.divaGroupLeaves = make(map[divaGroupLeaveKey]divaGroupLeaveMarker)
		}
		lc.divaGroupLeaves[key] = divaGroupLeaveMarker{
			eventType:  event.GroupEvent.Type,
			revision:   revision,
			receivedAt: now,
		}
	}
	lc.divaGroupLeaveMu.Unlock()

	if !admit {
		lc.UserLogin.Bridge.Log.Debug().
			Str("diva_event", "DIVA_GROUP_EVENT").
			Str("group_id", key.chat).
			Str("member_id", key.member).
			Str("left_revision", revision).
			Str("matched_removed_revision", previous.revision).
			Msg("[DIVA_GROUP_EVENT] suppressed trailing member_left")
		return false
	}
	if event.GroupEvent.Type == divaGroupEventMemberRemoved && hasPrevious &&
		previous.eventType == divaGroupEventMemberLeft {
		lc.UserLogin.Bridge.Log.Warn().
			Str("diva_event", "DIVA_GROUP_EVENT").
			Str("group_id", key.chat).
			Str("member_id", key.member).
			Str("removed_revision", revision).
			Str("previous_left_revision", previous.revision).
			Msg("[DIVA_GROUP_EVENT] member_removed arrived after member_left; both forwarded")
	}
	return true
}

func (lc *LineClient) handleDIVAInbound(msg *line.Message, chatMID, unwrappedText string, decryptionFailed bool, opType int, origin divaOrigin) {
	isGroupOrRoom := ToType(msg.ToType) == ToRoom || ToType(msg.ToType) == ToGroup
	isDirectCall := divaIsDirectCall(msg, opType)

	if !isGroupOrRoom && !isDirectCall {
		return
	}

	version := lc.divaContractVersion()
	var skipReason string
	if version == 1 {
		if origin != divaOriginLive {
			skipReason = divaSkipBackfill
		} else {
			skipReason = divaV1ForwardDecision(msg, unwrappedText, decryptionFailed)
		}
	}

	// Never log message text or decrypted payloads here: for media the
	// decrypted body contains the media keyMaterial.
	lc.UserLogin.Bridge.Log.Debug().
		Str("diva_event", "DIVA_RX").
		Str("group_id", chatMID).
		Str("sender_id", msg.From).
		Str("message_id", msg.ID).
		Int("content_type", msg.ContentType).
		Bool("decryption_failed", decryptionFailed).
		Str("origin", string(origin)).
		Int("contract_version", version).
		Bool("forward", skipReason == "").
		Str("skip_reason", skipReason).
		Msg("[DIVA_RX]")

	if skipReason != "" {
		return
	}

	// The event and any media fetch input are copied here, while msg is still
	// owned by the receive loop. Slow name lookup / OBS fetch / HTTP delivery
	// stay in the forward goroutine and never block LINE receive.
	var mediaLoader divaInboundMediaLoader
	if mediaKind, supported := divaMediaKindForContentType(ContentType(msg.ContentType)); version == divaContractV2 &&
		origin == divaOriginLive && supported && !decryptionFailed {
		mediaMsg := cloneDIVAMediaMessage(msg)
		decryptedBody := unwrappedText
		mediaLoader = func(ctx context.Context) (*divaInboundMedia, error) {
			return divaPrepareInboundMedia(lc, ctx, mediaKind, mediaMsg, decryptedBody)
		}
	}

	var encode func() ([]byte, error)
	if version == divaContractV2 {
		event := lc.buildDIVAV2Event(msg, chatMID, unwrappedText, decryptionFailed, opType, origin)
		lookupName := event.Sender.DisplayName == nil && origin == divaOriginLive && !event.Sender.IsFromMe
		encode = func() ([]byte, error) {
			if lookupName {
				event.Sender.DisplayName = lc.lookupDIVADisplayName(event.Sender.Mid)
				lc.UserLogin.Bridge.Log.Debug().
					Str("diva_event", "DIVA_NAME").
					Str("sender_id", event.Sender.Mid).
					Str("message_id", event.Message.ID).
					Bool("found", event.Sender.DisplayName != nil).
					Msg("[DIVA_NAME] display name lookup for uncached sender")
			}
			return json.Marshal(event)
		}
	} else {
		event := divaInboundEvent{
			Text:      unwrappedText,
			GroupID:   chatMID,
			SenderID:  msg.From,
			MessageID: msg.ID,
		}
		encode = func() ([]byte, error) { return json.Marshal(event) }
	}
	lc.forwardDIVAInbound(encode, mediaLoader, chatMID, msg.ID, origin == divaOriginLive)
}

// forwardDIVAInbound encodes an inbound event and posts it to the local DIVA
// worker. It is intentionally asynchronous: a DIVA worker outage or a slow
// sender name lookup must never block the LINE receive loop. If the worker
// returns reply_text and allowReply is set, Go sends that text back to the
// same LINE chat. Backfilled events never get a reply, whatever the worker
// answers.
func (lc *LineClient) forwardDIVAInbound(encode func() ([]byte, error), mediaLoader divaInboundMediaLoader, groupID, messageID string, allowReply bool) {
	endpoint := strings.TrimSpace(os.Getenv("DIVA_WEBHOOK_URL"))
	if endpoint == "" {
		return
	}

	go func() {
		payload, err := encode()
		if err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).Str("message_id", messageID).Msg("DIVA adapter failed to encode inbound event")
			return
		}

		requestBody := payload
		contentType := "application/json"
		httpClient := divaHTTPClient
		requestTimeout := defaultDIVAWebhookTimeout

		if mediaLoader != nil {
			mediaSlot := false
			select {
			case divaInboundMediaSem <- struct{}{}:
				mediaSlot = true
			default:
				lc.UserLogin.Bridge.Log.Warn().
					Str("message_id", messageID).
					Int("max_concurrent", divaInboundMediaMaxConcurrent).
					Msg("DIVA inbound media concurrency full; sending descriptor only")
			}
			if mediaSlot {
				defer func() { <-divaInboundMediaSem }()
				mediaCtx, mediaCancel := context.WithTimeout(context.Background(), defaultDIVAMediaWebhookTimeout)
				media, mediaErr := mediaLoader(mediaCtx)
				mediaCancel()
				if mediaErr != nil {
					// Descriptor JSON is still useful. Media failure must not delete the
					// event or make LINE receive depend on OBS availability.
					lc.UserLogin.Bridge.Log.Warn().Err(mediaErr).
						Str("message_id", messageID).
						Msg("DIVA inbound media unavailable; sending descriptor only")
				} else if media != nil {
					multipartBody, multipartType, buildErr := buildDIVAInboundMultipart(payload, media)
					if buildErr != nil {
						lc.UserLogin.Bridge.Log.Warn().Err(buildErr).
							Str("message_id", messageID).
							Msg("DIVA inbound media multipart build failed; sending descriptor only")
					} else {
						requestBody = multipartBody
						contentType = multipartType
						httpClient = divaMediaHTTPClient
						requestTimeout = defaultDIVAMediaWebhookTimeout
					}
				}
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(requestBody))
		if err != nil {
			lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("DIVA adapter failed to build request")
			return
		}
		req.Header.Set("Content-Type", contentType)
		signDIVAInbound(req, requestBody)

		resp, err := httpClient.Do(req)
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
		if !allowReply {
			lc.UserLogin.Bridge.Log.Warn().
				Str("group_id", groupID).
				Str("message_id", messageID).
				Msg("DIVA worker returned reply_text for a backfilled event, not sending")
			return
		}

		sendCtx, sendCancel := context.WithTimeout(context.Background(), defaultDIVASendTimeout)
		defer sendCancel()
		if err := lc.sendDIVAReplyText(sendCtx, groupID, replyText); err != nil {
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

// sendDIVAReplyText sends the worker's reply_text back to the chat the event
// came from, through the shared LINE send core (same E2EE, group key and
// fallback behaviour as a Matrix text message).
func (lc *LineClient) sendDIVAReplyText(ctx context.Context, chatMID, text string) error {
	chatMID = strings.TrimSpace(chatMID)
	if chatMID == "" || strings.TrimSpace(text) == "" {
		return markOutboundFailure(failInvalidRequest, errors.New("DIVA send requires group_id and text"))
	}
	_, err := lc.sendLineOutbound(ctx, &lineOutboundRequest{
		ChatMID:     chatMID,
		ContentType: ContentText,
		Text:        text,
	})
	return err
}