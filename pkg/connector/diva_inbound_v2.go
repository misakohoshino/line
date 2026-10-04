package connector

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// DIVA inbound contract v2: a generic LINE event envelope for Server A.
//
// Everything here is built from LINE message fields and unencrypted content
// metadata. The only decrypted value that is ever copied into this JSON event
// is the user text of a genuine text message. For media, the decrypted body
// carries keyMaterial and is never serialized here. ENC_KM, chunks, OID and
// media bytes must never appear in the v2 JSON metadata. Live media bytes may
// travel separately as the media part of the same /line/inbound multipart
// request; backfill remains descriptor-only JSON.

const divaContractV2 = 2

// divaOrigin tells Server A how the bridge came to see a message.
type divaOrigin string

const (
	// divaOriginLive is a message delivered by the realtime poll loop.
	divaOriginLive divaOrigin = "live"
	// divaOriginBackfill is a message recovered by backfillRecentMessages on
	// connect or after a fullSync. It may be old and may have been seen before.
	divaOriginBackfill divaOrigin = "backfill"
)

// divaV2MetadataWhitelist lists the only content metadata keys copied into a
// v2 event. Everything else, including ENC_KM, stays inside the bridge.
var divaV2MetadataWhitelist = []string{"ORGCONTP", "STKID", "STKPKGID"}

type divaV2Event struct {
	Version   int               `json:"version"`
	EventID   string            `json:"event_id"`
	EventType string            `json:"event_type"`
	Origin    divaOrigin        `json:"origin"`
	Account   divaV2Account     `json:"account"`
	Chat      divaV2Chat        `json:"chat"`
	Sender    divaV2Sender      `json:"sender"`
	Message   divaV2Message     `json:"message"`
	Content   any               `json:"content"`
	Relations divaV2Relations   `json:"relations"`
	Metadata  map[string]string `json:"metadata"`
}

type divaV2Account struct {
	Mid string `json:"mid"`
}

type divaV2Chat struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type divaV2Sender struct {
	Mid         string  `json:"mid"`
	IsFromMe    bool    `json:"is_from_me"`
	DisplayName *string `json:"display_name"`
}

type divaV2Message struct {
	ID               string `json:"id"`
	ContentType      int    `json:"content_type"`
	CreatedAt        *int64 `json:"created_at"`
	DecryptionFailed bool   `json:"decryption_failed"`
	E2EE             bool   `json:"e2ee"`
}

type divaV2Relations struct {
	ReplyTo    *divaV2ReplyTo  `json:"reply_to"`
	Mentions   []divaV2Mention `json:"mentions"`
	MentionAll bool            `json:"mention_all"`
}

type divaV2ReplyTo struct {
	MessageID string `json:"message_id"`
}

// divaV2Mention is one @mention. Start and End are LINE's UTF-16 offsets into
// the text, or null when LINE sent an offset that cannot be parsed.
type divaV2Mention struct {
	Mid   string `json:"mid"`
	Start *int   `json:"start"`
	End   *int   `json:"end"`
}

type divaV2TextContent struct {
	Type string  `json:"type"`
	Text *string `json:"text"`
}

type divaV2MediaContent struct {
	Type       string  `json:"type"`
	FileName   *string `json:"file_name"`
	FileSize   *int64  `json:"file_size"`
	DurationMs *int64  `json:"duration_ms,omitempty"`
}

type divaV2StickerContent struct {
	Type      string  `json:"type"`
	PackageID *string `json:"package_id"`
	StickerID *string `json:"sticker_id"`
}

type divaV2LocationContent struct {
	Type      string   `json:"type"`
	Title     *string  `json:"title"`
	Address   *string  `json:"address"`
	Latitude  *float64 `json:"latitude"`
	Longitude *float64 `json:"longitude"`
}

type divaV2ContactContent struct {
	Type        string  `json:"type"`
	Mid         *string `json:"mid"`
	DisplayName *string `json:"display_name"`
}

type divaV2CallContent struct {
	Type string `json:"type"`
}

type divaV2UnsupportedContent struct {
	Type string `json:"type"`
}

const (
	divaGroupEventMemberJoined  = "member_joined"
	divaGroupEventMemberLeft    = "member_left"
	divaGroupEventMemberRemoved = "member_removed"
)

type divaV2GroupMember struct {
	Mid      string `json:"mid"`
	IsFromMe bool   `json:"is_from_me"`
}

type divaV2GroupEventContent struct {
	Type      string             `json:"type"`
	Member    divaV2GroupMember  `json:"member"`
	Actor     *divaV2GroupMember `json:"actor"`
	CreatedAt *int64             `json:"created_at"`
}

type divaV2GroupEvent struct {
	Version    int                     `json:"version"`
	EventID    string                  `json:"event_id"`
	EventType  string                  `json:"event_type"`
	Origin     divaOrigin              `json:"origin"`
	Account    divaV2Account           `json:"account"`
	Chat       divaV2Chat              `json:"chat"`
	GroupEvent divaV2GroupEventContent `json:"group_event"`
}

// buildDIVAV2LiveGroupEvent normalizes only live LINE membership operations
// that DIVA needs in LINE-1D-D. Historical ContentSystem records deliberately
// stay out of DIVA until durable ingestion/deduplication exists; otherwise one
// logical membership change can be emitted once as an operation and again
// later during startup backfill.
func (lc *LineClient) buildDIVAV2LiveGroupEvent(op line.Operation) (*divaV2GroupEvent, bool) {
	revision, err := op.Revision.Int64()
	if err != nil || revision <= 0 {
		return nil, false
	}

	var eventType, chatMID, memberMID, actorMID string
	switch OperationType(op.Type) {
	case OpNotifiedJoinChat:
		eventType = divaGroupEventMemberJoined
		chatMID = op.Param1
		memberMID = op.Param2
	case OpNotifiedLeaveChat:
		eventType = divaGroupEventMemberLeft
		if isChatMID(op.Param1) {
			chatMID, memberMID = op.Param1, op.Param2
		} else if isChatMID(op.Param2) {
			chatMID, memberMID = op.Param2, op.Param1
		}
		actorMID = memberMID
	case OpNotifiedDeleteOtherFromChat:
		eventType = divaGroupEventMemberRemoved
		chatMID = op.Param1
		actorMID = op.Param2
		memberMID = op.Param3
	default:
		return nil, false
	}

	if !isChatMID(chatMID) || !isUserMID(memberMID) {
		return nil, false
	}
	if actorMID != "" && !isUserMID(actorMID) {
		return nil, false
	}

	chatType := "group"
	if strings.HasPrefix(strings.ToLower(chatMID), "r") {
		chatType = "room"
	}

	var createdAt *int64
	if ts, tsErr := op.CreatedTime.Int64(); tsErr == nil && ts > 0 {
		createdAt = &ts
	}

	member := divaV2GroupMember{Mid: memberMID, IsFromMe: lc.isOwnMID(memberMID)}
	var actor *divaV2GroupMember
	if actorMID != "" {
		actor = &divaV2GroupMember{Mid: actorMID, IsFromMe: lc.isOwnMID(actorMID)}
	}

	return &divaV2GroupEvent{
		Version:   divaContractV2,
		EventID:   "line:op:" + strconv.FormatInt(revision, 10),
		EventType: "group_event",
		Origin:    divaOriginLive,
		Account:   divaV2Account{Mid: lc.midOrFallback()},
		Chat:      divaV2Chat{ID: chatMID, Type: chatType},
		GroupEvent: divaV2GroupEventContent{
			Type:      eventType,
			Member:    member,
			Actor:     actor,
			CreatedAt: createdAt,
		},
	}, true
}

// buildDIVAV2Event builds the v2 envelope for one inbound message.
// unwrappedText is only used for genuine text messages.
func (lc *LineClient) buildDIVAV2Event(msg *line.Message, chatMID, unwrappedText string, decryptionFailed bool, opType int, origin divaOrigin) *divaV2Event {
	isFromMe := OperationType(opType) == OpSendMessage || (lc.Mid != "" && msg.From == lc.Mid)
	return &divaV2Event{
		Version:   divaContractV2,
		EventID:   "line:" + msg.ID,
		EventType: "message",
		Origin:    origin,
		Account:   divaV2Account{Mid: lc.midOrFallback()},
		Chat:      divaV2Chat{ID: chatMID, Type: divaChatType(msg.ToType)},
		Sender: divaV2Sender{
			Mid:         msg.From,
			IsFromMe:    isFromMe,
			DisplayName: lc.cachedDisplayName(msg.From),
		},
		Message: divaV2Message{
			ID:               msg.ID,
			ContentType:      msg.ContentType,
			CreatedAt:        divaCreatedAt(msg),
			DecryptionFailed: decryptionFailed,
			E2EE:             len(msg.Chunks) > 0,
		},
		Content:   divaV2ContentFor(msg, unwrappedText, decryptionFailed),
		Relations: divaV2RelationsFor(msg),
		Metadata:  divaV2Metadata(msg),
	}
}

func divaChatType(toType int) string {
	switch ToType(toType) {
	case ToGroup:
		return "group"
	case ToRoom:
		return "room"
	default:
		return "direct"
	}
}

// divaCreatedAt returns LINE's own createdTime in epoch milliseconds, never
// the time the bridge or Server A saw the message.
func divaCreatedAt(msg *line.Message) *int64 {
	ts, err := msg.CreatedTime.Int64()
	if err != nil || ts == 0 {
		return nil
	}
	return &ts
}

// cachedDisplayName is best-effort: it only reads the contact cache and never
// calls LINE from the receive path. It is LINE profile data, not a driver
// identity.
func (lc *LineClient) cachedDisplayName(mid string) *string {
	lc.cacheMu.Lock()
	cached, ok := lc.contactCache[mid]
	lc.cacheMu.Unlock()
	if !ok {
		return nil
	}
	return divaDisplayName(mid, cached.EffectiveDisplayName())
}

// divaDisplayNameLookupTimeout bounds the single contact lookup made for a v2
// live event whose sender is not in the contact cache yet.
var divaDisplayNameLookupTimeout = time.Second

// lookupDIVADisplayName fills a cold contact cache for one sender. The contact
// cache is otherwise only filled by Matrix ghost handling, which runs after the
// DIVA event has already been sent, so the first message from a sender would
// always carry display_name null.
//
// It must only run off the LINE receive loop (in the DIVA forward goroutine).
// getContact caches a successful lookup, so later messages are served from the
// cache. A lookup still running at the timeout keeps going in the background
// and may fill the cache for the next message; this event gets null.
func (lc *LineClient) lookupDIVADisplayName(mid string) *string {
	ctx, cancel := context.WithTimeout(context.Background(), divaDisplayNameLookupTimeout)
	defer cancel()
	found := make(chan line.Contact, 1)
	go func() { found <- lc.getContact(ctx, mid) }()
	select {
	case contact := <-found:
		return divaDisplayName(mid, contact.EffectiveDisplayName())
	case <-ctx.Done():
		return nil
	}
}

// divaDisplayName returns name unless it is empty or just the MID, which is
// what getContact falls back to when LINE has no profile for it.
func divaDisplayName(mid, name string) *string {
	name = strings.TrimSpace(name)
	if name == "" || strings.EqualFold(name, mid) {
		return nil
	}
	return &name
}

func divaV2ContentFor(msg *line.Message, unwrappedText string, decryptionFailed bool) any {
	// LINE call summaries are wrapped in ORGCONTP=CALL rather than a dedicated
	// numeric ContentType. LINE-1D-C only needs a stable call marker; voice/video,
	// duration and result stay inside the bridge.
	if divaIsDirectCall(msg, int(OpReceiveMessage)) {
		return divaV2CallContent{Type: "call"}
	}
	// Other LINE-wrapped notices are not user content yet.
	if msg.ContentMetadata["ORGCONTP"] != "" || isPostNotification(msg) {
		return divaV2UnsupportedContent{Type: "unsupported"}
	}
	meta := msg.ContentMetadata
	switch ContentType(msg.ContentType) {
	case ContentText:
		content := divaV2TextContent{Type: "text"}
		if !decryptionFailed {
			text := unwrappedText
			content.Text = &text
		}
		return content
	case ContentImage:
		return divaV2Media("image", meta, false)
	case ContentVideo:
		return divaV2Media("video", meta, true)
	case ContentAudio:
		return divaV2Media("audio", meta, true)
	case ContentFile:
		return divaV2Media("file", meta, false)
	case ContentSticker:
		return divaV2StickerContent{
			Type:      "sticker",
			PackageID: divaStrPtr(meta["STKPKGID"]),
			StickerID: divaStrPtr(meta["STKID"]),
		}
	case ContentLocation:
		content := divaV2LocationContent{Type: "location"}
		if loc := msg.Location; loc != nil {
			content.Title = divaStrPtr(loc.Title)
			content.Address = divaStrPtr(loc.Address)
			lat, lng := loc.Latitude, loc.Longitude
			content.Latitude = &lat
			content.Longitude = &lng
		}
		return content
	case ContentContact:
		return divaV2ContactContent{
			Type:        "contact",
			Mid:         divaStrPtr(meta["mid"]),
			DisplayName: divaStrPtr(meta["displayName"]),
		}
	default:
		return divaV2UnsupportedContent{Type: "unsupported"}
	}
}

// divaV2Media is a descriptor only: no bytes, no OID, no key material.
func divaV2Media(kind string, meta map[string]string, withDuration bool) divaV2MediaContent {
	content := divaV2MediaContent{
		Type:     kind,
		FileName: divaStrPtr(meta["FILE_NAME"]),
		FileSize: divaInt64Ptr(meta["FILE_SIZE"]),
	}
	if withDuration {
		content.DurationMs = divaInt64Ptr(meta["DURATION"])
	}
	return content
}

// divaV2RelationsFor reads reply and mention relations the same way the
// Matrix conversion path does (resolveReplyRelatesTo / MENTION metadata).
func divaV2RelationsFor(msg *line.Message) divaV2Relations {
	relations := divaV2Relations{Mentions: []divaV2Mention{}}

	relatedID := msg.RelatedMessageID
	if relatedID == "" {
		relatedID = msg.ContentMetadata["message_relation_server_message_id"]
	}
	if relatedID != "" && (msg.MessageRelationType == 0 || msg.MessageRelationType == 3) {
		relations.ReplyTo = &divaV2ReplyTo{MessageID: relatedID}
	}

	raw := msg.ContentMetadata["MENTION"]
	if raw == "" {
		return relations
	}
	var mentionData struct {
		MENTIONEES []struct {
			M string `json:"M,omitempty"`
			A string `json:"A,omitempty"`
			S string `json:"S,omitempty"`
			E string `json:"E,omitempty"`
		} `json:"MENTIONEES"`
	}
	if err := json.Unmarshal([]byte(raw), &mentionData); err != nil {
		return relations
	}
	for _, ment := range mentionData.MENTIONEES {
		if ment.A == "1" {
			relations.MentionAll = true
		}
		if ment.M == "" {
			continue
		}
		relations.Mentions = append(relations.Mentions, divaV2Mention{
			Mid:   ment.M,
			Start: divaIntPtr(ment.S),
			End:   divaIntPtr(ment.E),
		})
	}
	return relations
}

func divaV2Metadata(msg *line.Message) map[string]string {
	out := map[string]string{}
	for _, key := range divaV2MetadataWhitelist {
		if value := msg.ContentMetadata[key]; value != "" {
			out[key] = value
		}
	}
	return out
}

func divaStrPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func divaInt64Ptr(value string) *int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return nil
	}
	return &n
}

func divaIntPtr(value string) *int {
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return nil
	}
	return &n
}
