package connector

import (
	"testing"
	"time"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// lineDetermineFlow99999 is the determineMediaMessageFlow failure seen in
// Production for a group whose phone-native images use the plain media flow.
const lineDetermineFlow99999 = `{"code":99999,"message":"internal error"}`

func plainInboundImage(id, chatMID string) *line.Message {
	return &line.Message{
		ID:          id,
		From:        "usender000000000000000000000000000",
		To:          chatMID,
		ToType:      int(ToGroup),
		ContentType: int(ContentImage),
		ContentMetadata: map[string]string{
			"FILE_SIZE":          "12345",
			"MEDIA_CONTENT_INFO": `{"category":"original","fileSize":12345,"extension":"jpg"}`,
		},
	}
}

func e2eeInboundImage(id, chatMID string) *line.Message {
	return &line.Message{
		ID:          id,
		From:        "usender000000000000000000000000000",
		To:          chatMID,
		ToType:      int(ToGroup),
		ContentType: int(ContentImage),
		ContentMetadata: map[string]string{
			"OID":         "oid-e2ee",
			"SID":         "emi",
			"ENC_KM":      "key-material",
			"e2eeVersion": "2",
		},
		Chunks: []string{"a", "b", "c", "AAAAAQ==", "AAAAAg=="},
	}
}

func mediaFlowEntry(t *testing.T, lc *LineClient, chatMID string) (cachedMediaFlow, bool) {
	t.Helper()
	lc.cacheMu.Lock()
	defer lc.cacheMu.Unlock()
	entry, ok := lc.mediaFlowCache[chatMID]
	return entry, ok
}

func countMethod(f *fakeLine, method string) int {
	n := 0
	for _, m := range f.methods() {
		if m == method {
			n++
		}
	}
	return n
}

// 1. A plain inbound image seeds the cache with flow 1 for images.
func TestInboundPlainImageSeedsMediaFlowCache(t *testing.T) {
	env := newSendTestEnv(t, true)

	env.lc.observeInboundMediaFlow(plainInboundImage("900", sendTestGroup), sendTestGroup, divaOriginLive)

	entry, ok := mediaFlowEntry(t, env.lc, sendTestGroup)
	if !ok {
		t.Fatal("plain inbound image did not seed the media flow cache")
	}
	if got := entry.flowMap["1"]; got != 1 || len(entry.flowMap) != 1 {
		t.Fatalf("flowMap = %v, want only image -> 1", entry.flowMap)
	}
	if !entry.learnedOnly || entry.ttl != defaultMediaFlowTTL {
		t.Fatalf("entry = %+v, want learned-only with the default TTL", entry)
	}
	if n := len(env.fake.snapshot()); n != 0 {
		t.Fatalf("learning made %d LINE calls", n)
	}

	// A second plain image changes nothing.
	env.lc.observeInboundMediaFlow(plainInboundImage("902", sendTestGroup), sendTestGroup, divaOriginLive)
	again, _ := mediaFlowEntry(t, env.lc, sendTestGroup)
	if !again.cachedAt.Equal(entry.cachedAt) || again.flowMap["1"] != 1 {
		t.Fatalf("second plain image changed the entry: %+v", again)
	}
}

// 2. After learning, sends read the cache: plain, no determineMediaMessageFlow,
// even while LINE answers that call with 500/99999.
func TestLearnedPlainImageFlowSkipsDetermineMediaMessageFlow(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.fake.mediaFlowStatus = 500
	env.fake.mediaFlowBody = lineDetermineFlow99999

	env.lc.observeInboundMediaFlow(plainInboundImage("901", sendTestGroup), sendTestGroup, divaOriginLive)

	if env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage)) {
		t.Fatal("shouldUseE2EEMediaFlow = true after a plain inbound image, want false")
	}
	if n := countMethod(env.fake, "determineMediaMessageFlow"); n != 0 {
		t.Fatalf("determineMediaMessageFlow called %d times, want 0", n)
	}

	// The DIVA send_image path: the shared core sends plain media and never
	// touches the group key.
	result, err := env.lc.sendLineOutbound(t.Context(), &lineOutboundRequest{
		ChatMID:     sendTestGroup,
		ContentType: ContentImage,
		Media:       &outboundMedia{Data: testPNG(t), MimeType: "image/png", FileName: "dispatch.png"},
	})
	if err != nil {
		t.Fatalf("sendLineOutbound: %v", err)
	}
	if !result.SentPlaintext {
		t.Fatal("image was not sent through the plain media flow")
	}
	assertMethods(t, env.fake,
		"sendMessage",
		"acquireEncryptedAccessToken",
		"OBS /r/talk/m/srv-1",
		"OBS /r/talk/m/srv-1__ud-preview",
	)
	if env.crypto.fetches != 0 {
		t.Fatalf("group key fetched %d times in the plain media flow", env.crypto.fetches)
	}
}

// 3. E2EE and other non-plain inbound messages never seed the plain flow.
func TestInboundNonPlainMessagesDoNotSeedPlainFlow(t *testing.T) {
	video := plainInboundImage("906", sendTestGroup)
	video.ContentType = int(ContentVideo)
	chunksOnly := plainInboundImage("907", sendTestGroup)
	chunksOnly.Chunks = []string{"a", "b", "c", "AAAAAQ==", "AAAAAg=="}
	encKMOnly := plainInboundImage("908", sendTestGroup)
	encKMOnly.ContentMetadata["ENC_KM"] = "key-material"
	oidOnly := plainInboundImage("909", sendTestGroup)
	oidOnly.ContentMetadata["OID"] = "oid-1"
	public := plainInboundImage("910", sendTestGroup)
	public.ContentMetadata["DOWNLOAD_URL"] = "/r/talk/emi/public"
	wrapped := plainInboundImage("911", sendTestGroup)
	wrapped.ContentMetadata["ORGCONTP"] = "POSTNOTIFICATION"

	cases := []struct {
		name   string
		msg    *line.Message
		chat   string
		origin divaOrigin
	}{
		{"e2ee image", e2eeInboundImage("905", sendTestGroup), sendTestGroup, divaOriginLive},
		{"chunks without OID", chunksOnly, sendTestGroup, divaOriginLive},
		{"ENC_KM without OID", encKMOnly, sendTestGroup, divaOriginLive},
		{"OID in metadata", oidOnly, sendTestGroup, divaOriginLive},
		{"public DOWNLOAD_URL", public, sendTestGroup, divaOriginLive},
		{"wrapped notice", wrapped, sendTestGroup, divaOriginLive},
		{"plain video (only images are learned)", video, sendTestGroup, divaOriginLive},
		{"backfilled plain image", plainInboundImage("912", sendTestGroup), sendTestGroup, divaOriginBackfill},
		{"empty chat", plainInboundImage("913", sendTestGroup), "", divaOriginLive},
		{"nil message", nil, sendTestGroup, divaOriginLive},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newSendTestEnv(t, true)
			env.lc.observeInboundMediaFlow(tc.msg, tc.chat, tc.origin)
			if entry, ok := mediaFlowEntry(t, env.lc, sendTestGroup); ok {
				t.Fatalf("cache seeded: %+v", entry)
			}
		})
	}

	// An E2EE image does not touch a server entry that says E2EE.
	env := newSendTestEnv(t, true)
	if !env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage)) {
		t.Fatal("server flow 2 not used")
	}
	env.lc.observeInboundMediaFlow(e2eeInboundImage("914", sendTestGroup), sendTestGroup, divaOriginLive)
	entry, _ := mediaFlowEntry(t, env.lc, sendTestGroup)
	if entry.flowMap["1"] != 2 || entry.learnedOnly {
		t.Fatalf("server entry changed by an E2EE image: %+v", entry)
	}
}

// 4a. The existing server flow and its TTL are unchanged.
func TestServerMediaFlowCacheUnchanged(t *testing.T) {
	env := newSendTestEnv(t, true)
	if !env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage)) {
		t.Fatal("server flow 2 → want E2EE")
	}
	if env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage)) != true {
		t.Fatal("cached server flow 2 → want E2EE")
	}
	if n := countMethod(env.fake, "determineMediaMessageFlow"); n != 1 {
		t.Fatalf("determineMediaMessageFlow called %d times, want 1 (second read from cache)", n)
	}
	entry, _ := mediaFlowEntry(t, env.lc, sendTestGroup)
	if entry.ttl != 60*time.Second || entry.learnedOnly {
		t.Fatalf("server entry = %+v, want cacheTtlMillis 60000 and not learned", entry)
	}
	// A content type missing from a server map still means E2EE without asking again.
	if !env.lc.shouldUseE2EEMediaFlow(sendTestGroup, 99) {
		t.Fatal("unknown content type → want E2EE")
	}
	if n := countMethod(env.fake, "determineMediaMessageFlow"); n != 1 {
		t.Fatalf("determineMediaMessageFlow called %d times for a type missing from a server map", n)
	}

	// An expired server entry asks the server again.
	env.lc.cacheMu.Lock()
	entry.cachedAt = time.Now().Add(-2 * entry.ttl)
	env.lc.mediaFlowCache[sendTestGroup] = entry
	env.lc.cacheMu.Unlock()
	env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage))
	if n := countMethod(env.fake, "determineMediaMessageFlow"); n != 2 {
		t.Fatalf("expired entry: determineMediaMessageFlow called %d times, want 2", n)
	}
}

// 4b. A failing determineMediaMessageFlow alone is never treated as plain.
func TestDetermineMediaFlowFailureStillDefaultsToE2EE(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.fake.mediaFlowStatus = 500
	env.fake.mediaFlowBody = lineDetermineFlow99999

	for i := 0; i < 2; i++ {
		if !env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage)) {
			t.Fatal("500/99999 without evidence → want the existing E2EE default")
		}
	}
	if n := countMethod(env.fake, "determineMediaMessageFlow"); n != 2 {
		t.Fatalf("determineMediaMessageFlow called %d times, want 2 (failures are not cached)", n)
	}
	if entry, ok := mediaFlowEntry(t, env.lc, sendTestGroup); ok {
		t.Fatalf("failure was cached: %+v", entry)
	}
}

// 4c. A plain inbound image updates a valid server entry for images only and
// keeps the server entry's timing, so the server is asked again on schedule.
func TestLearnedFlowUpdatesValidServerEntryKeepingTTL(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage))
	before, _ := mediaFlowEntry(t, env.lc, sendTestGroup)

	env.lc.observeInboundMediaFlow(plainInboundImage("920", sendTestGroup), sendTestGroup, divaOriginLive)

	after, _ := mediaFlowEntry(t, env.lc, sendTestGroup)
	if after.flowMap["1"] != 1 || after.flowMap["2"] != 2 || after.flowMap["14"] != 2 {
		t.Fatalf("flowMap = %v, want image 1 and the other server values kept", after.flowMap)
	}
	if !after.cachedAt.Equal(before.cachedAt) || after.ttl != before.ttl || after.learnedOnly {
		t.Fatalf("timing changed: before %+v after %+v", before, after)
	}
	if before.flowMap["1"] != 2 {
		t.Fatalf("server map was mutated in place: %v", before.flowMap)
	}
	if env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage)) {
		t.Fatal("image after learning → want plain")
	}
	if !env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentVideo)) {
		t.Fatal("video keeps the server flow 2")
	}
	if n := countMethod(env.fake, "determineMediaMessageFlow"); n != 1 {
		t.Fatalf("determineMediaMessageFlow called %d times, want 1", n)
	}
}

// 4d. A learned-only entry does not answer for other content types; a normal
// server answer then takes priority, and a server failure keeps the learned value.
func TestLearnedOnlyEntryDefersToServer(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.lc.observeInboundMediaFlow(plainInboundImage("930", sendTestGroup), sendTestGroup, divaOriginLive)

	env.fake.mediaFlowStatus = 500
	env.fake.mediaFlowBody = lineDetermineFlow99999
	if !env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentVideo)) {
		t.Fatal("video with a failing server → want the E2EE default")
	}
	if env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage)) {
		t.Fatal("server failure dropped the learned image flow")
	}

	env.fake.mediaFlowStatus = 0 // the server answers normally again (image 2)
	if !env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentVideo)) {
		t.Fatal("server video flow 2 → want E2EE")
	}
	if !env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage)) {
		t.Fatal("a normal server answer must take priority over the learned image flow")
	}
	entry, _ := mediaFlowEntry(t, env.lc, sendTestGroup)
	if entry.learnedOnly || entry.ttl != 60*time.Second {
		t.Fatalf("entry = %+v, want the server entry", entry)
	}
	if n := countMethod(env.fake, "determineMediaMessageFlow"); n != 2 {
		t.Fatalf("determineMediaMessageFlow called %d times, want 2", n)
	}
}

// 4e. A learned entry expires like any other and the server is asked again.
func TestLearnedEntryExpires(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.lc.observeInboundMediaFlow(plainInboundImage("940", sendTestGroup), sendTestGroup, divaOriginLive)
	env.lc.cacheMu.Lock()
	entry := env.lc.mediaFlowCache[sendTestGroup]
	entry.cachedAt = time.Now().Add(-2 * defaultMediaFlowTTL)
	env.lc.mediaFlowCache[sendTestGroup] = entry
	env.lc.cacheMu.Unlock()

	if !env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage)) {
		t.Fatal("expired learned entry → want the server flow 2")
	}
	if n := countMethod(env.fake, "determineMediaMessageFlow"); n != 1 {
		t.Fatalf("determineMediaMessageFlow called %d times, want 1", n)
	}

	// Learning over an expired server entry starts a fresh learned-only entry.
	env.lc.cacheMu.Lock()
	entry = env.lc.mediaFlowCache[sendTestGroup]
	entry.cachedAt = time.Now().Add(-2 * entry.ttl)
	env.lc.mediaFlowCache[sendTestGroup] = entry
	env.lc.cacheMu.Unlock()
	env.lc.observeInboundMediaFlow(plainInboundImage("941", sendTestGroup), sendTestGroup, divaOriginLive)
	fresh, _ := mediaFlowEntry(t, env.lc, sendTestGroup)
	if !fresh.learnedOnly || len(fresh.flowMap) != 1 || fresh.flowMap["1"] != 1 || time.Since(fresh.cachedAt) > time.Minute {
		t.Fatalf("entry = %+v, want a fresh learned-only image entry", fresh)
	}
}
