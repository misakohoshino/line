package connector

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/highesttt/matrix-line-messenger/pkg/connector/handlers"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

const rawRecoveryGroup = "cgroup0000000000000000000000000001"

// rawRecoveryWorker is a fake DIVA worker. It records every request and the
// highest number of requests it saw in flight at once.
type rawRecoveryWorker struct {
	srv *httptest.Server

	mu           sync.Mutex
	bodies       []map[string]any
	contentTypes []string
	hits         int
	inFlight     int
	maxInFlight  int

	status    atomic.Int64
	replyText atomic.Value // string
	delay     atomic.Int64 // nanoseconds
}

func startRawRecoveryWorker(t *testing.T) *rawRecoveryWorker {
	t.Helper()
	w := &rawRecoveryWorker{}
	w.status.Store(http.StatusOK)
	w.replyText.Store("")
	w.srv = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.mu.Lock()
		w.hits++
		w.inFlight++
		if w.inFlight > w.maxInFlight {
			w.maxInFlight = w.inFlight
		}
		w.mu.Unlock()
		defer func() {
			w.mu.Lock()
			w.inFlight--
			w.mu.Unlock()
		}()

		body, _ := io.ReadAll(r.Body)
		var event map[string]any
		_ = json.Unmarshal(body, &event)
		w.mu.Lock()
		w.bodies = append(w.bodies, event)
		w.contentTypes = append(w.contentTypes, r.Header.Get("Content-Type"))
		w.mu.Unlock()

		if d := time.Duration(w.delay.Load()); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return
			}
		}
		status := int(w.status.Load())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(status)
		if reply := w.replyText.Load().(string); reply != "" {
			_, _ = rw.Write([]byte(`{"ok":true,"reply_text":` + strconv.Quote(reply) + `}`))
			return
		}
		_, _ = rw.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(w.srv.Close)
	t.Setenv("DIVA_WEBHOOK_URL", w.srv.URL+"/line/inbound")
	t.Setenv("DIVA_CONTRACT_VERSION", "2")
	return w
}

func (w *rawRecoveryWorker) snapshot() (hits, maxInFlight int, bodies []map[string]any, contentTypes []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.hits, w.maxInFlight, append([]map[string]any(nil), w.bodies...), append([]string(nil), w.contentTypes...)
}

// backfillSeams replaces the LINE / Matrix side of the recent-message pass.
type backfillSeams struct {
	mu       sync.Mutex
	chats    []string
	messages map[string][]*line.Message // chat → oldest first
	existing map[string]bool            // message ID → already in the Matrix DB
	queued   []string                   // message IDs queued to Matrix
	listErr  error
	listHook func()
}

func installBackfillSeams(t *testing.T) *backfillSeams {
	t.Helper()
	s := &backfillSeams{messages: map[string][]*line.Message{}, existing: map[string]bool{}}
	oldList, oldFetch, oldExists, oldQueue := listBackfillChatMIDs, fetchRecentMessagesForBackfill, backfillMessageExists, queueBackfillMessage
	t.Cleanup(func() {
		listBackfillChatMIDs, fetchRecentMessagesForBackfill, backfillMessageExists, queueBackfillMessage = oldList, oldFetch, oldExists, oldQueue
	})
	listBackfillChatMIDs = func(context.Context, *LineClient) (int, []string, error) {
		s.mu.Lock()
		hook, err, chats := s.listHook, s.listErr, append([]string(nil), s.chats...)
		s.mu.Unlock()
		if hook != nil {
			hook()
		}
		if err != nil {
			return 0, nil, err
		}
		return len(chats), chats, nil
	}
	fetchRecentMessagesForBackfill = func(_ context.Context, _ *LineClient, chatMID string, limit int) ([]*line.Message, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		oldestFirst := s.messages[chatMID]
		// GetRecentMessagesV2 returns newest first.
		out := make([]*line.Message, 0, len(oldestFirst))
		for i := len(oldestFirst) - 1; i >= 0 && len(out) < limit; i-- {
			copyMsg := *oldestFirst[i]
			out = append(out, &copyMsg)
		}
		return out, nil
	}
	backfillMessageExists = func(_ context.Context, _ *LineClient, msgID string) (bool, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.existing[msgID], nil
	}
	queueBackfillMessage = func(_ *LineClient, msg *line.Message, _ int) bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.queued = append(s.queued, msg.ID)
		return true
	}
	return s
}

func (s *backfillSeams) addChat(chatMID string, msgs ...*line.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chats = append(s.chats, chatMID)
	s.messages[chatMID] = append(s.messages[chatMID], msgs...)
}

func (s *backfillSeams) markExisting(ids ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.existing[id] = true
	}
}

func (s *backfillSeams) queuedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.queued...)
}

func groupText(id, text string) *line.Message {
	return &line.Message{
		ID: id, From: "usender", To: rawRecoveryGroup, ToType: int(ToGroup),
		ContentType: int(ContentText), Text: text, CreatedTime: json.Number("1760000000000"),
	}
}

func newRawRecoveryClient(logs io.Writer) *LineClient {
	lc := newDIVAV2TestClient(logs)
	lc.UserLogin.UserLogin = &database.UserLogin{ID: networkid.UserLoginID(divaTestAccount)}
	return lc
}

// runPrefetch runs one startup recent-message pass synchronously.
func runPrefetch(lc *LineClient) {
	lc.wg.Add(1)
	lc.prefetchMessages(context.Background(), divaRawRecoveryTriggerStartup)
}

// logLines decodes the JSON log lines whose message contains msg.
func logLines(t *testing.T, logs *divaSyncBuffer, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(logs.String()))
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		var line map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			continue
		}
		if m, _ := line["message"].(string); strings.Contains(m, msg) {
			out = append(out, line)
		}
	}
	return out
}

func passSummary(t *testing.T, logs *divaSyncBuffer) map[string]any {
	t.Helper()
	lines := logLines(t, logs, "[DIVA_RAW_RECOVERY] pass finished")
	if len(lines) != 1 {
		t.Fatalf("want exactly one pass summary, got %d: %s", len(lines), logs.String())
	}
	return lines[0]
}

func assertNumber(t *testing.T, fields map[string]any, key string, want int) {
	t.Helper()
	got, ok := fields[key].(float64)
	if !ok || int(got) != want {
		t.Fatalf("%s = %v, want %d (fields: %v)", key, fields[key], want, fields)
	}
}

// ---------------------------------------------------------------------------
// 1. Matrix existing → DIVA v2 origin=backfill, never re-queued to Matrix
// ---------------------------------------------------------------------------

func TestRawRecoveryForwardsExistingMessageWithoutRequeueingMatrix(t *testing.T) {
	worker := startRawRecoveryWorker(t)
	seams := installBackfillSeams(t)
	seams.addChat(rawRecoveryGroup, groupText("700000000000000001", "old"), groupText("700000000000000002", "new"))
	seams.markExisting("700000000000000001")
	logs := &divaSyncBuffer{}
	lc := newRawRecoveryClient(logs)

	runPrefetch(lc)

	hits, _, bodies, _ := worker.snapshot()
	if hits != 1 {
		t.Fatalf("worker hits = %d, want 1 (only the existing message is recovered synchronously)", hits)
	}
	event := bodies[0]
	if event["version"] != float64(2) || event["origin"] != "backfill" || event["event_id"] != "line:700000000000000001" {
		t.Fatalf("recovered event = %v", event)
	}
	if content, _ := event["content"].(map[string]any); content["text"] != "old" {
		t.Fatalf("recovered text = %v", event["content"])
	}
	// The existing message must not go back into the Matrix queue; the new
	// one keeps the normal backfill path.
	if got := seams.queuedIDs(); len(got) != 1 || got[0] != "700000000000000002" {
		t.Fatalf("Matrix queue = %v, want only the non-existing message", got)
	}
	summary := passSummary(t, logs)
	assertNumber(t, summary, "raw_recovery_forwarded", 1)
	if summary["recovery_enabled"] != true || summary["trigger"] != divaRawRecoveryTriggerStartup {
		t.Fatalf("summary = %v", summary)
	}
	if strings.Contains(logs.String(), "\"old\"") {
		t.Fatalf("logs leaked message text: %s", logs.String())
	}
	if lc.divaRawRecoveryActive.Load() {
		t.Fatal("single-flight guard not released after the pass")
	}
}

// ---------------------------------------------------------------------------
// 2. existing image / audio → JSON descriptor only, no media loader
// ---------------------------------------------------------------------------

func TestRawRecoveryMediaIsDescriptorOnly(t *testing.T) {
	worker := startRawRecoveryWorker(t)
	seams := installBackfillSeams(t)
	image := &line.Message{ID: "700000000000000011", From: "usender", To: rawRecoveryGroup, ToType: int(ToGroup),
		ContentType: int(ContentImage), ContentMetadata: map[string]string{"FILE_SIZE": "1234"}}
	audio := &line.Message{ID: "700000000000000012", From: "usender", To: rawRecoveryGroup, ToType: int(ToGroup),
		ContentType: int(ContentAudio), ContentMetadata: map[string]string{"FILE_SIZE": "99", "DURATION": "4000"}}
	seams.addChat(rawRecoveryGroup, image, audio)
	seams.markExisting(image.ID, audio.ID)

	oldPrepare := divaPrepareInboundMedia
	t.Cleanup(func() { divaPrepareInboundMedia = oldPrepare })
	var loaderCalls atomic.Int32
	divaPrepareInboundMedia = func(*LineClient, context.Context, handlers.MediaKind, line.Message, string) (*divaInboundMedia, error) {
		loaderCalls.Add(1)
		return nil, errors.New("must not be called")
	}

	runPrefetch(newRawRecoveryClient(&divaSyncBuffer{}))

	hits, _, bodies, contentTypes := worker.snapshot()
	if hits != 2 {
		t.Fatalf("worker hits = %d, want 2", hits)
	}
	for i, ct := range contentTypes {
		if ct != "application/json" {
			t.Fatalf("request %d content type = %q, want JSON only", i, ct)
		}
	}
	if got := bodies[0]["content"].(map[string]any); got["type"] != "image" || got["file_size"] != float64(1234) {
		t.Fatalf("image descriptor = %v", got)
	}
	if got := bodies[1]["content"].(map[string]any); got["type"] != "audio" || got["duration_ms"] != float64(4000) {
		t.Fatalf("audio descriptor = %v", got)
	}
	if loaderCalls.Load() != 0 {
		t.Fatalf("media loader called %d times during recovery", loaderCalls.Load())
	}
}

// ---------------------------------------------------------------------------
// 3. worker reply_text → never sent to LINE
// ---------------------------------------------------------------------------

func TestRawRecoveryNeverRepliesToLINE(t *testing.T) {
	worker := startRawRecoveryWorker(t)
	worker.replyText.Store("789")
	seams := installBackfillSeams(t)
	seams.addChat(rawRecoveryGroup, groupText("700000000000000021", "123456654"))
	seams.markExisting("700000000000000021")
	logs := &divaSyncBuffer{}

	// The test client has no LINE transport: reaching the send core would fail
	// or panic, so the log assertions below are the only possible outcome.
	runPrefetch(newRawRecoveryClient(logs))

	if hits, _, _, _ := worker.snapshot(); hits != 1 {
		t.Fatalf("worker hits = %d, want 1", hits)
	}
	if !strings.Contains(logs.String(), "recovered backfill event, not sending") {
		t.Fatalf("reply refusal not logged: %s", logs.String())
	}
	if strings.Contains(logs.String(), "DIVA auto-reply sent") {
		t.Fatalf("recovery sent a LINE reply: %s", logs.String())
	}
	assertNumber(t, passSummary(t, logs), "raw_recovery_forwarded", 1)
}

// ---------------------------------------------------------------------------
// 4. worker unreachable / 500 / timeout → fail-open, Matrix backfill continues
// ---------------------------------------------------------------------------

func TestRawRecoveryFailOpen(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, w *rawRecoveryWorker)
	}{
		{"unreachable", func(_ *testing.T, w *rawRecoveryWorker) { w.srv.Close() }},
		{"status_500", func(_ *testing.T, w *rawRecoveryWorker) { w.status.Store(http.StatusInternalServerError) }},
		{"timeout", func(t *testing.T, w *rawRecoveryWorker) {
			w.delay.Store(int64(500 * time.Millisecond))
			old := divaRawRecoveryTimeout
			divaRawRecoveryTimeout = 50 * time.Millisecond
			t.Cleanup(func() { divaRawRecoveryTimeout = old })
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			worker := startRawRecoveryWorker(t)
			tc.setup(t, worker)
			seams := installBackfillSeams(t)
			seams.addChat(rawRecoveryGroup,
				groupText("700000000000000031", "a"),
				groupText("700000000000000032", "b"),
				groupText("700000000000000033", "c"))
			seams.markExisting("700000000000000031", "700000000000000032")
			logs := &divaSyncBuffer{}
			lc := newRawRecoveryClient(logs)

			done := make(chan struct{})
			go func() {
				runPrefetch(lc)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("recovery failure blocked the backfill pass")
			}

			if got := seams.queuedIDs(); len(got) != 1 || got[0] != "700000000000000033" {
				t.Fatalf("Matrix backfill did not continue: queued %v", got)
			}
			summary := passSummary(t, logs)
			assertNumber(t, summary, "raw_recovery_failed", 2)
			assertNumber(t, summary, "raw_recovery_forwarded", 0)
			if lc.divaRawRecoveryActive.Load() {
				t.Fatal("guard still held after a failed pass")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 5. five consecutive failures stop recovery forwarding, not Matrix backfill
// ---------------------------------------------------------------------------

func TestRawRecoveryBreakerStopsForwardingButNotMatrixBackfill(t *testing.T) {
	worker := startRawRecoveryWorker(t)
	worker.status.Store(http.StatusInternalServerError)
	seams := installBackfillSeams(t)
	var msgs []*line.Message
	var existing []string
	for i := 0; i < 8; i++ {
		id := strconv.Itoa(700000000000000040 + i)
		msgs = append(msgs, groupText(id, "x"))
		existing = append(existing, id)
	}
	msgs = append(msgs, groupText("700000000000000099", "new"))
	seams.addChat(rawRecoveryGroup, msgs...)
	seams.markExisting(existing...)
	logs := &divaSyncBuffer{}

	runPrefetch(newRawRecoveryClient(logs))

	if hits, _, _, _ := worker.snapshot(); hits != divaRawRecoveryBreakerThreshold {
		t.Fatalf("worker hits = %d, want exactly %d before the breaker opens", hits, divaRawRecoveryBreakerThreshold)
	}
	if got := seams.queuedIDs(); len(got) != 1 || got[0] != "700000000000000099" {
		t.Fatalf("Matrix backfill stopped with the breaker: queued %v", got)
	}
	summary := passSummary(t, logs)
	assertNumber(t, summary, "raw_recovery_failed", 5)
	assertNumber(t, summary, "raw_recovery_skipped_breaker", 3)
	if summary["breaker_open"] != true {
		t.Fatalf("summary = %v", summary)
	}
	if !strings.Contains(logs.String(), "stopping recovery forwarding for this pass") {
		t.Fatalf("breaker not logged: %s", logs.String())
	}
}

func TestRawRecoverySuccessResetsBreakerCount(t *testing.T) {
	p := &divaRawRecoveryPass{enabled: true}
	for i := 0; i < divaRawRecoveryBreakerThreshold-1; i++ {
		p.record(divaRawRecoveryFailed)
	}
	p.record(divaRawRecoveryForwarded)
	for i := 0; i < divaRawRecoveryBreakerThreshold-1; i++ {
		if p.record(divaRawRecoveryFailed) {
			t.Fatal("breaker opened although a success reset the consecutive count")
		}
	}
	if !p.record(divaRawRecoveryFailed) || !p.isBreakerOpen() {
		t.Fatal("breaker did not open on the 5th consecutive failure")
	}
}

// ---------------------------------------------------------------------------
// 6. direct text existing → not forwarded; direct Call existing → forwarded, no reply
// ---------------------------------------------------------------------------

func TestRawRecoveryKeepsDirectMessagePolicy(t *testing.T) {
	worker := startRawRecoveryWorker(t)
	worker.replyText.Store(directCallReplyForTest)
	seams := installBackfillSeams(t)
	directText := &line.Message{ID: "700000000000000061", From: "usender", To: divaTestAccount,
		ToType: int(ToUser), ContentType: int(ContentText), Text: "private"}
	directCall := &line.Message{ID: "700000000000000062", From: "usender", To: divaTestAccount,
		ToType: int(ToUser), ContentType: int(ContentText),
		ContentMetadata: map[string]string{"MESSAGE_TARGET": "x"}}
	seams.addChat("usender", directText, directCall)
	seams.markExisting(directText.ID, directCall.ID)
	logs := &divaSyncBuffer{}

	runPrefetch(newRawRecoveryClient(logs))

	hits, _, bodies, _ := worker.snapshot()
	if hits != 1 {
		t.Fatalf("worker hits = %d, want only the direct Call", hits)
	}
	if bodies[0]["event_id"] != "line:700000000000000062" || bodies[0]["origin"] != "backfill" {
		t.Fatalf("forwarded event = %v", bodies[0])
	}
	if content, _ := bodies[0]["content"].(map[string]any); content["type"] != "call" {
		t.Fatalf("direct call content = %v", bodies[0]["content"])
	}
	if !strings.Contains(logs.String(), "recovered backfill event, not sending") ||
		strings.Contains(logs.String(), "DIVA auto-reply sent") {
		t.Fatalf("direct Call recovery replied or did not log the refusal: %s", logs.String())
	}
	if strings.Contains(logs.String(), "private") {
		t.Fatalf("direct text leaked into logs: %s", logs.String())
	}
}

const directCallReplyForTest = "這個帳號不開放通話"

// ---------------------------------------------------------------------------
// 7. decrypt failed existing → skipped, not forwarded
// ---------------------------------------------------------------------------

func TestRawRecoverySkipsDecryptionFailures(t *testing.T) {
	worker := startRawRecoveryWorker(t)
	seams := installBackfillSeams(t)
	encrypted := &line.Message{ID: "700000000000000071", From: "usender", To: rawRecoveryGroup,
		ToType: int(ToGroup), ContentType: int(ContentText), Chunks: []string{"a", "b", "c", "d", "e"}}
	seams.addChat(rawRecoveryGroup, encrypted)
	seams.markExisting(encrypted.ID)
	logs := &divaSyncBuffer{}

	// No E2EE manager: the encrypted message cannot be decrypted.
	runPrefetch(newRawRecoveryClient(logs))

	if hits, _, _, _ := worker.snapshot(); hits != 0 {
		t.Fatalf("worker hits = %d, decryption failures must not be forwarded", hits)
	}
	summary := passSummary(t, logs)
	assertNumber(t, summary, "raw_recovery_skipped_decrypt", 1)
	assertNumber(t, summary, "raw_recovery_forwarded", 0)
	assertNumber(t, summary, "raw_recovery_failed", 0)
}

// ---------------------------------------------------------------------------
// 8. len(msgs) == limit → window_full=true
// ---------------------------------------------------------------------------

func TestRecentMessageBackfillLogsWindowFull(t *testing.T) {
	startRawRecoveryWorker(t)
	seams := installBackfillSeams(t)
	var full, partial []*line.Message
	for i := 0; i < startupBackfillMessageLimit; i++ {
		full = append(full, groupText(strconv.Itoa(710000000000000000+i), "x"))
	}
	for i := 0; i < startupBackfillMessageLimit-1; i++ {
		partial = append(partial, groupText(strconv.Itoa(720000000000000000+i), "x"))
	}
	seams.addChat(rawRecoveryGroup, full...)
	seams.addChat("cgroup0000000000000000000000000002", partial...)
	for _, msg := range full {
		seams.markExisting(msg.ID)
	}
	logs := &divaSyncBuffer{}

	runPrefetch(newRawRecoveryClient(logs))

	byChat := map[string]map[string]any{}
	for _, line := range logLines(t, logs, "Finished recent-message backfill") {
		byChat[line["chat_mid"].(string)] = line
	}
	if byChat[rawRecoveryGroup]["window_full"] != true {
		t.Fatalf("50-message window not reported full: %v", byChat[rawRecoveryGroup])
	}
	if byChat["cgroup0000000000000000000000000002"]["window_full"] != false {
		t.Fatalf("49-message window reported full: %v", byChat["cgroup0000000000000000000000000002"])
	}
	recoveryLines := logLines(t, logs, "[DIVA_RAW_RECOVERY] finished chat")
	if len(recoveryLines) != 1 || recoveryLines[0]["chat_mid"] != rawRecoveryGroup || recoveryLines[0]["window_full"] != true {
		t.Fatalf("per-chat recovery log = %v", recoveryLines)
	}
	assertNumber(t, recoveryLines[0], "raw_recovery_forwarded", startupBackfillMessageLimit)
	assertNumber(t, recoveryLines[0], "raw_recovery_failed", 0)
	summary := passSummary(t, logs)
	assertNumber(t, summary, "chats", 2)
	assertNumber(t, summary, "window_full_chats", 1)
}

// ---------------------------------------------------------------------------
// 9. recovery forwarding concurrency is bounded by the chat workers
// ---------------------------------------------------------------------------

func TestRawRecoveryConcurrencyIsBounded(t *testing.T) {
	worker := startRawRecoveryWorker(t)
	worker.delay.Store(int64(20 * time.Millisecond))
	seams := installBackfillSeams(t)
	const chats, perChat = 12, 6
	for c := 0; c < chats; c++ {
		chat := "cgroup00000000000000000000000001" + strconv.Itoa(10+c)
		var msgs []*line.Message
		for m := 0; m < perChat; m++ {
			msg := groupText(strconv.Itoa(730000000000000000+c*100+m), "x")
			msg.To = chat
			msgs = append(msgs, msg)
			seams.markExisting(msg.ID)
		}
		seams.addChat(chat, msgs...)
	}

	runPrefetch(newRawRecoveryClient(&divaSyncBuffer{}))

	hits, maxInFlight, _, _ := worker.snapshot()
	if hits != chats*perChat {
		t.Fatalf("worker hits = %d, want %d", hits, chats*perChat)
	}
	if maxInFlight > prefetchMessagesConcurrency {
		t.Fatalf("max in-flight recovery requests = %d, want <= %d", maxInFlight, prefetchMessagesConcurrency)
	}
}

// A second pass while one is running keeps its Matrix backfill but does not
// forward recovered messages (single-flight across startup / fullSync / reconcile).
func TestRawRecoverySecondPassDoesNotForwardWhileOneIsActive(t *testing.T) {
	worker := startRawRecoveryWorker(t)
	seams := installBackfillSeams(t)
	seams.addChat(rawRecoveryGroup, groupText("700000000000000091", "a"), groupText("700000000000000092", "b"))
	seams.markExisting("700000000000000091")
	logs := &divaSyncBuffer{}
	lc := newRawRecoveryClient(logs)
	lc.divaRawRecoveryActive.Store(true) // another pass holds the guard

	runPrefetch(lc)

	if hits, _, _, _ := worker.snapshot(); hits != 0 {
		t.Fatalf("worker hits = %d, want 0 while another pass is active", hits)
	}
	if got := seams.queuedIDs(); len(got) != 1 || got[0] != "700000000000000092" {
		t.Fatalf("Matrix backfill did not run: queued %v", got)
	}
	if summary := passSummary(t, logs); summary["disabled_reason"] != divaRawRecoveryDisabledAlreadyActive {
		t.Fatalf("summary = %v", summary)
	}
	if !lc.divaRawRecoveryActive.Load() {
		t.Fatal("a pass released a guard it did not hold")
	}
}

// v1 never forwards backfill, so recovery is off too.
func TestRawRecoveryDisabledUnderContractV1(t *testing.T) {
	worker := startRawRecoveryWorker(t)
	t.Setenv("DIVA_CONTRACT_VERSION", "1")
	seams := installBackfillSeams(t)
	seams.addChat(rawRecoveryGroup, groupText("700000000000000095", "a"))
	seams.markExisting("700000000000000095")
	logs := &divaSyncBuffer{}

	runPrefetch(newRawRecoveryClient(logs))

	if hits, _, _, _ := worker.snapshot(); hits != 0 {
		t.Fatalf("worker hits = %d under contract v1", hits)
	}
	if summary := passSummary(t, logs); summary["disabled_reason"] != divaRawRecoveryDisabledContractV1 {
		t.Fatalf("summary = %v", summary)
	}
}

// ---------------------------------------------------------------------------
// 10. POST /diva/v1/raw-history/reconcile
// ---------------------------------------------------------------------------

type reconcileHarness struct {
	lc  *LineClient
	srv *httptest.Server
}

func newReconcileHarness(t *testing.T, logs io.Writer, cfg divaControlConfig) *reconcileHarness {
	t.Helper()
	lc := newRawRecoveryClient(logs)
	login := &bridgev2.UserLogin{Client: lc}
	lc.UserLogin.Client = lc
	cfg.token = divaTestToken
	if cfg.logins == nil {
		cfg.logins = func() []*bridgev2.UserLogin { return []*bridgev2.UserLogin{login} }
	}
	cfg.log = zerolog.New(io.Discard)
	cfg.send = func(context.Context, *LineClient, *lineOutboundRequest) (*lineOutboundResult, error) {
		t.Error("reconcile must never send to LINE")
		return nil, errors.New("unexpected send")
	}
	s := newDIVAControlServer(cfg)
	srv := httptest.NewServer(s.handler())
	t.Cleanup(func() {
		srv.Close()
		s.stop(5 * time.Second)
	})
	return &reconcileHarness{lc: lc, srv: srv}
}

// connect gives the client an active run, like Connect does.
func (h *reconcileHarness) connect(t *testing.T) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h.lc.runMu.Lock()
	h.lc.activeRun = &lineClientRun{ctx: ctx, cancel: cancel}
	h.lc.runMu.Unlock()
	t.Cleanup(func() {
		cancel()
		h.lc.wg.Wait()
	})
	return cancel
}

func (h *reconcileHarness) post(t *testing.T, method, token string) (int, divaReconcileResponse) {
	t.Helper()
	req, _ := http.NewRequest(method, h.srv.URL+"/diva/v1/raw-history/reconcile", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s reconcile: %v", method, err)
	}
	defer resp.Body.Close()
	var out divaReconcileResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func waitGuardReleased(t *testing.T, lc *LineClient) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for lc.divaRawRecoveryActive.Load() {
		if time.Now().After(deadline) {
			t.Fatal("reconcile guard was never released")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestReconcileEndpointAuth(t *testing.T) {
	var calls atomic.Int32
	h := newReconcileHarness(t, io.Discard, divaControlConfig{startRawReconcile: func(*LineClient) string {
		calls.Add(1)
		return divaReconcileStarted
	}})

	if status, _ := h.post(t, http.MethodPost, ""); status != http.StatusUnauthorized {
		t.Fatalf("no token status = %d", status)
	}
	if status, _ := h.post(t, http.MethodPost, "wrong-token-0123456789abcdefghijklmnopqrstuvwxyz"); status != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d", status)
	}
	if status, _ := h.post(t, http.MethodGet, divaTestToken); status != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d", status)
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected requests started %d reconcile passes", calls.Load())
	}
	status, out := h.post(t, http.MethodPost, divaTestToken)
	if status != http.StatusAccepted || !out.OK || out.Result != divaReconcileStarted || out.Started != 1 {
		t.Fatalf("authorized reconcile = %d %+v", status, out)
	}
	if calls.Load() != 1 {
		t.Fatalf("start calls = %d, want 1", calls.Load())
	}
}

func TestReconcileEndpointSingleFlightAndRawOnlyPass(t *testing.T) {
	worker := startRawRecoveryWorker(t)
	seams := installBackfillSeams(t)
	seams.addChat(rawRecoveryGroup, groupText("700000000000000101", "a"), groupText("700000000000000102", "b"))
	seams.markExisting("700000000000000101")
	release := make(chan struct{})
	var blockOnce sync.Once
	seams.listHook = func() { blockOnce.Do(func() { <-release }) }
	logs := &divaSyncBuffer{}
	h := newReconcileHarness(t, logs, divaControlConfig{})
	h.connect(t)

	status, out := h.post(t, http.MethodPost, divaTestToken)
	if status != http.StatusAccepted || out.Started != 1 {
		t.Fatalf("first reconcile = %d %+v", status, out)
	}
	// Concurrent calls while the first pass runs must not stack.
	var wg sync.WaitGroup
	var conflicts atomic.Int32
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if status, out := h.post(t, http.MethodPost, divaTestToken); status == http.StatusConflict && out.Result == divaReconcileAlreadyRunning {
				conflicts.Add(1)
			}
		}()
	}
	wg.Wait()
	if conflicts.Load() != 5 {
		t.Fatalf("concurrent reconcile conflicts = %d, want 5", conflicts.Load())
	}
	close(release)
	waitGuardReleased(t, h.lc)

	// Raw-only: the existing message is recovered, the new one is left to the
	// normal Matrix path (not queued, not forwarded).
	hits, _, bodies, _ := worker.snapshot()
	if hits != 1 || bodies[0]["event_id"] != "line:700000000000000101" || bodies[0]["origin"] != "backfill" {
		t.Fatalf("reconcile forwarded %d events: %v", hits, bodies)
	}
	if got := seams.queuedIDs(); len(got) != 0 {
		t.Fatalf("raw-only reconcile queued Matrix events: %v", got)
	}
	if summary := passSummary(t, logs); summary["trigger"] != divaRawRecoveryTriggerReconcile {
		t.Fatalf("summary = %v", summary)
	}

	// Once finished, a new reconcile can start again.
	if status, out := h.post(t, http.MethodPost, divaTestToken); status != http.StatusAccepted || out.Started != 1 {
		t.Fatalf("reconcile after completion = %d %+v", status, out)
	}
	waitGuardReleased(t, h.lc)
}

func TestReconcileEndpointFailureIsFailOpen(t *testing.T) {
	startRawRecoveryWorker(t)
	seams := installBackfillSeams(t)
	seams.listErr = errors.New("LINE unavailable")
	h := newReconcileHarness(t, &divaSyncBuffer{}, divaControlConfig{})
	h.connect(t)

	if status, _ := h.post(t, http.MethodPost, divaTestToken); status != http.StatusAccepted {
		t.Fatalf("reconcile status = %d", status)
	}
	waitGuardReleased(t, h.lc)

	// A crash inside the pass is contained and releases the guard too.
	seams.mu.Lock()
	seams.listErr = nil
	seams.listHook = func() { panic("boom") }
	seams.mu.Unlock()
	if status, _ := h.post(t, http.MethodPost, divaTestToken); status != http.StatusAccepted {
		t.Fatalf("reconcile after failure status = %d", status)
	}
	waitGuardReleased(t, h.lc)
	if status, _ := h.post(t, http.MethodPost, divaTestToken); status != http.StatusAccepted {
		t.Fatal("guard not reusable after a crashed pass")
	}
	waitGuardReleased(t, h.lc)
}

func TestReconcileEndpointUnavailable(t *testing.T) {
	// Not connected: no active run.
	startRawRecoveryWorker(t)
	h := newReconcileHarness(t, io.Discard, divaControlConfig{})
	if status, out := h.post(t, http.MethodPost, divaTestToken); status != http.StatusServiceUnavailable || out.OK || out.Unavailable != 1 {
		t.Fatalf("not connected = %d %+v", status, out)
	}

	// Connected but DIVA webhook not configured.
	t.Setenv("DIVA_WEBHOOK_URL", "")
	h.connect(t)
	if status, out := h.post(t, http.MethodPost, divaTestToken); status != http.StatusServiceUnavailable || out.Unavailable != 1 {
		t.Fatalf("webhook unset = %d %+v", status, out)
	}

	// No logins at all.
	none := newReconcileHarness(t, io.Discard, divaControlConfig{logins: func() []*bridgev2.UserLogin { return nil }})
	if status, out := none.post(t, http.MethodPost, divaTestToken); status != http.StatusServiceUnavailable || out.Result != "unavailable" {
		t.Fatalf("no logins = %d %+v", status, out)
	}
}

// ---------------------------------------------------------------------------
// 11. regression: non-existing startup backfill keeps the old path
// ---------------------------------------------------------------------------

func TestStartupBackfillNonExistingMessagesKeepMatrixPath(t *testing.T) {
	worker := startRawRecoveryWorker(t)
	seams := installBackfillSeams(t)
	seams.addChat(rawRecoveryGroup,
		groupText("700000000000000111", "first"),
		groupText("700000000000000112", "second"),
		groupText("700000000000000113", "third"))
	logs := &divaSyncBuffer{}

	runPrefetch(newRawRecoveryClient(logs))

	got := seams.queuedIDs()
	want := []string{"700000000000000111", "700000000000000112", "700000000000000113"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("queued = %v, want oldest-first %v", got, want)
	}
	// Non-existing messages reach DIVA through queueIncomingMessage (replaced
	// here), not through the synchronous recovery path.
	if hits, _, _, _ := worker.snapshot(); hits != 0 {
		t.Fatalf("recovery forwarded %d non-existing messages", hits)
	}
	assertNumber(t, passSummary(t, logs), "raw_recovery_forwarded", 0)
}

// Recovery waits for the whole Matrix backfill: every non-existing message is
// queued to Matrix before the first recovery POST reaches the worker.
func TestRawRecoveryRunsAfterMatrixBackfill(t *testing.T) {
	worker := startRawRecoveryWorker(t)
	seams := installBackfillSeams(t)
	var seq atomic.Int64
	var lastQueued, firstPost atomic.Int64
	oldQueue := queueBackfillMessage
	queueBackfillMessage = func(lc *LineClient, msg *line.Message, opType int) bool {
		lastQueued.Store(seq.Add(1))
		return oldQueue(lc, msg, opType)
	}
	worker.srv.Config.Handler = wrapFirstHit(worker.srv.Config.Handler, func() { firstPost.CompareAndSwap(0, seq.Add(1)) })
	for c := 0; c < 6; c++ {
		chat := "cgroup0000000000000000000000000" + strconv.Itoa(300+c)
		old := groupText(strconv.Itoa(740000000000000000+c*10), "old")
		fresh := groupText(strconv.Itoa(740000000000000001+c*10), "new")
		old.To, fresh.To = chat, chat
		seams.addChat(chat, old, fresh)
		seams.markExisting(old.ID)
	}

	runPrefetch(newRawRecoveryClient(&divaSyncBuffer{}))

	if len(seams.queuedIDs()) != 6 {
		t.Fatalf("queued = %v", seams.queuedIDs())
	}
	if hits, _, _, _ := worker.snapshot(); hits != 6 {
		t.Fatalf("worker hits = %d, want 6", hits)
	}
	if firstPost.Load() <= lastQueued.Load() {
		t.Fatalf("recovery POST (seq %d) ran before the Matrix backfill finished (last queue seq %d)", firstPost.Load(), lastQueued.Load())
	}
}

func wrapFirstHit(next http.Handler, onHit func()) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		onHit()
		next.ServeHTTP(w, r)
	})
}
