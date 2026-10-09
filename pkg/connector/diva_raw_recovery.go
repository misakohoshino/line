package connector

// DIVA Raw History recovery reconcile.
//
// The startup / fullSync recent-message backfill skips messages the local
// Matrix/Beeper DB already has. If the DIVA worker or its Raw History DB was
// down when such a message first arrived, the worker's fail-open design means
// the Raw History row was never written, and the old skip meant DIVA never saw
// the message again. Recovery closes that gap without touching Matrix:
//
//	backfillRecentMessages (Matrix phase, unchanged timing)
//	  ├─ Matrix DB does not have it → existing backfill path (unchanged)
//	  └─ Matrix DB already has it   → collected for recovery, not re-queued
//	runDIVARawRecoveryPhase (after the Matrix phase and its chat resync)
//	  └─ recover() per collected message (raw-only, this file)
//	       ├─ same DIVA admission as live/backfill
//	       ├─ decrypt; failure → skip, retry next pass
//	       ├─ v2 event, origin=backfill, JSON only
//	       └─ synchronous POST, never QueueRemoteEvent
//
// Safety rules:
//   - origin stays "backfill": the worker only writes Raw History for it and
//     never replies; the bridge never sends reply_text for it either.
//   - No media bytes: recovered media stays descriptor-only JSON, so the
//     worker records UNAVAILABLE and never calls a paid STT / Vision provider.
//   - Bounded: the recovery phase uses at most prefetchMessagesConcurrency
//     chat workers and each POST is synchronous inside its worker, so at most
//     that many recovery requests are in flight. No per-message goroutine.
//   - Matrix first: recovery starts only after the Matrix backfill finished,
//     so it never delays messages Matrix is still missing.
//   - Fail-open: a failed POST is counted and the pass goes on. After
//     divaRawRecoveryBreakerThreshold consecutive failures the pass stops
//     forwarding; the Matrix backfill itself always continues.
//   - Single-flight: one recovery pass per LineClient at a time.
//   - The live receive path is not changed.
//
// POST /diva/v1/raw-history/reconcile (same listener and Bearer token as
// /diva/v1/send) starts one raw-only pass. It never sends to LINE.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

const (
	divaRawRecoveryTriggerStartup   = "startup"
	divaRawRecoveryTriggerFullSync  = "full_sync"
	divaRawRecoveryTriggerReconcile = "reconcile_endpoint"

	// divaRawRecoveryBreakerThreshold consecutive failed recovery POSTs stop
	// recovery forwarding for the rest of the pass.
	divaRawRecoveryBreakerThreshold = 5

	divaRawRecoveryDisabledWebhookUnset  = "webhook_unset"
	divaRawRecoveryDisabledContractV1    = "contract_v1"
	divaRawRecoveryDisabledAlreadyActive = "already_running"
)

// divaRawRecoveryTimeout bounds one synchronous recovery POST. It is the same
// budget as a live JSON forward; tests shorten it.
var divaRawRecoveryTimeout = defaultDIVAWebhookTimeout

type divaRawRecoveryOutcome int

const (
	divaRawRecoveryNotEligible divaRawRecoveryOutcome = iota
	divaRawRecoveryForwarded
	divaRawRecoveryFailed
	divaRawRecoverySkippedDecrypt
	divaRawRecoverySkippedBreaker
)

// divaRawRecoveryStats counts recovery outcomes for one chat.
type divaRawRecoveryStats struct {
	forwarded      int
	failed         int
	skippedDecrypt int
	skippedBreaker int
}

func (s *divaRawRecoveryStats) add(outcome divaRawRecoveryOutcome) {
	switch outcome {
	case divaRawRecoveryForwarded:
		s.forwarded++
	case divaRawRecoveryFailed:
		s.failed++
	case divaRawRecoverySkippedDecrypt:
		s.skippedDecrypt++
	case divaRawRecoverySkippedBreaker:
		s.skippedBreaker++
	}
}

// divaRawRecoveryItem is one message the local Matrix DB already has.
type divaRawRecoveryItem struct {
	msg    *line.Message
	opType int
}

// divaRawRecoveryChat is the recovery work collected for one chat.
type divaRawRecoveryChat struct {
	chatMID    string
	windowFull bool
	items      []divaRawRecoveryItem
}

// divaRawRecoveryPass is the state of one recovery pass. A nil or disabled
// pass is valid and recovers nothing.
type divaRawRecoveryPass struct {
	trigger        string
	endpoint       string
	enabled        bool
	disabledReason string
	holdsGuard     bool
	started        time.Time

	mu                  sync.Mutex
	consecutiveFailures int
	breakerOpen         bool
	chats               int
	windowFullChats     int
	totals              divaRawRecoveryStats
	pending             []divaRawRecoveryChat
}

// divaRawRecoveryEndpoint returns the worker URL, or a disabled reason when
// recovery cannot run with the current DIVA configuration.
func (lc *LineClient) divaRawRecoveryEndpoint() (string, string) {
	endpoint := strings.TrimSpace(os.Getenv("DIVA_WEBHOOK_URL"))
	if endpoint == "" {
		return "", divaRawRecoveryDisabledWebhookUnset
	}
	// v1 cannot label backfill, so it never forwards backfill at all.
	if lc.divaContractVersion() != divaContractV2 {
		return "", divaRawRecoveryDisabledContractV1
	}
	return endpoint, ""
}

// beginDIVARawRecoveryPass prepares recovery for a startup / fullSync pass.
// When another recovery pass is still running, this pass keeps its Matrix
// backfill but does not forward recovered messages.
func (lc *LineClient) beginDIVARawRecoveryPass(trigger string) *divaRawRecoveryPass {
	pass := &divaRawRecoveryPass{trigger: trigger, started: time.Now()}
	endpoint, disabled := lc.divaRawRecoveryEndpoint()
	if disabled != "" {
		pass.disabledReason = disabled
		return pass
	}
	if !lc.divaRawRecoveryActive.CompareAndSwap(false, true) {
		pass.disabledReason = divaRawRecoveryDisabledAlreadyActive
		lc.UserLogin.Bridge.Log.Info().
			Str("diva_event", "DIVA_RAW_RECOVERY").
			Str("trigger", trigger).
			Msg("[DIVA_RAW_RECOVERY] another recovery pass is running; this backfill pass will not forward recovered messages")
		return pass
	}
	pass.endpoint = endpoint
	pass.enabled = true
	pass.holdsGuard = true
	return pass
}

// collecting reports whether messages should be collected for recovery.
func (p *divaRawRecoveryPass) collecting() bool {
	return p != nil && p.enabled
}

// addChat records one visited chat, whether its recent-message window was full
// (len(msgs) == limit, so older messages may be truncated), and the messages
// the local Matrix DB already has, oldest first.
func (p *divaRawRecoveryPass) addChat(chatMID string, windowFull bool, items []divaRawRecoveryItem) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.chats++
	if windowFull {
		p.windowFullChats++
	}
	if p.enabled && len(items) > 0 {
		p.pending = append(p.pending, divaRawRecoveryChat{chatMID: chatMID, windowFull: windowFull, items: items})
	}
}

// runDIVARawRecoveryPhase forwards the collected messages, at most
// prefetchMessagesConcurrency chats at a time and one synchronous POST per
// worker. It returns when every chat is done or ctx ends.
func (lc *LineClient) runDIVARawRecoveryPhase(ctx context.Context, p *divaRawRecoveryPass) {
	if p == nil || !p.enabled {
		return
	}
	p.mu.Lock()
	chats := p.pending
	p.pending = nil
	p.mu.Unlock()

	workerCount := prefetchMessagesConcurrency
	if len(chats) < workerCount {
		workerCount = len(chats)
	}
	if workerCount == 0 {
		return
	}
	jobs := make(chan divaRawRecoveryChat)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer workers.Done()
			for chat := range jobs {
				p.recoverChat(ctx, lc, chat)
			}
		}()
	}
	for _, chat := range chats {
		if ctx.Err() != nil {
			break
		}
		jobs <- chat
	}
	close(jobs)
	workers.Wait()
}

func (p *divaRawRecoveryPass) recoverChat(ctx context.Context, lc *LineClient, chat divaRawRecoveryChat) {
	start := time.Now()
	var stats divaRawRecoveryStats
	for _, item := range chat.items {
		if ctx.Err() != nil {
			break
		}
		stats.add(p.recover(ctx, lc, item.msg, item.opType))
	}
	lc.UserLogin.Bridge.Log.Debug().
		Str("diva_event", "DIVA_RAW_RECOVERY").
		Str("trigger", p.trigger).
		Str("chat_mid", chat.chatMID).
		Bool("window_full", chat.windowFull).
		Int("candidates", len(chat.items)).
		Int("raw_recovery_forwarded", stats.forwarded).
		Int("raw_recovery_failed", stats.failed).
		Int("raw_recovery_skipped_decrypt", stats.skippedDecrypt).
		Int("raw_recovery_skipped_breaker", stats.skippedBreaker).
		Dur("duration", time.Since(start)).
		Msg("[DIVA_RAW_RECOVERY] finished chat")
}

func (p *divaRawRecoveryPass) isBreakerOpen() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.breakerOpen
}

func (p *divaRawRecoveryPass) record(outcome divaRawRecoveryOutcome) (openedBreaker bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.totals.add(outcome)
	switch outcome {
	case divaRawRecoveryForwarded:
		p.consecutiveFailures = 0
	case divaRawRecoveryFailed:
		p.consecutiveFailures++
		if !p.breakerOpen && p.consecutiveFailures >= divaRawRecoveryBreakerThreshold {
			p.breakerOpen = true
			return true
		}
	}
	return false
}

// finish releases the single-flight guard and logs one non-sensitive summary.
func (p *divaRawRecoveryPass) finish(lc *LineClient) {
	if p == nil {
		return
	}
	if p.holdsGuard {
		lc.divaRawRecoveryActive.Store(false)
	}
	p.mu.Lock()
	totals, chats, windowFull, breaker := p.totals, p.chats, p.windowFullChats, p.breakerOpen
	p.mu.Unlock()
	lc.UserLogin.Bridge.Log.Info().
		Str("diva_event", "DIVA_RAW_RECOVERY").
		Str("trigger", p.trigger).
		Bool("recovery_enabled", p.enabled).
		Str("disabled_reason", p.disabledReason).
		Int("chats", chats).
		Int("window_full_chats", windowFull).
		Int("raw_recovery_forwarded", totals.forwarded).
		Int("raw_recovery_failed", totals.failed).
		Int("raw_recovery_skipped_decrypt", totals.skippedDecrypt).
		Int("raw_recovery_skipped_breaker", totals.skippedBreaker).
		Bool("breaker_open", breaker).
		Dur("duration", time.Since(p.started)).
		Msg("[DIVA_RAW_RECOVERY] pass finished")
}

// recover forwards one message that the local Matrix DB already has to the
// DIVA worker as a raw-only v2 backfill event. It never queues a Matrix event,
// never loads media bytes and never sends to LINE. It runs synchronously in
// the calling recovery worker.
func (p *divaRawRecoveryPass) recover(ctx context.Context, lc *LineClient, msg *line.Message, opType int) divaRawRecoveryOutcome {
	if p == nil || !p.enabled || msg == nil || ctx.Err() != nil {
		return divaRawRecoveryNotEligible
	}
	if !isBridgeableContentType(msg) {
		return divaRawRecoveryNotEligible
	}
	// Same admission as handleDIVAInbound: groups / rooms, plus the direct Call
	// notice. Ordinary direct messages never reach DIVA, and are rejected here
	// before decryption so no peer key is fetched for them.
	isGroupOrRoom := ToType(msg.ToType) == ToRoom || ToType(msg.ToType) == ToGroup
	if !isGroupOrRoom && !divaIsDirectCall(msg, opType) {
		return divaRawRecoveryNotEligible
	}
	if p.isBreakerOpen() {
		p.record(divaRawRecoverySkippedBreaker)
		return divaRawRecoverySkippedBreaker
	}

	chatMID := portalMIDForMessage(msg, opType)
	_, unwrappedText, decryptionFailed := lc.decryptMessageBody(msg, chatMID, opType)
	if decryptionFailed {
		// Writing it now would store text=NULL for good: Raw History dedupes on
		// event_id. Leave the gap for the next reconcile, when keys may exist.
		lc.UserLogin.Bridge.Log.Debug().
			Str("diva_event", "DIVA_RAW_RECOVERY").
			Str("group_id", chatMID).
			Str("message_id", msg.ID).
			Int("content_type", msg.ContentType).
			Msg("[DIVA_RAW_RECOVERY] decryption failed, not forwarding; will retry on the next reconcile")
		p.record(divaRawRecoverySkippedDecrypt)
		return divaRawRecoverySkippedDecrypt
	}

	event := lc.buildDIVAV2Event(msg, chatMID, unwrappedText, false, opType, divaOriginBackfill)
	payload, err := json.Marshal(event)
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Str("message_id", msg.ID).
			Msg("[DIVA_RAW_RECOVERY] failed to encode recovered event")
		return p.fail(lc, msg.ID)
	}

	// Never log message text or decrypted payloads here.
	lc.UserLogin.Bridge.Log.Debug().
		Str("diva_event", "DIVA_RX").
		Str("group_id", chatMID).
		Str("sender_id", msg.From).
		Str("message_id", msg.ID).
		Int("content_type", msg.ContentType).
		Str("origin", string(divaOriginBackfill)).
		Bool("recovery", true).
		Msg("[DIVA_RX]")

	replyRequested, err := postDIVARawRecovery(ctx, p.endpoint, payload)
	if err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).
			Str("message_id", msg.ID).
			Bool("recovery", true).
			Msg("[DIVA_RAW_RECOVERY] delivery failed")
		return p.fail(lc, msg.ID)
	}
	if replyRequested {
		lc.UserLogin.Bridge.Log.Warn().
			Str("group_id", chatMID).
			Str("message_id", msg.ID).
			Bool("recovery", true).
			Msg("DIVA worker returned reply_text for a recovered backfill event, not sending")
	}
	p.record(divaRawRecoveryForwarded)
	return divaRawRecoveryForwarded
}

func (p *divaRawRecoveryPass) fail(lc *LineClient, messageID string) divaRawRecoveryOutcome {
	if p.record(divaRawRecoveryFailed) {
		lc.UserLogin.Bridge.Log.Warn().
			Str("diva_event", "DIVA_RAW_RECOVERY").
			Str("trigger", p.trigger).
			Str("message_id", messageID).
			Int("consecutive_failures", divaRawRecoveryBreakerThreshold).
			Msg("[DIVA_RAW_RECOVERY] too many consecutive failures; stopping recovery forwarding for this pass (Matrix backfill continues)")
	}
	return divaRawRecoveryFailed
}

// postDIVARawRecovery posts one recovered event and waits for the worker. It
// reports whether the worker asked for a reply; the caller never sends one.
func postDIVARawRecovery(ctx context.Context, endpoint string, payload []byte) (bool, error) {
	reqCtx, cancel := context.WithTimeout(ctx, divaRawRecoveryTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	signDIVAInbound(req, payload)
	resp, err := divaHTTPClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, fmt.Errorf("DIVA worker rejected recovered event with status %d", resp.StatusCode)
	}
	if readErr != nil {
		return false, readErr
	}
	var decision divaInboundDecision
	if len(body) > 0 {
		if err := json.Unmarshal(body, &decision); err != nil {
			return false, errors.New("DIVA worker returned invalid JSON")
		}
	}
	return strings.TrimSpace(decision.ReplyText) != "", nil
}

// ---------------------------------------------------------------------------
// raw-only reconcile pass (control endpoint)
// ---------------------------------------------------------------------------

const (
	divaReconcileStarted        = "started"
	divaReconcileAlreadyRunning = "already_running"
	divaReconcileNotConnected   = "not_connected"
	divaReconcileDisabled       = "disabled"
)

// startDIVARawHistoryReconcile starts one raw-only recovery pass in the
// background for the current run, unless one is already running. It returns
// at once and never blocks the LINE receive loop.
func (lc *LineClient) startDIVARawHistoryReconcile() string {
	endpoint, disabled := lc.divaRawRecoveryEndpoint()
	if disabled != "" {
		return divaReconcileDisabled
	}
	if lc.isSessionInvalidated() {
		return divaReconcileNotConnected
	}
	lc.runMu.Lock()
	run := lc.activeRun
	if lc.stopped || run == nil || run.ctx == nil || run.ctx.Err() != nil {
		lc.runMu.Unlock()
		return divaReconcileNotConnected
	}
	if !lc.divaRawRecoveryActive.CompareAndSwap(false, true) {
		lc.runMu.Unlock()
		return divaReconcileAlreadyRunning
	}
	// Disconnect sets stopped under runMu before it waits on wg, so this Add
	// can never race a finished Wait.
	lc.wg.Add(1)
	ctx := run.ctx
	lc.runMu.Unlock()

	pass := &divaRawRecoveryPass{
		trigger:    divaRawRecoveryTriggerReconcile,
		endpoint:   endpoint,
		enabled:    true,
		holdsGuard: true,
		started:    time.Now(),
	}
	go func() {
		defer lc.wg.Done()
		defer pass.finish(lc)
		defer func() {
			// A reconcile failure must never take the bridge down.
			if r := recover(); r != nil {
				lc.UserLogin.Bridge.Log.Error().
					Str("diva_event", "DIVA_RAW_RECOVERY").
					Str("panic", fmt.Sprint(r)).
					Msg("[DIVA_RAW_RECOVERY] reconcile pass crashed")
			}
		}()
		lc.runRecentMessagesPass(ctx, true, pass)
		lc.runDIVARawRecoveryPhase(ctx, pass)
	}()
	return divaReconcileStarted
}

type divaReconcileResponse struct {
	OK             bool   `json:"ok"`
	Result         string `json:"result"`
	Started        int    `json:"started"`
	AlreadyRunning int    `json:"already_running"`
	Unavailable    int    `json:"unavailable"`
}

func (s *divaControlServer) handleRawHistoryReconcile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeDIVAError(w, http.StatusMethodNotAllowed, "", outboundInvalidRequest, "method not allowed")
		return
	}
	if !s.authorized(r) {
		writeDIVAError(w, http.StatusUnauthorized, "", outboundUnauthorized, "missing or invalid bearer token")
		return
	}
	var logins []*bridgev2.UserLogin
	if s.cfg.logins != nil {
		logins = s.cfg.logins()
	}
	start := s.cfg.startRawReconcile
	if start == nil {
		start = func(lc *LineClient) string { return lc.startDIVARawHistoryReconcile() }
	}

	var out divaReconcileResponse
	for _, login := range logins {
		if login == nil {
			continue
		}
		lc, ok := login.Client.(*LineClient)
		if !ok || lc == nil {
			continue
		}
		switch start(lc) {
		case divaReconcileStarted:
			out.Started++
		case divaReconcileAlreadyRunning:
			out.AlreadyRunning++
		default:
			out.Unavailable++
		}
	}

	status := http.StatusServiceUnavailable
	out.Result = "unavailable"
	switch {
	case out.Started > 0:
		status, out.Result, out.OK = http.StatusAccepted, divaReconcileStarted, true
	case out.AlreadyRunning > 0:
		status, out.Result, out.OK = http.StatusConflict, divaReconcileAlreadyRunning, true
	}
	s.cfg.log.Info().
		Str("diva_event", "DIVA_RAW_RECOVERY").
		Str("result", out.Result).
		Int("started", out.Started).
		Int("already_running", out.AlreadyRunning).
		Int("unavailable", out.Unavailable).
		Msg("[DIVA_RAW_RECOVERY] reconcile requested")
	writeDIVAJSON(w, status, out)
}