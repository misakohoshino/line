package connector

// DIVA outbound control endpoint (LINE-1A PR 4).
//
//	Server A → POST /diva/v1/send → shared send core → LINE
//
// Safety model:
//   - The listener only starts when DIVA_CONTROL_TOKEN is set (at least 32
//     characters). Every send needs "Authorization: Bearer <token>".
//   - It is meant for the Docker internal network: compose must use `expose`,
//     never `ports`.
//   - request_id is mandatory. A request_id is either in flight or completed;
//     a second call with the same request_id never starts a second LINE send.
//   - line.Client.SendMessage does not take a context, so an HTTP wait budget
//     (timeout_ms) cannot cancel a LINE send. The send runs in the background;
//     the handler waits at most timeout_ms and otherwise answers
//     TIMEOUT / delivery=unknown. The real result is stored under the same
//     request_id when the send finishes.
//   - Sends to one chat run in arrival order; different chats run in
//     parallel up to a global limit.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2"
)

const (
	divaControlDefaultListen  = ":8090"
	divaControlMinTokenLength = 32
	divaControlMaxBodyBytes   = 1 << 20
	divaControlMaxConcurrent  = 4
	divaControlMaxPending     = 64
	divaControlMaxEntries     = 10000
	divaControlResultTTL      = 10 * time.Minute
	divaControlDefaultWait    = 15 * time.Second
	divaControlMinWait        = 100 * time.Millisecond
	divaControlMaxWait        = 30 * time.Second
	// divaControlSendBudget bounds one background send. The core may make
	// several LINE calls (retries), each limited by the 30 s HTTP client.
	divaControlSendBudget = 3 * time.Minute
)

var (
	divaUUIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	// divaChatMIDPattern accepts LINE group/room chat IDs as opaque identifiers.
	// Production inbound has shown group IDs beginning with uppercase C and
	// lengths that are not the old fixed 33-char test shape. Keep only the
	// business boundary here: group/room (c/C/r/R) allowed, direct user
	// targets (u/U) refused. The shared send core remains the format authority.
	divaChatMIDPattern   = regexp.MustCompile(`^[cCrR][0-9A-Za-z]+$`)
	divaUserMIDPattern   = regexp.MustCompile(`^u[0-9A-Za-z]{32}$`)
	divaMessageIDPattern = regexp.MustCompile(`^[0-9]{1,32}$`)
)

// ---------------------------------------------------------------------------
// request contract
// ---------------------------------------------------------------------------

type divaSendRequest struct {
	Version   *int   `json:"version"`
	RequestID string `json:"request_id"`
	Target    struct {
		ChatID     string  `json:"chat_id"`
		AccountMID *string `json:"account_mid"`
	} `json:"target"`
	MessageType string          `json:"message_type"`
	Content     json.RawMessage `json:"content"`
	Relations   *struct {
		ReplyTo *struct {
			MessageID string `json:"message_id"`
		} `json:"reply_to"`
		Mentions   []divaSendMention `json:"mentions"`
		MentionAll bool              `json:"mention_all"`
	} `json:"relations"`
	Options *struct {
		ReplyFallback string `json:"reply_fallback"`
		TimeoutMS     *int   `json:"timeout_ms"`
	} `json:"options"`
}

type divaSendMention struct {
	MID  string `json:"mid"`
	Name string `json:"name"`
}

type divaTextContent struct {
	Text string `json:"text"`
}

const (
	divaReplyFallbackFail   = "fail"
	divaReplyFallbackDrop   = "send_without_reply"
	divaMessageTypeText     = "text"
	divaSendContractVersion = 1
)

// divaSendJob is a validated request ready for the send core.
type divaSendJob struct {
	requestID   string
	accountMID  string
	chatID      string
	wait        time.Duration
	req         *lineOutboundRequest
	fingerprint [32]byte
}

// divaRequestError is a request-level rejection: nothing was queued or sent.
type divaRequestError struct {
	status int
	code   outboundErrorCode
	detail string
}

func (e *divaRequestError) Error() string { return e.detail }

func invalidRequest(format string, args ...any) *divaRequestError {
	return &divaRequestError{status: http.StatusBadRequest, code: outboundInvalidRequest, detail: fmt.Sprintf(format, args...)}
}

// parseDIVASendRequest validates the body and builds the send-core request.
// It never touches LINE.
func parseDIVASendRequest(body []byte) (*divaSendRequest, *divaSendJob, *divaRequestError) {
	var raw divaSendRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return nil, nil, invalidRequest("invalid JSON: %v", err)
	}
	if dec.More() {
		return &raw, nil, invalidRequest("invalid JSON: trailing data")
	}
	if raw.Version == nil || *raw.Version != divaSendContractVersion {
		return &raw, nil, invalidRequest("unsupported version: only version %d is accepted", divaSendContractVersion)
	}
	if !divaUUIDPattern.MatchString(raw.RequestID) {
		return &raw, nil, invalidRequest("request_id must be a UUID")
	}
	if !divaChatMIDPattern.MatchString(raw.Target.ChatID) {
		return &raw, nil, invalidRequest("target.chat_id must be a LINE group/room chat ID beginning with c/C or r/R; direct user targets are not allowed")
	}
	accountMID := ""
	if raw.Target.AccountMID != nil {
		accountMID = *raw.Target.AccountMID
		if !divaUserMIDPattern.MatchString(accountMID) {
			return &raw, nil, invalidRequest("target.account_mid must be a LINE user MID or null")
		}
	}
	if raw.MessageType != divaMessageTypeText {
		return &raw, nil, &divaRequestError{
			status: http.StatusBadRequest,
			code:   outboundUnsupportedMessageType,
			detail: fmt.Sprintf("message_type %q is not supported; only %q", raw.MessageType, divaMessageTypeText),
		}
	}

	var content divaTextContent
	contentDec := json.NewDecoder(bytes.NewReader(raw.Content))
	contentDec.DisallowUnknownFields()
	if len(raw.Content) == 0 || contentDec.Decode(&content) != nil {
		return &raw, nil, invalidRequest("content must be {\"text\": \"...\"}")
	}
	if strings.TrimSpace(content.Text) == "" {
		return &raw, nil, invalidRequest("content.text must not be empty")
	}

	replyTo := ""
	var mentions []divaSendMention
	mentionAll := false
	if raw.Relations != nil {
		if raw.Relations.ReplyTo != nil {
			replyTo = raw.Relations.ReplyTo.MessageID
			if !divaMessageIDPattern.MatchString(replyTo) {
				return &raw, nil, invalidRequest("relations.reply_to.message_id must be a LINE message ID")
			}
		}
		mentions = raw.Relations.Mentions
		mentionAll = raw.Relations.MentionAll
	}
	mentionMeta, err := buildDIVAMentionMetadata(content.Text, mentions, mentionAll)
	if err != nil {
		return &raw, nil, invalidRequest("%v", err)
	}

	replyFallback := divaReplyFallbackFail
	wait := divaControlDefaultWait
	if raw.Options != nil {
		switch raw.Options.ReplyFallback {
		case "", divaReplyFallbackFail:
		case divaReplyFallbackDrop:
			replyFallback = divaReplyFallbackDrop
		default:
			return &raw, nil, invalidRequest("options.reply_fallback must be %q or %q", divaReplyFallbackFail, divaReplyFallbackDrop)
		}
		if raw.Options.TimeoutMS != nil {
			wait = time.Duration(*raw.Options.TimeoutMS) * time.Millisecond
			wait = max(divaControlMinWait, min(wait, divaControlMaxWait))
		}
	}

	job := &divaSendJob{
		requestID:  raw.RequestID,
		accountMID: accountMID,
		chatID:     raw.Target.ChatID,
		wait:       wait,
		req: &lineOutboundRequest{
			ChatMID:       raw.Target.ChatID,
			ContentType:   ContentText,
			Text:          content.Text,
			MentionMeta:   mentionMeta,
			ReplyToID:     replyTo,
			ReplyFallback: replyFallback == divaReplyFallbackDrop,
		},
	}
	// The fingerprint covers what is sent and where, not the wait budget, so a
	// retry may change timeout_ms.
	fp, _ := json.Marshal(struct {
		AccountMID, ChatID, MessageType, Text, ReplyTo, ReplyFallback string
		Mentions                                                      []divaSendMention
		MentionAll                                                    bool
	}{accountMID, raw.Target.ChatID, raw.MessageType, content.Text, replyTo, replyFallback, mentions, mentionAll})
	job.fingerprint = sha256.Sum256(fp)
	return &raw, job, nil
}

// buildDIVAMentionMetadata finds "@<name>" for every mention and "@All" for
// mention_all in text, and builds LINE's MENTION metadata with UTF-16 offsets.
// A mention that cannot be placed is an error: it is never silently sent as
// plain text.
func buildDIVAMentionMetadata(text string, mentions []divaSendMention, mentionAll bool) (map[string]string, error) {
	if len(mentions) == 0 && !mentionAll {
		return nil, nil
	}
	type span struct {
		start, end int
		entry      mentionEntry
	}
	var claimed []span
	overlaps := func(start, end int) bool {
		for _, s := range claimed {
			if start < s.end && s.start < end {
				return true
			}
		}
		return false
	}
	claim := func(start, end int, entry mentionEntry) error {
		s16, ok1 := byteIndexToUTF16Offset(text, start)
		e16, ok2 := byteIndexToUTF16Offset(text, end)
		if !ok1 || !ok2 {
			return fmt.Errorf("cannot compute mention offsets")
		}
		entry.S, entry.E = strconv.Itoa(s16), strconv.Itoa(e16)
		claimed = append(claimed, span{start: start, end: end, entry: entry})
		return nil
	}

	// Place longer names first so "@阿明哥" is not taken by a mention of "阿明".
	order := make([]int, len(mentions))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return len(mentions[order[a]].Name) > len(mentions[order[b]].Name) })

	for _, i := range order {
		m := mentions[i]
		if !divaUserMIDPattern.MatchString(m.MID) {
			return nil, fmt.Errorf("relations.mentions[%d].mid must be a LINE user MID", i)
		}
		if strings.TrimSpace(m.Name) == "" {
			return nil, fmt.Errorf("relations.mentions[%d].name must not be empty", i)
		}
		needle := "@" + m.Name
		placed := false
		for from := 0; from <= len(text); {
			idx := strings.Index(text[from:], needle)
			if idx < 0 {
				break
			}
			start := from + idx
			end := start + len(needle)
			if !overlaps(start, end) {
				if err := claim(start, end, mentionEntry{M: m.MID}); err != nil {
					return nil, err
				}
				placed = true
				break
			}
			from = start + 1
		}
		if !placed {
			return nil, fmt.Errorf("relations.mentions[%d]: %q not found in content.text", i, needle)
		}
	}

	if mentionAll {
		placed := false
		for i := 0; i+4 <= len(text); i++ {
			if text[i] == '@' && strings.EqualFold(text[i:i+4], "@all") && !overlaps(i, i+4) {
				if err := claim(i, i+4, mentionEntry{A: "1"}); err != nil {
					return nil, err
				}
				placed = true
				break
			}
		}
		if !placed {
			return nil, fmt.Errorf("relations.mention_all requires \"@All\" in content.text")
		}
	}

	sort.Slice(claimed, func(a, b int) bool { return claimed[a].start < claimed[b].start })
	entries := make([]mentionEntry, len(claimed))
	for i, s := range claimed {
		entries[i] = s.entry
	}
	data, err := json.Marshal(map[string]any{"MENTIONEES": entries})
	if err != nil {
		return nil, err
	}
	return map[string]string{"MENTION": string(data)}, nil
}

// ---------------------------------------------------------------------------
// login selection
// ---------------------------------------------------------------------------

// selectDIVALogin picks the LINE login to send from. It never guesses which
// account is Server A: with several connected logins, account_mid is required.
func selectDIVALogin(logins []*bridgev2.UserLogin, accountMID string) (*LineClient, *divaRequestError) {
	var all, active []*LineClient
	for _, login := range logins {
		if login == nil {
			continue
		}
		client, ok := login.Client.(*LineClient)
		if !ok || client == nil {
			continue
		}
		all = append(all, client)
		if client.IsLoggedIn() {
			active = append(active, client)
		}
	}
	unavailable := func(code outboundErrorCode, detail string) *divaRequestError {
		return &divaRequestError{status: http.StatusServiceUnavailable, code: code, detail: detail}
	}
	if accountMID != "" {
		for _, client := range all {
			if client.Mid == accountMID || (client.UserLogin != nil && string(client.UserLogin.ID) == accountMID) {
				if !client.IsLoggedIn() {
					return nil, unavailable(outboundLineSessionInvalid, "the LINE login for account_mid is not connected")
				}
				return client, nil
			}
		}
		return nil, unavailable(outboundNoActiveLogin, "no LINE login for account_mid")
	}
	switch {
	case len(active) == 1:
		return active[0], nil
	case len(active) > 1:
		return nil, unavailable(outboundNoActiveLogin, "several LINE logins are connected; target.account_mid is required")
	case len(all) == 1:
		return nil, unavailable(outboundLineSessionInvalid, "the LINE login is not connected")
	default:
		return nil, unavailable(outboundNoActiveLogin, "no connected LINE login")
	}
}

// ---------------------------------------------------------------------------
// server
// ---------------------------------------------------------------------------

type divaSendFunc func(ctx context.Context, lc *LineClient, req *lineOutboundRequest) (*lineOutboundResult, error)

type divaControlConfig struct {
	token         string
	logins        func() []*bridgev2.UserLogin
	send          divaSendFunc
	log           zerolog.Logger
	maxConcurrent int
	maxPending    int
	resultTTL     time.Duration
}

// divaRequestEntry is one request_id: in flight until done is closed, then
// completed with result until expiresAt.
type divaRequestEntry struct {
	fingerprint [32]byte
	done        chan struct{}
	result      outboundResult
	expiresAt   time.Time
}

type divaControlServer struct {
	cfg       divaControlConfig
	tokenHash [32]byte
	baseCtx   context.Context
	cancel    context.CancelFunc
	sem       chan struct{}

	mu       sync.Mutex
	requests map[string]*divaRequestEntry
	// lanes holds, per account/chat, the done channel of the last queued
	// send. A lane is removed when its last send finishes.
	lanes   map[string]chan struct{}
	pending int

	wg         sync.WaitGroup
	httpServer *http.Server
	addr       string
}

func newDIVAControlServer(cfg divaControlConfig) *divaControlServer {
	if cfg.maxConcurrent <= 0 {
		cfg.maxConcurrent = divaControlMaxConcurrent
	}
	if cfg.maxPending <= 0 {
		cfg.maxPending = divaControlMaxPending
	}
	if cfg.resultTTL <= 0 {
		cfg.resultTTL = divaControlResultTTL
	}
	if cfg.send == nil {
		cfg.send = func(ctx context.Context, lc *LineClient, req *lineOutboundRequest) (*lineOutboundResult, error) {
			return lc.sendLineOutbound(ctx, req)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &divaControlServer{
		cfg:       cfg,
		tokenHash: sha256.Sum256([]byte(cfg.token)),
		baseCtx:   ctx,
		cancel:    cancel,
		sem:       make(chan struct{}, cfg.maxConcurrent),
		requests:  map[string]*divaRequestEntry{},
		lanes:     map[string]chan struct{}{},
	}
}

func (s *divaControlServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/diva/v1/health", s.handleHealth)
	mux.HandleFunc("/diva/v1/send", s.handleSend)
	return mux
}

// startDIVAControl starts the endpoint if DIVA_CONTROL_TOKEN is set. It never
// fails the bridge: problems are logged and the endpoint stays off.
func startDIVAControl(br *bridgev2.Bridge) *divaControlServer {
	log := br.Log.With().Str("component", "diva_control").Logger()
	token := os.Getenv("DIVA_CONTROL_TOKEN")
	if token == "" {
		log.Info().Msg("DIVA control endpoint disabled: DIVA_CONTROL_TOKEN is not set")
		return nil
	}
	if len(token) < divaControlMinTokenLength {
		log.Error().Int("min_length", divaControlMinTokenLength).Msg("DIVA control endpoint disabled: DIVA_CONTROL_TOKEN is too short")
		return nil
	}
	addr := strings.TrimSpace(os.Getenv("DIVA_CONTROL_LISTEN"))
	if addr == "" {
		addr = divaControlDefaultListen
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error().Err(err).Str("listen", addr).Msg("DIVA control endpoint disabled: cannot listen")
		return nil
	}
	s := newDIVAControlServer(divaControlConfig{
		token:  token,
		logins: br.GetAllCachedUserLogins,
		log:    log,
	})
	s.addr = listener.Addr().String()
	s.httpServer = &http.Server{
		Handler:           s.handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      divaControlMaxWait + 10*time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	go func() {
		if err := s.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("DIVA control endpoint stopped")
		}
	}()
	log.Info().Str("listen", s.addr).Msg("DIVA control endpoint listening")
	return s
}

// stop closes the listener and cancels sends that have not started yet.
func (s *divaControlServer) stop(timeout time.Duration) {
	if s == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if s.httpServer != nil {
		_ = s.httpServer.Shutdown(ctx)
	}
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (s *divaControlServer) authorized(r *http.Request) bool {
	header := r.Header.Get("Authorization")
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok {
		return false
	}
	got := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(got[:], s.tokenHash[:]) == 1
}

func (s *divaControlServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeDIVAError(w, http.StatusMethodNotAllowed, "", outboundInvalidRequest, "method not allowed")
		return
	}
	writeDIVAJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *divaControlServer) handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeDIVAError(w, http.StatusMethodNotAllowed, "", outboundInvalidRequest, "method not allowed")
		return
	}
	if !s.authorized(r) {
		writeDIVAError(w, http.StatusUnauthorized, "", outboundUnauthorized, "missing or invalid bearer token")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, divaControlMaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeDIVAError(w, http.StatusRequestEntityTooLarge, "", outboundInvalidRequest, "request body too large")
			return
		}
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "cannot read request body")
		return
	}

	raw, job, reqErr := parseDIVASendRequest(body)
	requestID := ""
	if raw != nil {
		requestID = raw.RequestID
	}
	if reqErr != nil {
		writeDIVAError(w, reqErr.status, requestID, reqErr.code, reqErr.detail)
		return
	}

	result, status := s.submit(r.Context(), job)
	writeDIVAJSON(w, status, result)
}

// submit runs the dedupe / queue / wait logic for one validated request.
func (s *divaControlServer) submit(reqCtx context.Context, job *divaSendJob) (outboundResult, int) {
	s.mu.Lock()
	s.sweepLocked(time.Now())
	if entry, ok := s.requests[job.requestID]; ok {
		s.mu.Unlock()
		if entry.fingerprint != job.fingerprint {
			return divaErrorResult(job.requestID, outboundInvalidRequest, false, "request_id was already used for a different request"), http.StatusConflict
		}
		s.cfg.log.Info().Str("request_id", job.requestID).Msg("DIVA send deduplicated")
		return s.wait(reqCtx, job, entry, true), http.StatusOK
	}
	s.mu.Unlock()

	// Login selection happens outside the lock and is not cached: a missing
	// login is retryable with the same request_id.
	var logins []*bridgev2.UserLogin
	if s.cfg.logins != nil {
		logins = s.cfg.logins()
	}
	lc, selErr := selectDIVALogin(logins, job.accountMID)
	if selErr != nil {
		return divaErrorResult(job.requestID, selErr.code, false, selErr.detail), selErr.status
	}

	s.mu.Lock()
	// Another call may have registered the same request_id meanwhile.
	if entry, ok := s.requests[job.requestID]; ok {
		s.mu.Unlock()
		if entry.fingerprint != job.fingerprint {
			return divaErrorResult(job.requestID, outboundInvalidRequest, false, "request_id was already used for a different request"), http.StatusConflict
		}
		return s.wait(reqCtx, job, entry, true), http.StatusOK
	}
	if s.pending >= s.cfg.maxPending || len(s.requests) >= divaControlMaxEntries {
		s.mu.Unlock()
		return divaErrorResult(job.requestID, outboundInternal, true, "too many pending DIVA sends; nothing was sent"), http.StatusServiceUnavailable
	}
	entry := &divaRequestEntry{fingerprint: job.fingerprint, done: make(chan struct{})}
	s.requests[job.requestID] = entry
	laneKey := lc.midOrFallback() + "/" + job.chatID
	prev := s.lanes[laneKey]
	mine := make(chan struct{})
	s.lanes[laneKey] = mine
	s.pending++
	s.wg.Add(1)
	s.mu.Unlock()

	s.cfg.log.Info().
		Str("request_id", job.requestID).
		Str("chat_id", job.chatID).
		Bool("reply", job.req.ReplyToID != "").
		Bool("mention", job.req.MentionMeta != nil).
		Msg("DIVA send accepted")
	go s.run(job, entry, lc, laneKey, prev, mine)
	return s.wait(reqCtx, job, entry, false), http.StatusOK
}

// run performs one send in the background, after the previous send to the
// same chat, and stores the result under the request_id.
func (s *divaControlServer) run(job *divaSendJob, entry *divaRequestEntry, lc *LineClient, laneKey string, prev, mine chan struct{}) {
	var result outboundResult
	defer func() {
		if recovered := recover(); recovered != nil {
			s.cfg.log.Error().Any("panic", recovered).Str("request_id", job.requestID).Msg("DIVA send panicked")
			result = divaErrorResult(job.requestID, outboundInternal, false, "internal error during send")
			result.Error.Delivery = deliveryUnknown
		}
		s.complete(job, entry, result)
		close(mine)
		s.mu.Lock()
		if s.lanes[laneKey] == mine {
			delete(s.lanes, laneKey)
		}
		s.pending--
		s.mu.Unlock()
		s.wg.Done()
	}()

	notStarted := func() outboundResult {
		return divaErrorResult(job.requestID, outboundInternal, true, "bridge is shutting down; nothing was sent")
	}
	if prev != nil {
		select {
		case <-prev:
		case <-s.baseCtx.Done():
			result = notStarted()
			return
		}
	}
	select {
	case s.sem <- struct{}{}:
	case <-s.baseCtx.Done():
		result = notStarted()
		return
	}
	defer func() { <-s.sem }()

	ctx, cancel := context.WithTimeout(s.baseCtx, divaControlSendBudget)
	defer cancel()
	sent, err := s.cfg.send(ctx, lc, job.req)
	result = lc.buildOutboundResult(job.chatID, sent, err)
	result.RequestID = job.requestID
}

func (s *divaControlServer) complete(job *divaSendJob, entry *divaRequestEntry, result outboundResult) {
	logEvt := s.cfg.log.Info().Str("request_id", job.requestID).Bool("ok", result.OK)
	if result.Message != nil {
		logEvt = logEvt.Str("message_id", result.Message.ID)
	}
	if result.Error != nil {
		logEvt = logEvt.Str("code", string(result.Error.Code)).Str("delivery", string(result.Error.Delivery))
	}
	logEvt.Msg("DIVA send finished")

	s.mu.Lock()
	entry.result = result
	entry.expiresAt = time.Now().Add(s.cfg.resultTTL)
	// Nothing reached LINE: forget the request_id so the caller can retry it.
	if !result.OK && result.Error != nil && result.Error.Delivery == deliveryNotSent && s.requests[job.requestID] == entry {
		delete(s.requests, job.requestID)
	}
	s.mu.Unlock()
	close(entry.done)
}

// wait returns the entry's result, or TIMEOUT / unknown when it does not
// finish within the request's wait budget. The send is never cancelled.
func (s *divaControlServer) wait(reqCtx context.Context, job *divaSendJob, entry *divaRequestEntry, deduplicated bool) outboundResult {
	timer := time.NewTimer(job.wait)
	defer timer.Stop()
	select {
	case <-entry.done:
		s.mu.Lock()
		result := entry.result
		s.mu.Unlock()
		result.RequestID = job.requestID
		result.Deduplicated = deduplicated
		if result.Fallbacks == nil {
			result.Fallbacks = []outboundFallback{}
		}
		return result
	case <-timer.C:
	case <-reqCtx.Done():
	}
	result := divaErrorResult(job.requestID, outboundTimeout, true,
		"no result within timeout_ms; the send continues in the background. Retry with the same request_id to get the result")
	result.Error.Delivery = deliveryUnknown
	result.Deduplicated = deduplicated
	return result
}

func (s *divaControlServer) sweepLocked(now time.Time) {
	for id, entry := range s.requests {
		if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			delete(s.requests, id)
		}
	}
}

// ---------------------------------------------------------------------------
// responses
// ---------------------------------------------------------------------------

func divaErrorResult(requestID string, code outboundErrorCode, retryable bool, detail string) outboundResult {
	return outboundResult{
		RequestID: requestID,
		Fallbacks: []outboundFallback{},
		Error: &outboundResultError{
			Code:      code,
			Retryable: retryable,
			Delivery:  deliveryNotSent,
			Detail:    detail,
		},
	}
}

func writeDIVAError(w http.ResponseWriter, status int, requestID string, code outboundErrorCode, detail string) {
	writeDIVAJSON(w, status, divaErrorResult(requestID, code, false, detail))
}

func writeDIVAJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
