package connector

// OWNER P2 profile writes on the private Docker control endpoint.
//
// Only four fixed operations exist: the logged-in Server A account's own
// display name, status message, profile picture and profile cover. Callers cannot choose an
// attribute, a target MID, a URL or an RPC method. Every operation is off
// unless DIVA_OWNER_PROFILE_OPERATIONS lists it. A request_id is executed at
// most once; a second request with the same request_id and the same payload
// fingerprint returns the stored result, a different payload is refused. One
// write per account is in flight at a time. Success means LINE read back the
// new value; a timeout or unclear response is reported as unknown and is never
// retried here. Logs never contain values, image bytes, tokens or MIDs.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

const (
	divaOwnerProfileOperationsEnv = "DIVA_OWNER_PROFILE_OPERATIONS"

	divaProfileOperationName   = "profile_name"
	divaProfileOperationStatus = "profile_status"
	divaProfileOperationPhoto  = "profile_photo"
	divaProfileOperationCover  = "profile_cover"

	divaProfileStatePending     = "pending"
	divaProfileStateConfirmed   = "confirmed"
	divaProfileStateUnchanged   = "unchanged"
	divaProfileStateUnconfirmed = "unconfirmed"
	divaProfileStateRejected    = "rejected"
	divaProfileStateNotSent     = "not_sent"
	divaProfileStateUnknown     = "unknown"

	divaProfileWriteNotAttempted = "not_attempted"
	divaProfileWriteAccepted     = "accepted"
	divaProfileWriteRejected     = "rejected"
	divaProfileWriteUnknown      = "unknown"

	divaProfileReadbackNotAttempted = "not_attempted"
	divaProfileReadbackMatch        = "match"
	divaProfileReadbackMismatch     = "mismatch"
	divaProfileReadbackUnavailable  = "unavailable"

	divaProfileContractVersion = 1
	divaProfileMaxJSONBytes    = 16 << 10
	divaProfileMaxMetaBytes    = 4 << 10
	divaProfileResultTTL       = 30 * time.Minute
	divaProfileMaxEntries      = 1000
	divaProfileJobBudget       = 2 * time.Minute
	divaProfileWriteTimeout    = 30 * time.Second
	divaProfileReadTimeout     = 10 * time.Second
	divaProfileReadbackTries   = 3
	divaProfileDefaultWait     = 25 * time.Second
	divaProfileMediaReadBudget = 2 * time.Minute
)

var divaProfileReadbackDelay = 1500 * time.Millisecond

var divaProfileOperationOrder = []string{divaProfileOperationName, divaProfileOperationStatus, divaProfileOperationPhoto, divaProfileOperationCover}

// parseDIVAProfileOperations reads the comma separated allow list. Any unknown
// token disables everything (fail closed) instead of guessing.
func parseDIVAProfileOperations(raw string) (map[string]bool, error) {
	enabled := map[string]bool{}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return enabled, nil
	}
	for _, token := range strings.Split(raw, ",") {
		token = strings.TrimSpace(token)
		switch token {
		case divaProfileOperationName, divaProfileOperationStatus, divaProfileOperationPhoto, divaProfileOperationCover:
			enabled[token] = true
		default:
			return map[string]bool{}, errors.New("unknown profile operation in allow list")
		}
	}
	return enabled, nil
}

// divaProfileAPI is the LINE side of a profile write; tests replace it.
type divaProfileAPI interface {
	readSelf(ctx context.Context, lc *LineClient) (*line.Profile, error)
	updateAttribute(ctx context.Context, lc *LineClient, attribute int, value string) error
	uploadImage(ctx context.Context, lc *LineClient, selfMID string, jpeg []byte) (*line.ProfileImageUploadResult, error)
}

// lineProfileAPI performs single attempts with the current access token. It
// deliberately bypasses token recovery/re-login: an auth failure is reported.
type lineProfileAPI struct{}

// The current account has confirmed the Chrome getCover response through
// authenticated self-only production GET (AI_EXC #140); the background write
// candidate is now gated by BOTH existing Worker and Bridge operation allow
// lists, and can only be entered by a fresh OWNER command plus picture.
// Association still needs a one-shot OWNER E2E verification. Failure or
// unknown write/readback never triggers an automatic retry.
func (lineProfileAPI) protocolBlock(operation string) string {
	return ""
}

type divaProfileCoverAPI interface {
	readCover(context.Context, *LineClient, string) (string, error)
	uploadCover(context.Context, *LineClient, string, []byte) (*line.ProfileImageUploadResult, error)
}

func (lineProfileAPI) readCover(ctx context.Context, lc *LineClient, mid string) (string, error) {
	return lc.newClient().ReadProfileCoverContext(ctx, mid)
}
func (lineProfileAPI) uploadCover(ctx context.Context, lc *LineClient, mid string, jpeg []byte) (*line.ProfileImageUploadResult, error) {
	return lc.newClient().UploadProfileCoverContext(ctx, mid, jpeg)
}
func (s *divaControlServer) profileProtocolBlock(operation string) string {
	if blocker, ok := s.cfg.profileAPI.(interface{ protocolBlock(string) string }); ok {
		return blocker.protocolBlock(operation)
	}
	if operation == divaProfileOperationCover {
		if _, ok := s.cfg.profileAPI.(divaProfileCoverAPI); !ok {
			return "PROFILE_COVER_UNSUPPORTED"
		}
	}
	return ""
}

func (lineProfileAPI) readSelf(ctx context.Context, lc *LineClient) (*line.Profile, error) {
	return lc.newClient().GetProfileContext(ctx)
}

func (lineProfileAPI) updateAttribute(ctx context.Context, lc *LineClient, attribute int, value string) error {
	return lc.newClient().UpdateProfileAttributeContext(ctx, int64(lc.nextUntrackedReqSeq()), attribute, value)
}

func (lineProfileAPI) uploadImage(ctx context.Context, lc *LineClient, selfMID string, jpeg []byte) (*line.ProfileImageUploadResult, error) {
	return lc.newClient().UploadProfileImageContext(ctx, selfMID, jpeg)
}

type divaProfileRequest struct {
	Version    *int   `json:"version"`
	RequestID  string `json:"request_id"`
	AccountMID string `json:"account_mid"`
	Operation  string `json:"operation"`
	Value      string `json:"value"`
}

type divaProfilePhotoMetadata struct {
	Version    *int   `json:"version"`
	RequestID  string `json:"request_id"`
	AccountMID string `json:"account_mid"`
	Operation  string `json:"operation"`
	MimeType   string `json:"mime_type"`
}

type divaProfileJob struct {
	requestID   string
	accountMID  string
	operation   string
	value       string
	image       []byte
	imageInfo   line.ProfileImageInfo
	fingerprint [32]byte
}

type divaProfileResult struct {
	Version      int    `json:"version"`
	RequestID    string `json:"request_id"`
	Operation    string `json:"operation"`
	State        string `json:"state"`
	Write        string `json:"write"`
	Readback     string `json:"readback"`
	Code         string `json:"code,omitempty"`
	Deduplicated bool   `json:"deduplicated"`
	// httpStatus is LINE's HTTP status for a failed write; log only.
	httpStatus int
}

type divaProfileEntry struct {
	accountMID  string
	fingerprint [32]byte
	done        chan struct{}
	result      divaProfileResult
	expiresAt   time.Time
}

func divaProfileFingerprint(account, operation string, payload []byte) [32]byte {
	h := sha256.New()
	for _, part := range [][]byte{[]byte(account), []byte(operation), payload} {
		_, _ = fmt.Fprintf(h, "%d:", len(part))
		_, _ = h.Write(part)
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

func divaProfileRejection(requestID, operation, code string) divaProfileResult {
	return divaProfileResult{
		Version: divaProfileContractVersion, RequestID: requestID, Operation: operation,
		State: divaProfileStateNotSent, Write: divaProfileWriteNotAttempted,
		Readback: divaProfileReadbackNotAttempted, Code: code,
	}
}

func (s *divaControlServer) profileEnabled(operation string) bool {
	return s.cfg.profileOperations[operation]
}

func (s *divaControlServer) enabledProfileOperations() []string {
	enabled := []string{}
	for _, operation := range divaProfileOperationOrder {
		if s.profileEnabled(operation) && s.profileProtocolBlock(operation) == "" {
			enabled = append(enabled, operation)
		}
	}
	return enabled
}

// profileLogin resolves the explicit account. Unlike send, account_mid is
// always mandatory: a profile write never falls back to "the only login".
func (s *divaControlServer) profileLogin(account string) (*LineClient, *divaRequestError) {
	if !divaUserMIDPattern.MatchString(account) {
		return nil, invalidRequest("one explicit account_mid is required")
	}
	var logins []*bridgev2.UserLogin
	if s.cfg.logins != nil {
		logins = s.cfg.logins()
	}
	lc, selectionError := selectDIVALogin(logins, account)
	if selectionError != nil {
		return nil, selectionError
	}
	if lc.Mid != "" && lc.Mid != account {
		return nil, &divaRequestError{status: http.StatusServiceUnavailable, code: outboundNoActiveLogin, detail: "account_mid does not match the login"}
	}
	return lc, nil
}

func (s *divaControlServer) profileAccountQuery(w http.ResponseWriter, r *http.Request, extra ...string) (url.Values, string, bool) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeDIVAError(w, http.StatusMethodNotAllowed, "", outboundInvalidRequest, "method not allowed")
		return nil, "", false
	}
	if !s.authorized(r) {
		writeDIVAError(w, http.StatusUnauthorized, "", outboundUnauthorized, "missing or invalid bearer token")
		return nil, "", false
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	allowed := map[string]bool{"account_mid": true}
	for _, name := range extra {
		allowed[name] = true
	}
	valid := err == nil && len(query) == len(allowed)
	for name := range allowed {
		valid = valid && len(query[name]) == 1
	}
	account := query.Get("account_mid")
	if !valid || !divaUserMIDPattern.MatchString(account) {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "invalid query")
		return nil, "", false
	}
	return query, account, true
}

// handleOwnerProfileCapabilities reports which operations this Bridge allows
// and whether the explicit account is connected. It never calls LINE.
func (s *divaControlServer) handleOwnerProfileCapabilities(w http.ResponseWriter, r *http.Request) {
	_, account, ok := s.profileAccountQuery(w, r)
	if !ok {
		return
	}
	_, selectionError := s.profileLogin(account)
	blocked := map[string]string{}
	for _, op := range divaProfileOperationOrder {
		if code := s.profileProtocolBlock(op); code != "" {
			blocked[op] = code
		}
	}
	writeDIVAJSON(w, http.StatusOK, map[string]any{
		"version": divaProfileContractVersion, "ok": true,
		"account_connected":       selectionError == nil,
		"enabled_operations":      s.enabledProfileOperations(),
		"blocked_operations":      blocked,
		"image_protocol":          "chrome-native-gateway-candidate",
		"image_protocol_verified": false,
	})
}

// CoverInspector does not expose object IDs and cannot write a profile image.
type divaCoverInspector interface {
	inspectCover(context.Context, *LineClient, string) (line.ProfileCoverInspection, error)
}

func (lineProfileAPI) inspectCover(ctx context.Context, lc *LineClient, selfMID string) (line.ProfileCoverInspection, error) {
	return lc.newClient().InspectProfileCoverContext(ctx, selfMID)
}

// handleOwnerProfileCoverInspect is a strictly authenticated, self-only,
// read-only probe. It is independent of profile_cover write enablement.
// Never returns raw LINE JSON, MID, token, cover ID, or images.
func (s *divaControlServer) handleOwnerProfileCoverInspect(w http.ResponseWriter, r *http.Request) {
	_, account, ok := s.profileAccountQuery(w, r)
	if !ok {
		return
	}
	lc, selectErr := s.profileLogin(account)
	if selectErr != nil {
		writeDIVAError(w, selectErr.status, "", selectErr.code, selectErr.detail)
		return
	}
	inspector, supported := s.cfg.profileAPI.(divaCoverInspector)
	if !supported {
		writeDIVAError(w, http.StatusServiceUnavailable, "", "PROFILE_COVER_INSPECT_UNSUPPORTED", "cover inspection unavailable")
		return
	}
	select {
	case s.ownerQuerySem <- struct{}{}:
		defer func() { <-s.ownerQuerySem }()
	default:
		writeDIVAError(w, http.StatusServiceUnavailable, "", outboundInternal, "owner query busy")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), divaProfileReadTimeout)
	defer cancel()
	info, err := inspector.inspectCover(ctx, lc, account)
	code := ""
	if err != nil {
		_, code = line.ProfileWriteOutcomeOf(err)
		if code == "" {
			code = "PROFILE_COVER_INSPECT_UNAVAILABLE"
		}
	}
	writeDIVAJSON(w, http.StatusOK, map[string]any{
		"version":   divaProfileContractVersion,
		"ok":        err == nil,
		"read_only": true,
		"schema":    info,
		"code":      code,
	})
}

// handleOwnerProfileSelf is the read-only check used to settle an unknown
// result after a restart. It returns hashes, never the profile values.
func (s *divaControlServer) handleOwnerProfileSelf(w http.ResponseWriter, r *http.Request) {
	_, account, ok := s.profileAccountQuery(w, r)
	if !ok {
		return
	}
	if len(s.enabledProfileOperations()) == 0 {
		writeDIVAError(w, http.StatusForbidden, "", outboundInvalidRequest, "profile operations are disabled")
		return
	}
	lc, selectionError := s.profileLogin(account)
	if selectionError != nil {
		writeDIVAError(w, selectionError.status, "", selectionError.code, selectionError.detail)
		return
	}
	select {
	case s.ownerQuerySem <- struct{}{}:
		defer func() { <-s.ownerQuerySem }()
	default:
		writeDIVAError(w, http.StatusServiceUnavailable, "", outboundInternal, "owner query busy")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), divaProfileReadTimeout)
	defer cancel()
	profile, err := s.cfg.profileAPI.readSelf(ctx, lc)
	if err != nil || profile == nil || profile.Mid != account {
		writeDIVAError(w, http.StatusServiceUnavailable, "", outboundInternal, "profile read back unavailable")
		return
	}
	digest := func(value string) string {
		sum := sha256.Sum256([]byte(value))
		return hex.EncodeToString(sum[:])
	}
	writeDIVAJSON(w, http.StatusOK, map[string]any{
		"version": divaProfileContractVersion, "ok": true,
		"display_name_sha256":   digest(profile.DisplayName),
		"status_message_sha256": digest(profile.StatusMessage),
		"picture_sha256":        digest(profile.PictureStatus + "\x00" + profile.PicturePath),
	})
}

func (s *divaControlServer) handleOwnerProfileUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeDIVAError(w, http.StatusMethodNotAllowed, "", outboundInvalidRequest, "method not allowed")
		return
	}
	if !s.authorized(r) {
		writeDIVAError(w, http.StatusUnauthorized, "", outboundUnauthorized, "missing or invalid bearer token")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, divaProfileMaxJSONBytes))
	if err != nil {
		writeDIVAError(w, http.StatusRequestEntityTooLarge, "", outboundInvalidRequest, "request body too large or unreadable")
		return
	}
	var raw divaProfileRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil || dec.More() {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "invalid JSON")
		return
	}
	attribute := 0
	switch raw.Operation {
	case divaProfileOperationName:
		attribute = line.ProfileAttributeDisplayName
	case divaProfileOperationStatus:
		attribute = line.ProfileAttributeStatusMessage
	default:
		writeDIVAError(w, http.StatusBadRequest, raw.RequestID, outboundInvalidRequest, "operation must be profile_name or profile_status")
		return
	}
	if raw.Version == nil || *raw.Version != divaProfileContractVersion || !divaUUIDPattern.MatchString(raw.RequestID) ||
		!divaUserMIDPattern.MatchString(raw.AccountMID) {
		writeDIVAError(w, http.StatusBadRequest, raw.RequestID, outboundInvalidRequest, "version, request_id and account_mid are required")
		return
	}
	if line.ValidateProfileText(attribute, raw.Value) != nil {
		writeDIVAJSON(w, http.StatusBadRequest, divaProfileRejection(raw.RequestID, raw.Operation, "PROFILE_VALUE_INVALID"))
		return
	}
	job := &divaProfileJob{
		requestID: raw.RequestID, accountMID: raw.AccountMID, operation: raw.Operation, value: raw.Value,
		fingerprint: divaProfileFingerprint(raw.AccountMID, raw.Operation, []byte(raw.Value)),
	}
	result, status := s.submitProfile(r.Context(), job)
	writeDIVAJSON(w, status, result)
}

func (s *divaControlServer) handleOwnerProfilePhoto(w http.ResponseWriter, r *http.Request) {
	s.handleOwnerProfileImage(w, r, divaProfileOperationPhoto)
}
func (s *divaControlServer) handleOwnerProfileCover(w http.ResponseWriter, r *http.Request) {
	s.handleOwnerProfileImage(w, r, divaProfileOperationCover)
}
func (s *divaControlServer) handleOwnerProfileImage(w http.ResponseWriter, r *http.Request, operation string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeDIVAError(w, http.StatusMethodNotAllowed, "", outboundInvalidRequest, "method not allowed")
		return
	}
	if !s.authorized(r) {
		writeDIVAError(w, http.StatusUnauthorized, "", outboundUnauthorized, "missing or invalid bearer token")
		return
	}
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Now().Add(divaProfileMediaReadBudget))
	defer func() { _ = rc.SetReadDeadline(time.Time{}) }()
	r.Body = http.MaxBytesReader(w, r.Body, line.ProfileImageMaxInputBytes+divaProfileMaxJSONBytes)
	mr, err := r.MultipartReader()
	if err != nil {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "content type must be multipart/form-data")
		return
	}
	metaPart, err := mr.NextPart()
	if err != nil || metaPart.FormName() != "metadata" {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "first multipart part must be metadata")
		return
	}
	metaBytes, err := io.ReadAll(io.LimitReader(metaPart, divaProfileMaxMetaBytes+1))
	_ = metaPart.Close()
	if err != nil || len(metaBytes) > divaProfileMaxMetaBytes {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "metadata part is too large or unreadable")
		return
	}
	var raw divaProfilePhotoMetadata
	dec := json.NewDecoder(bytes.NewReader(metaBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil || dec.More() {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "invalid metadata JSON")
		return
	}
	if raw.Version == nil || *raw.Version != divaProfileContractVersion || !divaUUIDPattern.MatchString(raw.RequestID) ||
		!divaUserMIDPattern.MatchString(raw.AccountMID) || raw.Operation != operation {
		writeDIVAError(w, http.StatusBadRequest, raw.RequestID, outboundInvalidRequest, "version, request_id, account_mid and operation are required")
		return
	}
	mediaPart, err := mr.NextPart()
	if err != nil || mediaPart.FormName() != "media" {
		writeDIVAError(w, http.StatusBadRequest, raw.RequestID, outboundInvalidRequest, "second multipart part must be media")
		return
	}
	data, err := io.ReadAll(io.LimitReader(mediaPart, line.ProfileImageMaxInputBytes+1))
	_ = mediaPart.Close()
	if err != nil || len(data) > line.ProfileImageMaxInputBytes {
		writeDIVAJSON(w, http.StatusRequestEntityTooLarge, divaProfileRejection(raw.RequestID, raw.Operation, "PROFILE_IMAGE_INVALID"))
		return
	}
	if extra, nextErr := mr.NextPart(); nextErr == nil {
		_ = extra.Close()
		writeDIVAError(w, http.StatusBadRequest, raw.RequestID, outboundInvalidRequest, "unexpected extra multipart part")
		return
	} else if !errors.Is(nextErr, io.EOF) {
		writeDIVAError(w, http.StatusBadRequest, raw.RequestID, outboundInvalidRequest, "invalid multipart body")
		return
	}
	fingerprint := divaProfileFingerprint(raw.AccountMID, raw.Operation, data)
	normalized, info, normalizeErr := line.NormalizeProfileImage(data, raw.MimeType)
	data = nil
	if normalizeErr != nil {
		writeDIVAJSON(w, http.StatusBadRequest, divaProfileRejection(raw.RequestID, raw.Operation, "PROFILE_IMAGE_INVALID"))
		return
	}
	job := &divaProfileJob{
		requestID: raw.RequestID, accountMID: raw.AccountMID, operation: raw.Operation,
		image: normalized, imageInfo: info, fingerprint: fingerprint,
	}
	_ = rc.SetWriteDeadline(time.Now().Add(divaProfileDefaultWait + 10*time.Second))
	result, status := s.submitProfile(r.Context(), job)
	writeDIVAJSON(w, status, result)
}

// handleOwnerProfileResult returns the stored result of one request_id for the
// same account. It never starts or repeats a write.
func (s *divaControlServer) handleOwnerProfileResult(w http.ResponseWriter, r *http.Request) {
	query, account, ok := s.profileAccountQuery(w, r, "request_id")
	if !ok {
		return
	}
	requestID := query.Get("request_id")
	if !divaUUIDPattern.MatchString(requestID) {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "request_id must be a UUID")
		return
	}
	s.mu.Lock()
	s.sweepProfileLocked(time.Now())
	entry, found := s.profileRequests[requestID]
	var result divaProfileResult
	completed := false
	if found && entry.accountMID == account {
		select {
		case <-entry.done:
			result, completed = entry.result, true
		default:
		}
	}
	s.mu.Unlock()
	switch {
	case !found || entry.accountMID != account:
		writeDIVAJSON(w, http.StatusNotFound, divaProfileRejection(requestID, "", "PROFILE_RESULT_NOT_FOUND"))
	case !completed:
		writeDIVAJSON(w, http.StatusOK, divaProfileResult{
			Version: divaProfileContractVersion, RequestID: requestID, State: divaProfileStatePending,
			Write: divaProfileWriteUnknown, Readback: divaProfileReadbackNotAttempted, Deduplicated: true,
		})
	default:
		result.Deduplicated = true
		writeDIVAJSON(w, http.StatusOK, result)
	}
}

func (s *divaControlServer) sweepProfileLocked(now time.Time) {
	for id, entry := range s.profileRequests {
		if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			delete(s.profileRequests, id)
		}
	}
}

// submitProfile applies request_id dedupe, the per-account single-flight gate
// and starts the write in the background. It waits a bounded time only.
func (s *divaControlServer) submitProfile(reqCtx context.Context, job *divaProfileJob) (divaProfileResult, int) {
	s.mu.Lock()
	s.sweepProfileLocked(time.Now())
	if entry, ok := s.profileRequests[job.requestID]; ok {
		s.mu.Unlock()
		if entry.fingerprint != job.fingerprint || entry.accountMID != job.accountMID {
			return divaProfileRejection(job.requestID, job.operation, "PROFILE_REQUEST_ID_REUSED"), http.StatusConflict
		}
		return s.waitProfile(reqCtx, job, entry, true), http.StatusOK
	}
	s.mu.Unlock()

	if !s.profileEnabled(job.operation) {
		return divaProfileRejection(job.requestID, job.operation, "PROFILE_OPERATION_DISABLED"), http.StatusForbidden
	}
	if code := s.profileProtocolBlock(job.operation); code != "" {
		return divaProfileRejection(job.requestID, job.operation, code), http.StatusForbidden
	}
	lc, selectionError := s.profileLogin(job.accountMID)
	if selectionError != nil {
		return divaProfileRejection(job.requestID, job.operation, "PROFILE_ACCOUNT_UNAVAILABLE"), selectionError.status
	}

	s.mu.Lock()
	if entry, ok := s.profileRequests[job.requestID]; ok {
		s.mu.Unlock()
		if entry.fingerprint != job.fingerprint || entry.accountMID != job.accountMID {
			return divaProfileRejection(job.requestID, job.operation, "PROFILE_REQUEST_ID_REUSED"), http.StatusConflict
		}
		return s.waitProfile(reqCtx, job, entry, true), http.StatusOK
	}
	if s.profileBusy[job.accountMID] {
		s.mu.Unlock()
		return divaProfileRejection(job.requestID, job.operation, "PROFILE_ACCOUNT_BUSY"), http.StatusConflict
	}
	if len(s.profileRequests) >= divaProfileMaxEntries {
		s.mu.Unlock()
		return divaProfileRejection(job.requestID, job.operation, "PROFILE_QUEUE_FULL"), http.StatusServiceUnavailable
	}
	entry := &divaProfileEntry{accountMID: job.accountMID, fingerprint: job.fingerprint, done: make(chan struct{})}
	s.profileRequests[job.requestID] = entry
	s.profileBusy[job.accountMID] = true
	s.wg.Add(1)
	s.mu.Unlock()

	go s.runProfile(job, entry, lc)
	return s.waitProfile(reqCtx, job, entry, false), http.StatusOK
}

func (s *divaControlServer) waitProfile(reqCtx context.Context, job *divaProfileJob, entry *divaProfileEntry, deduplicated bool) divaProfileResult {
	timer := time.NewTimer(s.cfg.profileWait)
	defer timer.Stop()
	select {
	case <-entry.done:
		s.mu.Lock()
		result := entry.result
		s.mu.Unlock()
		result.Deduplicated = deduplicated
		return result
	case <-timer.C:
	case <-reqCtx.Done():
	}
	return divaProfileResult{
		Version: divaProfileContractVersion, RequestID: job.requestID, Operation: job.operation,
		State: divaProfileStatePending, Write: divaProfileWriteUnknown,
		Readback: divaProfileReadbackNotAttempted, Deduplicated: deduplicated,
	}
}

func (s *divaControlServer) runProfile(job *divaProfileJob, entry *divaProfileEntry, lc *LineClient) {
	result := divaProfileResult{
		Version: divaProfileContractVersion, RequestID: job.requestID, Operation: job.operation,
		State: divaProfileStateUnknown, Write: divaProfileWriteUnknown, Readback: divaProfileReadbackNotAttempted,
		Code: "PROFILE_INTERNAL_UNKNOWN",
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			s.cfg.log.Error().Str("request_id", job.requestID).Msg("DIVA owner profile write panicked")
		}
		job.image = nil
		s.cfg.log.Info().Str("request_id", job.requestID).Str("operation", job.operation).
			Str("state", result.State).Str("write", result.Write).Str("readback", result.Readback).
			Str("code", result.Code).Int("http_status", result.httpStatus).Int("image_bytes", job.imageInfo.OutputBytes).
			Msg("DIVA owner profile write finished")
		s.mu.Lock()
		entry.result = result
		entry.expiresAt = time.Now().Add(divaProfileResultTTL)
		delete(s.profileBusy, job.accountMID)
		s.mu.Unlock()
		close(entry.done)
		s.wg.Done()
	}()
	ctx, cancel := context.WithTimeout(s.baseCtx, divaProfileJobBudget)
	defer cancel()
	result = s.executeProfile(ctx, job, lc)
}

func (s *divaControlServer) readSelfProfile(ctx context.Context, lc *LineClient, account string) (*line.Profile, bool) {
	readCtx, cancel := context.WithTimeout(ctx, divaProfileReadTimeout)
	defer cancel()
	profile, err := s.cfg.profileAPI.readSelf(readCtx, lc)
	if err != nil || profile == nil || profile.Mid != account {
		return nil, false
	}
	return profile, true
}

// divaProfileWriteCode maps a LINE RPC auth rejection of the text profile
// write to PROFILE_LINE_AUTH_REJECTED. OBS photo upload statuses keep their own
// codes: an OBS 401/403 rejects that upload's authorization and must not be
// reported as "the LINE login needs refreshing".
func divaProfileWriteCode(err error, code string) string {
	switch code {
	case "PROFILE_LINE_REJECTED", "PROFILE_HTTP_REJECTED":
		if line.IsAuthError(err) {
			return "PROFILE_LINE_AUTH_REJECTED"
		}
	}
	return code
}

// executeProfile: verify the account is really the logged-in self, write once,
// then read back. Only a matching fresh read back reports confirmed.
func (s *divaControlServer) executeProfile(ctx context.Context, job *divaProfileJob, lc *LineClient) divaProfileResult {
	result := divaProfileResult{
		Version: divaProfileContractVersion, RequestID: job.requestID, Operation: job.operation,
		Write: divaProfileWriteNotAttempted, Readback: divaProfileReadbackNotAttempted,
	}
	notSent := func(code string) divaProfileResult {
		result.State, result.Code = divaProfileStateNotSent, code
		return result
	}
	if ctx.Err() != nil {
		return notSent("PROFILE_SHUTTING_DOWN")
	}
	if code := s.profileProtocolBlock(job.operation); code != "" {
		return notSent(code)
	}
	before, ok := s.readSelfProfile(ctx, lc, job.accountMID)
	if !ok {
		return notSent("PROFILE_SELF_VERIFY_FAILED")
	}

	var matches func(*line.Profile) bool
	var writeErr error
	writeCtx, cancelWrite := context.WithTimeout(ctx, divaProfileWriteTimeout)
	switch job.operation {
	case divaProfileOperationName, divaProfileOperationStatus:
		attribute, current := line.ProfileAttributeDisplayName, before.DisplayName
		read := func(p *line.Profile) string { return p.DisplayName }
		if job.operation == divaProfileOperationStatus {
			attribute, current = line.ProfileAttributeStatusMessage, before.StatusMessage
			read = func(p *line.Profile) string { return p.StatusMessage }
		}
		if current == job.value {
			cancelWrite()
			result.State, result.Readback = divaProfileStateUnchanged, divaProfileReadbackMatch
			return result
		}
		matches = func(p *line.Profile) bool { return read(p) == job.value }
		writeErr = s.cfg.profileAPI.updateAttribute(writeCtx, lc, attribute, job.value)
	case divaProfileOperationCover:
		coverAPI, ok := s.cfg.profileAPI.(divaProfileCoverAPI)
		if !ok {
			cancelWrite()
			return notSent("PROFILE_COVER_UNSUPPORTED")
		}
		// A reliable fresh baseline must exist before any upload.
		readCtx, cancel := context.WithTimeout(ctx, divaProfileReadTimeout)
		_, err := coverAPI.readCover(readCtx, lc, job.accountMID)
		cancel()
		if err != nil {
			cancelWrite()
			return notSent("PROFILE_COVER_SELF_VERIFY_FAILED")
		}
		uploaded, err := coverAPI.uploadCover(writeCtx, lc, job.accountMID, job.image)
		writeErr = err
		cancelWrite()
		if writeErr != nil {
			outcome, code := line.ProfileWriteOutcomeOf(writeErr)
			result.Code = code
			result.httpStatus = line.ProfileWriteHTTPStatus(writeErr)
			switch outcome {
			case line.ProfileWriteNotSent:
				result.State = divaProfileStateNotSent
				return result
			case line.ProfileWriteRejected:
				result.State, result.Write = divaProfileStateRejected, divaProfileWriteRejected
				return result
			default:
				result.State, result.Write = divaProfileStateUnknown, divaProfileWriteUnknown
				return result
			}
		}
		result.Write, result.Readback = divaProfileWriteAccepted, divaProfileReadbackUnavailable
		if uploaded == nil || uploaded.ObjectID == "" {
			result.State = divaProfileStateUnknown
			result.Code = "PROFILE_COVER_UPLOAD_RESULT_INVALID"
			return result
		}
		// Upload + association is still not proof. Fresh self-scoped object
		// ID must match exactly, not just a changed thumbnail or public URL.
		for attempt := 0; attempt < divaProfileReadbackTries; attempt++ {
			if ctx.Err() != nil {
				break
			}
			if attempt > 0 {
				timer := time.NewTimer(divaProfileReadbackDelay)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
				}
			}
			readCtx, cancel := context.WithTimeout(ctx, divaProfileReadTimeout)
			oid, err := coverAPI.readCover(readCtx, lc, job.accountMID)
			cancel()
			if err != nil {
				continue
			}
			result.Readback = divaProfileReadbackMismatch
			if oid == uploaded.ObjectID {
				result.State, result.Readback = divaProfileStateConfirmed, divaProfileReadbackMatch
				return result
			}
		}
		result.State, result.Code = divaProfileStateUnconfirmed, "PROFILE_COVER_READBACK_NOT_CONFIRMED"
		return result
	case divaProfileOperationPhoto:
		// A changed picture marker is required; HTTP 200 or OBS identifiers alone
		// are not success. An unchanged marker stays unconfirmed.
		matches = func(p *line.Profile) bool {
			return (p.PictureStatus != "" || p.PicturePath != "") &&
				(p.PictureStatus != before.PictureStatus || p.PicturePath != before.PicturePath)
		}
		_, writeErr = s.cfg.profileAPI.uploadImage(writeCtx, lc, job.accountMID, job.image)
	default:
		cancelWrite()
		return notSent("PROFILE_OPERATION_INVALID")
	}
	cancelWrite()
	if writeErr == nil {
		result.Write = divaProfileWriteAccepted
	} else {
		outcome, code := line.ProfileWriteOutcomeOf(writeErr)
		result.Code = divaProfileWriteCode(writeErr, code)
		result.httpStatus = line.ProfileWriteHTTPStatus(writeErr)
		switch outcome {
		case line.ProfileWriteNotSent:
			result.State = divaProfileStateNotSent
			return result
		case line.ProfileWriteRejected:
			result.State, result.Write = divaProfileStateRejected, divaProfileWriteRejected
			return result
		default:
			result.Write = divaProfileWriteUnknown
		}
	}

	result.Readback = divaProfileReadbackUnavailable
	for attempt := 0; attempt < divaProfileReadbackTries; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(divaProfileReadbackDelay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			}
		}
		if ctx.Err() != nil {
			break
		}
		after, readOK := s.readSelfProfile(ctx, lc, job.accountMID)
		if !readOK {
			continue
		}
		if matches(after) {
			result.Readback = divaProfileReadbackMatch
			break
		}
		result.Readback = divaProfileReadbackMismatch
	}
	switch {
	case result.Readback == divaProfileReadbackMatch:
		// Even an unclear write response is confirmed by LINE's own read back.
		result.State, result.Code = divaProfileStateConfirmed, ""
	case result.Write == divaProfileWriteAccepted:
		result.State, result.Code = divaProfileStateUnconfirmed, "PROFILE_READBACK_NOT_CONFIRMED"
	default:
		result.State = divaProfileStateUnknown
	}
	return result
}
