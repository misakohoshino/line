package connector

// DIVA read-only LINE receive status (AO-10 C3).
//
//	Server A host monitor → GET /diva/v1/status → recorded receive facts
//
// /diva/v1/health only says the control server is alive. /diva/v1/status
// reports what the LINE receive loop itself observed: the last bridge state
// the connector sent, SSE events and failures, and the periodic receive auth
// probe (getLastOpRevision). It never infers health from customer traffic.
//
// Safety model:
//   - Same listener and Bearer token as /diva/v1/send.
//   - Read only: the handler copies the recorded struct. It never logs in,
//     reconnects, probes or sends.
//   - Minimal disclosure: no tokens, MIDs, login IDs, message content or raw
//     error text. Logins are listed by an opaque positional index.

import (
	"net/http"
	"sort"
	"sync"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/status"
)

// receiveStatusNow is the clock for recorded receive facts (tests override it).
var receiveStatusNow = time.Now

// lineReceiveStatus holds the receive-channel facts of one LineClient. The
// zero value is ready to use.
type lineReceiveStatus struct {
	mu sync.Mutex

	bridgeStateEvent status.BridgeStateEvent
	bridgeStateError status.BridgeStateErrorCode
	bridgeStateAt    time.Time

	lastSSEEventAt         time.Time
	lastSSEFailureAt       time.Time
	consecutiveSSEFailures int

	lastAuthProbeOKAt            time.Time
	lastAuthProbeFailedAt        time.Time
	consecutiveAuthProbeFailures int
}

type lineReceiveStatusSnapshot struct {
	bridgeStateEvent             status.BridgeStateEvent
	bridgeStateError             status.BridgeStateErrorCode
	bridgeStateAt                time.Time
	lastSSEEventAt               time.Time
	lastSSEFailureAt             time.Time
	consecutiveSSEFailures       int
	lastAuthProbeOKAt            time.Time
	lastAuthProbeFailedAt        time.Time
	consecutiveAuthProbeFailures int
}

func (s *lineReceiveStatus) recordBridgeState(event status.BridgeStateEvent, code status.BridgeStateErrorCode) {
	now := receiveStatusNow()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bridgeStateEvent = event
	s.bridgeStateError = code
	s.bridgeStateAt = now
	if event == status.StateConnected {
		// A fresh connection starts a new receive loop; earlier failure streaks
		// no longer describe it.
		s.consecutiveSSEFailures = 0
		s.consecutiveAuthProbeFailures = 0
	}
}

// recordSSEEvent is called for every SSE event the receive loop sees,
// keepalives included. Any event proves the stream is flowing again.
func (s *lineReceiveStatus) recordSSEEvent() {
	now := receiveStatusNow()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSSEEventAt = now
	s.consecutiveSSEFailures = 0
}

func (s *lineReceiveStatus) recordSSEFailure() {
	now := receiveStatusNow()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSSEFailureAt = now
	s.consecutiveSSEFailures++
}

func (s *lineReceiveStatus) recordAuthProbe(ok bool) {
	now := receiveStatusNow()
	s.mu.Lock()
	defer s.mu.Unlock()
	if ok {
		s.lastAuthProbeOKAt = now
		s.consecutiveAuthProbeFailures = 0
		return
	}
	s.lastAuthProbeFailedAt = now
	s.consecutiveAuthProbeFailures++
}

func (s *lineReceiveStatus) snapshot() lineReceiveStatusSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return lineReceiveStatusSnapshot{
		bridgeStateEvent:             s.bridgeStateEvent,
		bridgeStateError:             s.bridgeStateError,
		bridgeStateAt:                s.bridgeStateAt,
		lastSSEEventAt:               s.lastSSEEventAt,
		lastSSEFailureAt:             s.lastSSEFailureAt,
		consecutiveSSEFailures:       s.consecutiveSSEFailures,
		lastAuthProbeOKAt:            s.lastAuthProbeOKAt,
		lastAuthProbeFailedAt:        s.lastAuthProbeFailedAt,
		consecutiveAuthProbeFailures: s.consecutiveAuthProbeFailures,
	}
}

// sendBridgeState records the state for /diva/v1/status and sends it to the
// bridge exactly as before.
func (lc *LineClient) sendBridgeState(st status.BridgeState) {
	lc.receiveStatus.recordBridgeState(st.StateEvent, st.Error)
	if lc.UserLogin != nil && lc.UserLogin.BridgeState != nil {
		lc.UserLogin.BridgeState.Send(st)
	}
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

type divaStatusResponse struct {
	Now        string            `json:"now"`
	LoginCount int               `json:"login_count"`
	Logins     []divaLoginStatus `json:"logins"`
}

type divaBridgeStateStatus struct {
	StateEvent string  `json:"state_event"`
	ErrorCode  *string `json:"error_code"`
	At         string  `json:"at"`
}

type divaLoginStatus struct {
	Index                        int                    `json:"index"`
	LoggedIn                     bool                   `json:"logged_in"`
	SessionInvalidated           bool                   `json:"session_invalidated"`
	LastBridgeState              *divaBridgeStateStatus `json:"last_bridge_state"`
	LastSSEEventAt               *string                `json:"last_sse_event_at"`
	ConsecutiveSSEFailures       int                    `json:"consecutive_sse_failures"`
	LastSSEFailureAt             *string                `json:"last_sse_failure_at"`
	SSEReconnecting              bool                   `json:"sse_reconnecting"`
	LastAuthProbeOKAt            *string                `json:"last_auth_probe_ok_at"`
	LastAuthProbeFailedAt        *string                `json:"last_auth_probe_failed_at"`
	ConsecutiveAuthProbeFailures int                    `json:"consecutive_auth_probe_failures"`
}

func divaStatusTime(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format(time.RFC3339)
	return &s
}

// buildDIVAStatus reads the recorded facts of every LINE login. Logins are
// ordered by login ID for a stable index, but the ID itself is not reported.
func buildDIVAStatus(logins []*bridgev2.UserLogin, now time.Time) divaStatusResponse {
	type entry struct {
		key string
		lc  *LineClient
	}
	var clients []entry
	for _, login := range logins {
		if login == nil {
			continue
		}
		lc, ok := login.Client.(*LineClient)
		if !ok || lc == nil {
			continue
		}
		key := ""
		if login.UserLogin != nil {
			key = string(login.ID)
		}
		clients = append(clients, entry{key: key, lc: lc})
	}
	sort.SliceStable(clients, func(a, b int) bool { return clients[a].key < clients[b].key })

	out := divaStatusResponse{
		Now:        now.UTC().Format(time.RFC3339),
		LoginCount: len(clients),
		Logins:     make([]divaLoginStatus, 0, len(clients)),
	}
	for i, c := range clients {
		snap := c.lc.receiveStatus.snapshot()
		item := divaLoginStatus{
			Index:                        i,
			LoggedIn:                     c.lc.IsLoggedIn(),
			SessionInvalidated:           c.lc.isSessionInvalidated(),
			LastSSEEventAt:               divaStatusTime(snap.lastSSEEventAt),
			ConsecutiveSSEFailures:       snap.consecutiveSSEFailures,
			LastSSEFailureAt:             divaStatusTime(snap.lastSSEFailureAt),
			SSEReconnecting:              snap.consecutiveSSEFailures > 0,
			LastAuthProbeOKAt:            divaStatusTime(snap.lastAuthProbeOKAt),
			LastAuthProbeFailedAt:        divaStatusTime(snap.lastAuthProbeFailedAt),
			ConsecutiveAuthProbeFailures: snap.consecutiveAuthProbeFailures,
		}
		if snap.bridgeStateEvent != "" {
			bs := &divaBridgeStateStatus{
				StateEvent: string(snap.bridgeStateEvent),
				At:         snap.bridgeStateAt.UTC().Format(time.RFC3339),
			}
			if snap.bridgeStateError != "" {
				code := string(snap.bridgeStateError)
				bs.ErrorCode = &code
			}
			item.LastBridgeState = bs
		}
		out.Logins = append(out.Logins, item)
	}
	return out
}

func (s *divaControlServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
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
	writeDIVAJSON(w, http.StatusOK, buildDIVAStatus(logins, receiveStatusNow()))
}
