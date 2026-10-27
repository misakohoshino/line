package connector

// Dispatch round 2 (DIVA ServerB docs/17 §0.9): before nudging a selected
// driver with "Y" to join the rebate report group, Server A asks whether that
// driver is a joined member of the group.
//
//	Server A → POST /diva/v1/group/member-check {"chat_id","mid"} → IN / OUT / UNKNOWN
//
// Read-only: one GetChats (with members) call, never sends, never returns the
// member list, never logs group or member identifiers. OUT is only answered
// from a fresh, non-empty LINE member list; anything ambiguous (LINE's known
// empty member map, errors, timeouts) is UNKNOWN so Server A never nudges blindly.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"time"

	"maunium.net/go/mautrix/bridgev2"
)

const (
	divaMemberIn      = "IN"
	divaMemberOut     = "OUT"
	divaMemberUnknown = "UNKNOWN"

	divaMemberCheckMaxBodyBytes = 4 << 10
	divaMemberCheckTimeout      = 10 * time.Second
)

// divaGroupMembers is one lookup result. Fresh is the joined member list from
// LINE (including the caller); Fallback holds members known only from the
// cache or the Matrix room, which can prove IN but never OUT.
type divaGroupMembers struct {
	Fresh    []string
	Fallback []string
}

type divaMemberCheckRequest struct {
	ChatID     string  `json:"chat_id"`
	MID        string  `json:"mid"`
	AccountMID *string `json:"account_mid"`
}

// classifyDIVAMember answers IN / OUT / UNKNOWN for one member.
func classifyDIVAMember(members divaGroupMembers, self, target string) string {
	if slices.Contains(members.Fresh, target) || slices.Contains(members.Fallback, target) {
		return divaMemberIn
	}
	for _, mid := range members.Fresh {
		if mid != self {
			// A non-empty fresh list without the target: the driver is not a joined member
			// (pending invitees are excluded on purpose, they cannot read the group yet).
			return divaMemberOut
		}
	}
	return divaMemberUnknown
}

// lookupDIVAGroupMembers reads the joined members of one group. When LINE returns
// only the caller (known empty MemberMids issue) the cached list and the Matrix
// room members are added as fallback evidence.
func lookupDIVAGroupMembers(ctx context.Context, lc *LineClient, chatID string) (divaGroupMembers, error) {
	fresh, _, err := lc.getChatMemberMIDs(ctx, chatID)
	if err != nil {
		return divaGroupMembers{}, err
	}
	result := divaGroupMembers{Fresh: fresh}
	if len(fresh) > 1 {
		return result, nil
	}
	lc.cacheMu.Lock()
	cached := append([]string(nil), lc.groupMemberCache[chatID]...)
	lc.cacheMu.Unlock()
	result.Fallback = cached
	if matrixMembers, matrixErr := lc.getGroupMemberMIDsViaMatrix(ctx, chatID); matrixErr == nil {
		result.Fallback = append(result.Fallback, matrixMembers...)
	}
	return result, nil
}

func (s *divaControlServer) handleGroupMemberCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeDIVAError(w, http.StatusMethodNotAllowed, "", outboundInvalidRequest, "method not allowed")
		return
	}
	if !s.authorized(r) {
		writeDIVAError(w, http.StatusUnauthorized, "", outboundUnauthorized, "missing or invalid bearer token")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, divaMemberCheckMaxBodyBytes))
	if err != nil {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "request body too large or unreadable")
		return
	}
	var req divaMemberCheckRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if dec.Decode(&req) != nil || dec.More() {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "body must be {\"chat_id\",\"mid\"}")
		return
	}
	account := ""
	if req.AccountMID != nil {
		account = *req.AccountMID
	}
	if !isDIVAGroupID(req.ChatID) || !divaUserMIDPattern.MatchString(req.MID) ||
		(account != "" && !divaUserMIDPattern.MatchString(account)) {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "chat_id must be a group and mid a user")
		return
	}
	var logins []*bridgev2.UserLogin
	if s.cfg.logins != nil {
		logins = s.cfg.logins()
	}
	lc, selectionError := selectDIVALogin(logins, account)
	if selectionError != nil {
		writeDIVAError(w, selectionError.status, "", selectionError.code, selectionError.detail)
		return
	}
	select {
	case s.memberCheckSem <- struct{}{}:
		defer func() { <-s.memberCheckSem }()
	default:
		writeDIVAError(w, http.StatusServiceUnavailable, "", outboundInternal, "member check busy")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), divaMemberCheckTimeout)
	defer cancel()
	lookup := s.cfg.groupMembers
	if lookup == nil {
		lookup = lookupDIVAGroupMembers
	}
	status := divaMemberUnknown
	members, lookupErr := lookup(ctx, lc, req.ChatID)
	if lookupErr == nil {
		status = classifyDIVAMember(members, lc.Mid, req.MID)
	}
	// Never log group/member identifiers or LINE error text.
	s.cfg.log.Debug().Str("member_check", status).Bool("lookup_failed", lookupErr != nil).Msg("DIVA group member check")
	writeDIVAJSON(w, http.StatusOK, map[string]any{"version": 1, "ok": true, "status": status})
}
