package connector

// OWNER P1 read-only, private Docker control endpoint. No arbitrary RPC,
// portal creation, invitations, profile writes or auth recovery/login here.
import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"time"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

type divaJoinedGroup struct {
	GID  string `json:"gid"`
	Name string `json:"name"`
}

type divaOwnerGroupReader struct {
	self  string
	mids  func(context.Context) (*line.GetAllChatMidsResponse, error)
	chats func(context.Context, []string) (*line.GetChatsResponse, error)
}

func readDIVAOwnerGroups(ctx context.Context, reader divaOwnerGroupReader) ([]divaJoinedGroup, error) {
	mids, err := reader.mids(ctx)
	if err != nil || mids == nil {
		return nil, errors.New("joined membership list unavailable")
	}
	seen := make(map[string]bool)
	groups := make([]divaJoinedGroup, 0)
	// Only fresh self member groups are candidates; portals/history are never proof.
	for _, mid := range mids.MemberChatMids {
		state, _ := classifyDIVAGroup(mids, nil, reader.self, mid)
		if state == divaGroupUnknown && isDIVAGroupID(mid) {
			return nil, errDIVAGroupConflict
		}
		if !seen[mid] && state == divaGroupJoined {
			seen[mid] = true
			groups = append(groups, divaJoinedGroup{GID: mid})
		}
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].GID < groups[j].GID })
	for start := 0; start < len(groups); start += 20 {
		if ctx.Err() != nil {
			break // Membership is complete; unresolved names remain empty.
		}
		end := min(start+20, len(groups))
		batch := make([]string, end-start)
		positions := make(map[string]int, len(batch))
		for i := start; i < end; i++ {
			batch[i-start] = groups[i].GID
			positions[groups[i].GID] = i
		}
		chats, fetchErr := reader.chats(ctx, batch)
		if fetchErr != nil || chats == nil {
			continue // A name failure must not erase a member GID.
		}
		for _, chat := range chats.Chats {
			if index, ok := positions[chat.ChatMid]; ok {
				if reader.self != "" {
					state, _ := classifyDIVAGroup(mids, &chat, reader.self, chat.ChatMid)
					if state != divaGroupJoined {
						return nil, errDIVAGroupConflict
					}
				}
				groups[index].Name = chat.ChatName
			}
		}
	}
	return groups, nil
}

func listDIVAOwnerGroups(ctx context.Context, lc *LineClient) ([]divaJoinedGroup, error) {
	client := lc.newClient()
	reader := newDIVAGroupReader(client)
	for attempt := 0; attempt < 3; attempt++ {
		groups, err := readDIVAOwnerGroups(ctx, divaOwnerGroupReader{self: lc.Mid, mids: reader.mids, chats: reader.chats})
		if !errors.Is(err, errDIVAGroupConflict) {
			return groups, err
		}
		lc.UserLogin.Bridge.Log.Warn().Str("group_query_reason", "self_conflict").Int("attempt", attempt+1).Msg("LG fresh membership contradiction")
		if attempt < 2 && waitDIVAGroup(ctx) != nil {
			return nil, errDIVAGroupConflict
		}
	}
	return nil, errDIVAGroupConflict
}

func (s *divaControlServer) handleOwnerJoinedGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeDIVAError(w, http.StatusMethodNotAllowed, "", outboundInvalidRequest, "method not allowed")
		return
	}
	if !s.authorized(r) {
		writeDIVAError(w, http.StatusUnauthorized, "", outboundUnauthorized, "missing or invalid bearer token")
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	account := query.Get("account_mid")
	if err != nil || len(query) != 1 || len(query["account_mid"]) != 1 || !divaUserMIDPattern.MatchString(account) {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "one explicit account_mid is required")
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
	case s.ownerQuerySem <- struct{}{}:
		defer func() { <-s.ownerQuerySem }()
	default:
		writeDIVAError(w, http.StatusServiceUnavailable, "", outboundInternal, "owner query busy")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	list := s.cfg.listOwnerGroups
	if list == nil {
		list = listDIVAOwnerGroups
	}
	groups, listErr := list(ctx, lc)
	if listErr != nil {
		// Never return error text, token, account or group identifiers in logs.
		writeDIVAError(w, http.StatusServiceUnavailable, "", outboundInternal, "joined group list unavailable")
		return
	}
	writeDIVAJSON(w, http.StatusOK, map[string]any{
		"version": 1, "ok": true, "complete": true, "total": len(groups), "groups": groups,
	})
}
