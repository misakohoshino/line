package connector

// P4: A's own pending GROUP invitations are automatically accepted, regardless
// of inviter/OWNER identity. Only fresh LINE membership lists are authoritative.
// Confirmation is required before a signed notification fact reaches Worker.
import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

var divaP4InFlight sync.Map

func divaP4Membership(mids *line.GetAllChatMidsResponse, chatMID string) (joined, pending bool) {
	if mids == nil {
		return false, false
	}
	for _, mid := range mids.MemberChatMids {
		if mid == chatMID {
			return true, false
		}
	}
	for _, mid := range mids.InvitedChatMids {
		if mid == chatMID {
			return false, true
		}
	}
	return false, false
}

func (lc *LineClient) maybeAutoAcceptGroupInvite(chatMID string) {
	if os.Getenv("DIVA_P4_AUTO_ACCEPT") != "true" {
		return
	}
	if !divaChatMIDPattern.MatchString(chatMID) || !strings.EqualFold(chatMID[:1], "c") {
		return
	}
	key := string(lc.UserLogin.ID) + "/" + chatMID
	if _, loaded := divaP4InFlight.LoadOrStore(key, true); loaded {
		return
	}
	defer divaP4InFlight.Delete(key)

	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	client := lc.newClient()
	mids, err := client.GetAllChatMidsContext(ctx, true, true)
	if err != nil || mids == nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("P4 pending membership unavailable")
		return
	}
	joined, pending := divaP4Membership(mids, chatMID)
	if joined || !pending {
		return
	} // Never accept an invitation concerning another user.

	chats, err := client.GetChatsContext(ctx, []string{chatMID}, true, true)
	if err != nil || chats == nil || len(chats.Chats) != 1 || chats.Chats[0].ChatMid != chatMID || chats.Chats[0].Type != 0 {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("P4 pending group metadata unavailable")
		return
	}
	if err := client.AcceptChatInvitation(int64(lc.nextReqSeq()), chatMID); err != nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("P4 accept group invite failed")
		return
	}
	confirmed := false
	for attempt := 0; attempt < 3; attempt++ {
		refreshed, refreshErr := client.GetAllChatMidsContext(ctx, true, true)
		if refreshErr == nil && refreshed != nil {
			member, _ := divaP4Membership(refreshed, chatMID)
			if member {
				confirmed = true
				break
			}
		}
		if attempt < 2 {
			select {
			case <-time.After(700 * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}
	}
	if !confirmed {
		lc.UserLogin.Bridge.Log.Warn().Msg("P4 accept membership not confirmed, no success notification")
		return
	}
	// Existing portal resync clears Beeper's pending message request after actual join.
	lc.syncChatsNow(ctx)
	confirmedChat, err := client.GetChatsContext(ctx, []string{chatMID}, false, false)
	name := "名稱未能取得"
	if err == nil && confirmedChat != nil && len(confirmedChat.Chats) == 1 && confirmedChat.Chats[0].ChatMid == chatMID {
		if n := strings.TrimSpace(confirmedChat.Chats[0].ChatName); n != "" {
			name = n
		}
	}
	// The invitation operation parameters do not reliably identify the inviter.
	// Worker must display 未能取得, never substitute chat creator.
	eventID := fmt.Sprintf("p4/%d", time.Now().UnixNano())
	fact := map[string]any{
		"version": 1, "event_type": "owner_group_auto_join",
		"event_id": eventID, "account_mid": lc.Mid, "group_id": chatMID,
		"group_name": name, "inviter_mid": "",
	}
	lc.forwardDIVAInbound(func() ([]byte, error) { return json.Marshal(fact) }, nil, chatMID, eventID, false)
	lc.UserLogin.Bridge.Log.Info().Msg("P4 group join confirmed; signed owner notification fact queued")
}
