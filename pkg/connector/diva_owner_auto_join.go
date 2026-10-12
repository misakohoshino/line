package connector

// P4: A's own pending GROUP invitations are automatically accepted, regardless
// of inviter/OWNER identity. Only fresh LINE membership lists are authoritative.
// Confirmation is required before a signed notification fact reaches Worker.
import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
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

// P4 numeric group-stable ID allows Worker's 90-day ledger to suppress replay
// from repeated invite operations and reconnects without falsely re-notifying.
func divaP4EventID(chatMID string) string {
  h := fnv.New64a()
  _, _ = h.Write([]byte(chatMID))
  return fmt.Sprintf("p4/%d", h.Sum64())
}

func (lc *LineClient) p4AnnounceConfirmedJoin(ctx context.Context, client *line.Client, chatMID string) {
  // Reconcile the newly joined group; never make the notice conditional on
  // Beeper portal creation, which can fail independently.
  lc.syncChatsNow(ctx)
  confirmedChat, err := client.GetChatsContext(ctx, []string{chatMID}, false, false)
  name := "名稱未能取得"
  if err == nil && confirmedChat != nil && len(confirmedChat.Chats) == 1 && confirmedChat.Chats[0].ChatMid == chatMID {
    if n := strings.TrimSpace(confirmedChat.Chats[0].ChatName); n != "" { name = n }
  }
  eventID := divaP4EventID(chatMID)
  fact := map[string]any{
    "version": 1, "event_type": "owner_group_auto_join",
    "event_id": eventID, "account_mid": lc.Mid, "group_id": chatMID,
    "group_name": name, "inviter_mid": "",
  }
  lc.forwardDIVAInbound(func() ([]byte, error) { return json.Marshal(fact) }, nil, chatMID, eventID, false)
  lc.UserLogin.Bridge.Log.Info().Str("p4_case", "confirmed").Msg("P4 self group membership verified; signed owner notice queued")
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
  // A self-invite event must never be inferred merely from seeing another
  // member invited into a pre-existing group. Use the pre-event joined cache.
  knownGroups := lc.getKnownMemberChatMIDs()
  wasAlreadyJoined := false
  for _, mid := range knownGroups {
    if mid == chatMID { wasAlreadyJoined = true; break }
  }
  if joined {
    if len(knownGroups) > 0 && !wasAlreadyJoined {
      lc.UserLogin.Bridge.Log.Info().Str("p4_case","auto_member_list").Msg("P4 auto-join confirmed by fresh self membership")
      lc.p4AnnounceConfirmedJoin(ctx, client, chatMID)
    }
    return
  }

	chats, err := client.GetChatsContext(ctx, []string{chatMID}, true, true)
	if err != nil || chats == nil || len(chats.Chats) != 1 || chats.Chats[0].ChatMid != chatMID || chats.Chats[0].Type != 0 || chats.Chats[0].Extra.GroupExtra == nil {
		lc.UserLogin.Bridge.Log.Warn().Err(err).Msg("P4 group metadata unavailable")
		return
	}
  if !pending {
    // In automatic-join groups, GetChats can expose the member update before
    // GetAllChatMids reflects it. Require TWO independent fresh GetChats reads,
    // both saying the authenticated self is a member and is not an invitee.
    // Also require a populated pre-event baseline without this group.
    if len(knownGroups) == 0 || wasAlreadyJoined { return }
    first := chats.Chats[0].Extra.GroupExtra
    if !first.MemberMids[lc.Mid] || first.InviteeMids[lc.Mid] {
      lc.UserLogin.Bridge.Log.Debug().Str("p4_case","neither_not_self_member").Msg("P4 skip unverified group invite")
      return
    }
    select {
    case <-time.After(600 * time.Millisecond):
    case <-ctx.Done(): return
    }
    again, checkErr := client.GetChatsContext(ctx, []string{chatMID}, true, true)
    if checkErr != nil || again == nil || len(again.Chats) != 1 || again.Chats[0].ChatMid != chatMID || again.Chats[0].Type != 0 || again.Chats[0].Extra.GroupExtra == nil { return }
    second := again.Chats[0].Extra.GroupExtra
    if !second.MemberMids[lc.Mid] || second.InviteeMids[lc.Mid] { return }
    lc.UserLogin.Bridge.Log.Info().Str("p4_case","auto_chat_snapshot").Msg("P4 auto-join confirmed by repeated self member snapshots")
    lc.p4AnnounceConfirmedJoin(ctx, client, chatMID)
    return
  }
  // Only a verified self pending invitation can be accepted explicitly.
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
	lc.p4AnnounceConfirmedJoin(ctx, client, chatMID)
}
