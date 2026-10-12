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

type divaP4InviterRecord struct {
  inviter string
  recorded time.Time
}
var divaP4Inviters sync.Map

// Only a LINE system invitation whose explicit invitee is this account can
// name the inviter. Op123/124 parameters and group creator are not evidence.
func (lc *LineClient) rememberP4Inviter(msg *line.Message) {
  if msg == nil || msg.ContentMetadata == nil || !divaChatMIDPattern.MatchString(msg.To) { return }
  switch msg.ContentMetadata["LOC_KEY"] {
  case "C_GI", "C_MI", "A_MI":
  default: return
  }
  parts := strings.SplitN(msg.ContentMetadata["LOC_ARGS"], "\x1e", 2)
  if len(parts) != 2 || !lc.isOwnMID(parts[1]) { return }
  inviter := parts[0]
  if !divaUserMIDPattern.MatchString(inviter) || lc.isOwnMID(inviter) || msg.From != inviter { return }
  divaP4Inviters.Store(string(lc.UserLogin.ID)+"/"+msg.To, divaP4InviterRecord{inviter: inviter, recorded: time.Now()})
}

func (lc *LineClient) takeP4Inviter(chatMID string) string {
  key := string(lc.UserLogin.ID)+"/"+chatMID
  value, ok := divaP4Inviters.LoadAndDelete(key)
  if !ok { return "" }
  record, ok := value.(divaP4InviterRecord)
  if !ok || time.Since(record.recorded) > 2*time.Minute { return "" }
  return record.inviter
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
		if n := strings.TrimSpace(confirmedChat.Chats[0].ChatName); n != "" {
			name = n
		}
	}
	// System messages may arrive after Op124; wait briefly before finalizing the
  // inviter field, without delaying LINE membership confirmation itself.
  select {
  case <-time.After(2*time.Second):
  case <-ctx.Done(): return
  }
  inviterMID := lc.takeP4Inviter(chatMID)
  inviterName := ""
  if inviterMID != "" {
    if contacts, err := client.GetContactsV2([]string{inviterMID}); err == nil && contacts != nil {
      if wrapper, ok := contacts.Contacts[inviterMID]; ok {
        inviterName = strings.TrimSpace(wrapper.Contact.EffectiveDisplayName())
        if len([]rune(inviterName)) > 80 { inviterName = string([]rune(inviterName)[:80]) }
      }
    }
  }
  eventID := divaP4EventID(chatMID)
	fact := map[string]any{
		"version": 1, "event_type": "owner_group_auto_join",
		"event_id": eventID, "account_mid": lc.Mid, "group_id": chatMID,
		"group_name": name, "inviter_mid": inviterMID, "inviter_name": inviterName,
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

	lc.cacheMu.Lock()
	ready := lc.knownMemberChatMIDsReady
	_, already := lc.knownMemberChatMIDs[chatMID]
	lc.cacheMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := lc.newClient()
	state, accepted, reason := reconcileDIVAP4(ctx, newDIVAGroupReader(client), lc.Mid, chatMID,
		func() error { return client.AcceptChatInvitation(int64(lc.nextReqSeq()), chatMID) }, waitDIVAGroup)
	if !divaP4NewJoin(ready, already, accepted, state) {
		lc.UserLogin.Bridge.Log.Info().Str("p4_state", string(state)).Str("p4_reason", reason).
			Bool("baseline_ready", ready).Bool("already_joined", already).Msg("P4 membership checked; no new-join notice")
		return
	}
	lc.cacheMu.Lock()
	if lc.knownMemberChatMIDs == nil {
		lc.knownMemberChatMIDs = make(map[string]struct{})
	}
	lc.knownMemberChatMIDs[chatMID] = struct{}{}
	lc.cacheMu.Unlock()
	lc.UserLogin.Bridge.Log.Info().Str("p4_state", string(state)).Str("p4_reason", reason).Msg("P4 new self join confirmed")
	lc.p4AnnounceConfirmedJoin(ctx, client, chatMID)
}
