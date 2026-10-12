package connector

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

type divaGroupState string

const (
	divaGroupUnknown divaGroupState = "unknown"
	divaGroupJoined  divaGroupState = "joined"
	divaGroupPending divaGroupState = "pending"
	divaGroupLeft    divaGroupState = "left"
)

func isDIVAGroupID(mid string) bool {
	return divaChatMIDPattern.MatchString(mid) && strings.EqualFold(mid[:1], "c")
}

// A fresh self list is authoritative. A c/C MID plus groupExtra identifies a
// group: current Chrome returns type=1 for real GROUPs, unlike old fixtures.
// Absence of self in GetChats alone is inconclusive (Chrome can omit caller).
func classifyDIVAGroup(mids *line.GetAllChatMidsResponse, chat *line.Chat, self, gid string) (divaGroupState, string) {
	if mids == nil || !isDIVAGroupID(gid) {
		return divaGroupUnknown, "list_unavailable"
	}
	member, pending := false, false
	for _, m := range mids.MemberChatMids {
		member = member || m == gid
	}
	for _, m := range mids.InvitedChatMids {
		pending = pending || m == gid
	}
	if member && pending {
		return divaGroupUnknown, "list_conflict"
	}
	if chat != nil {
		if chat.ChatMid != gid || !isDIVAGroupID(chat.ChatMid) || chat.Extra.GroupExtra == nil || (chat.Type != 0 && chat.Type != 1) {
			return divaGroupUnknown, "chat_invalid"
		}
		extra := chat.Extra.GroupExtra
		selfMember, selfPending := extra.MemberMids[self], extra.InviteeMids[self]
		if selfMember && selfPending || member && selfPending || pending && selfMember {
			return divaGroupUnknown, "self_conflict"
		}
		if member {
			return divaGroupJoined, "self_list"
		}
		if pending {
			return divaGroupPending, "self_list"
		}
		if selfMember {
			return divaGroupJoined, "self_chat"
		}
		// A detail-only invite is not sufficient authority to accept an invitation.
		if selfPending {
			return divaGroupUnknown, "invite_not_in_self_list"
		}
		return divaGroupLeft, "self_absent"
	}
	if member {
		return divaGroupJoined, "self_list"
	}
	if pending {
		return divaGroupPending, "self_list"
	}
	return divaGroupUnknown, "detail_unavailable"
}

type divaGroupReader struct {
	mids  func(context.Context) (*line.GetAllChatMidsResponse, error)
	chats func(context.Context, []string) (*line.GetChatsResponse, error)
}

func newDIVAGroupReader(client *line.Client) divaGroupReader {
	return divaGroupReader{
		mids: func(ctx context.Context) (*line.GetAllChatMidsResponse, error) {
			return client.GetAllChatMidsContext(ctx, true, true)
		},
		chats: func(ctx context.Context, ids []string) (*line.GetChatsResponse, error) {
			return client.GetChatsContext(ctx, ids, true, true)
		},
	}
}
func waitDIVAGroup(ctx context.Context) error {
	timer := time.NewTimer(700 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// At most one accept, three fresh checks. Detail-only auto-join needs two
// consecutive independent self snapshots. Conflicts/errors never mean success.
func reconcileDIVAP4(ctx context.Context, reader divaGroupReader, self, gid string, accept func() error, wait func(context.Context) error) (divaGroupState, bool, string) {
	accepted, detailConfirmed := false, false
	reason := "unverified"
	lastState := divaGroupUnknown
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			return divaGroupUnknown, accepted, "deadline"
		}
		mids, err := reader.mids(ctx)
		if err != nil || mids == nil {
			reason = "list_unavailable"
			detailConfirmed = false
			lastState = divaGroupUnknown
		} else {
			chats, fetchErr := reader.chats(ctx, []string{gid})
			if fetchErr != nil || chats == nil || len(chats.Chats) != 1 {
				reason = "detail_unavailable"
				detailConfirmed = false
				lastState = divaGroupUnknown
			} else {
				state, why := classifyDIVAGroup(mids, &chats.Chats[0], self, gid)
				reason = why
				lastState = state
				if state == divaGroupJoined {
					if why == "self_list" || detailConfirmed {
						return state, accepted, why
					}
					detailConfirmed = true
				} else {
					detailConfirmed = false
					if state == divaGroupPending && !accepted {
						if accept == nil || accept() != nil {
							return divaGroupUnknown, false, "accept_failed"
						}
						accepted = true
					}
				}
			}
		}
		if attempt < 2 && wait(ctx) != nil {
			return divaGroupUnknown, accepted, "deadline"
		}
	}
	if lastState == divaGroupLeft || lastState == divaGroupPending {
		return lastState, accepted, reason
	}
	return divaGroupUnknown, accepted, reason
}
func divaP4NewJoin(ready, already, accepted bool, state divaGroupState) bool {
	return state == divaGroupJoined && (accepted || ready && !already)
}

var errDIVAGroupConflict = errors.New("self membership verification unavailable")
