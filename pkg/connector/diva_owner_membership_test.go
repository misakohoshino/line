package connector

import (
	"context"
	"errors"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
	"testing"
)

func group171(member, pending bool) line.Chat {
	members, invitees := line.FlexibleMidMap{}, line.FlexibleMidMap{}
	if member {
		members["Uself_171"] = true
	}
	if pending {
		invitees["Uself_171"] = true
	}
	return line.Chat{ChatMid: "Cgroup_fake-171", Type: 1, Extra: line.ChatExtra{GroupExtra: &line.GroupExtra{MemberMids: members, InviteeMids: invitees}}}
}
func list171(member, pending bool) *line.GetAllChatMidsResponse {
	l := &line.GetAllChatMidsResponse{}
	if member {
		l.MemberChatMids = []string{"Cgroup_fake-171"}
	}
	if pending {
		l.InvitedChatMids = []string{"Cgroup_fake-171"}
	}
	return l
}
func TestDIVAGroupCurrentChromeOpaqueIDsAndStates(t *testing.T) {
	for _, tc := range []struct {
		lm, lp, cm, cp bool
		want           divaGroupState
	}{
		{true, false, true, false, divaGroupJoined}, {false, true, false, true, divaGroupPending},
		{false, false, false, false, divaGroupLeft}, {true, true, true, false, divaGroupUnknown},
		{true, false, false, true, divaGroupUnknown}, {false, true, true, false, divaGroupUnknown},
		{false, false, true, true, divaGroupUnknown}, {false, false, true, false, divaGroupJoined},
		{false, false, false, true, divaGroupUnknown},
	} {
		c := group171(tc.cm, tc.cp)
		s, _ := classifyDIVAGroup(list171(tc.lm, tc.lp), &c, "Uself_171", c.ChatMid)
		if s != tc.want {
			t.Fatalf("%+v: got %s", tc, s)
		}
	}
	for _, gid := range []string{"Cabc-def_171", "cabc_def-171", "Rroom_171", "Uuser_171", "Cbad/171", "Cbad 171"} {
		want := gid == "Cabc-def_171" || gid == "cabc_def-171"
		if isDIVAGroupID(gid) != want {
			t.Fatalf("wrong group boundary for %q", gid)
		}
	}
}
func TestDIVAP4ReconciliationBranches(t *testing.T) {
	joined := group171(true, false)
	pending := group171(false, true)
	left := group171(false, false)
	type snapshot struct {
		list    *line.GetAllChatMidsResponse
		chat    line.Chat
		failure bool
	}
	for _, tc := range []struct {
		name           string
		seq            []snapshot
		want           divaGroupState
		accepts, reads int
	}{
		{"automatic", []snapshot{{list171(true, false), joined, false}}, divaGroupJoined, 0, 1},
		{"manual_accept", []snapshot{{list171(true, false), joined, false}}, divaGroupJoined, 0, 1},
		{"explicit_accept", []snapshot{{list171(false, true), pending, false}, {list171(true, false), joined, false}}, divaGroupJoined, 1, 2},
		{"accept_unconfirmed", []snapshot{{list171(false, true), pending, false}}, divaGroupPending, 1, 3},
		{"detail_before_list", []snapshot{{list171(false, false), joined, false}}, divaGroupJoined, 0, 2},
		{"old_portal_or_left", []snapshot{{list171(false, false), left, false}}, divaGroupLeft, 0, 3},
		{"other_invitee", []snapshot{{list171(false, false), pending, false}}, divaGroupUnknown, 0, 3},
		{"contradiction", []snapshot{{list171(true, false), pending, false}}, divaGroupUnknown, 0, 3},
		{"conflict_resolves", []snapshot{{list171(true, false), pending, false}, {list171(true, false), joined, false}}, divaGroupJoined, 0, 2},
		{"failure", []snapshot{{nil, line.Chat{}, true}}, divaGroupUnknown, 0, 3},
		{"failure_resolves", []snapshot{{nil, line.Chat{}, true}, {list171(true, false), joined, false}}, divaGroupJoined, 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads, accepts := 0, 0
			current := snapshot{}
			reader := divaGroupReader{
				mids: func(context.Context) (*line.GetAllChatMidsResponse, error) {
					current = tc.seq[min(reads, len(tc.seq)-1)]
					reads++
					if current.failure {
						return nil, errors.New("private")
					}
					return current.list, nil
				},
				chats: func(context.Context, []string) (*line.GetChatsResponse, error) {
					return &line.GetChatsResponse{Chats: []line.Chat{current.chat}}, nil
				},
			}
			state, accepted, _ := reconcileDIVAP4(context.Background(), reader, "Uself_171", "Cgroup_fake-171", func() error { accepts++; return nil }, func(context.Context) error { return nil })
			if state != tc.want || accepts != tc.accepts || reads != tc.reads || accepted != (accepts > 0) {
				t.Fatalf("state=%s accepts=%d reads=%d accepted=%t", state, accepts, reads, accepted)
			}
		})
	}
}
func TestDIVAP4BaselineDuplicateOtherAndReconnect(t *testing.T) {
	if !divaP4NewJoin(true, false, false, divaGroupJoined) {
		t.Fatal("first join with empty initialized baseline lost")
	}
	if !divaP4NewJoin(false, false, true, divaGroupJoined) {
		t.Fatal("confirmed explicit accept lost")
	}
	for _, tc := range []struct {
		ready, already, accepted bool
		state                    divaGroupState
	}{
		{true, true, false, divaGroupJoined}, // other person, duplicate op, reconnect
		{false, false, false, divaGroupJoined},
		{true, false, false, divaGroupUnknown}, {true, false, false, divaGroupLeft},
	} {
		if divaP4NewJoin(tc.ready, tc.already, tc.accepted, tc.state) {
			t.Fatal("unverified/new join inferred")
		}
	}
	lc := &LineClient{Mid: "Uself_171"}
	lc.setKnownMemberChatMIDs([]string{"Cgroup_fake-171"})
	lc.removeGroupMemberFromCache("Cgroup_fake-171", "Uother_171")
	if len(lc.getKnownMemberChatMIDs()) != 1 {
		t.Fatal("other member removal erased self")
	}
	lc.removeGroupMemberFromCache("Cgroup_fake-171", "Uself_171")
	if len(lc.getKnownMemberChatMIDs()) != 0 || !lc.knownMemberChatMIDsReady {
		t.Fatal("self removal left stale joined cache")
	}
}
func TestDIVAOwnerLGUsesFreshListAndRejectsConflict(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		groups, err := readDIVAOwnerGroups(context.Background(), divaOwnerGroupReader{
			self: "Uself_171",
			mids: func(context.Context) (*line.GetAllChatMidsResponse, error) { return list171(true, false), nil },
			chats: func(context.Context, []string) (*line.GetChatsResponse, error) {
				chat := group171(!conflict, conflict)
				chat.ChatName = "group"
				return &line.GetChatsResponse{Chats: []line.Chat{chat}}, nil
			},
		})
		if conflict {
			if err == nil {
				t.Fatal("contradictory LG succeeded")
			}
		} else if err != nil || len(groups) != 1 || groups[0].GID != "Cgroup_fake-171" {
			t.Fatalf("opaque group missing: %v %v", groups, err)
		}
	}
}
