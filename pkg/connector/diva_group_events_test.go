package connector

import (
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

func TestDIVAV2LiveGroupEventShapes(t *testing.T) {
	lc := newDIVAV2TestClient(io.Discard)

	tests := []struct {
		name       string
		op         line.Operation
		wantType   string
		wantChat   string
		wantMember string
		wantActor  string
	}{
		{
			name: "member joined",
			op: line.Operation{
				Revision: "701", Type: int(OpNotifiedJoinChat),
				Param1: "cgroup", Param2: "udriver",
				CreatedTime: "1727654321000",
			},
			wantType: divaGroupEventMemberJoined, wantChat: "cgroup", wantMember: "udriver",
		},
		{
			name: "member left with reversed param shape",
			op: line.Operation{
				Revision: "702", Type: int(OpNotifiedLeaveChat),
				Param1: "udriver", Param2: "cgroup",
				CreatedTime: "1727654322000",
			},
			wantType: divaGroupEventMemberLeft, wantChat: "cgroup", wantMember: "udriver", wantActor: "udriver",
		},
		{
			name: "member removed",
			op: line.Operation{
				Revision: "703", Type: int(OpNotifiedDeleteOtherFromChat),
				Param1: "cgroup", Param2: "uadmin", Param3: "udriver",
				CreatedTime: "1727654323000",
			},
			wantType: divaGroupEventMemberRemoved, wantChat: "cgroup", wantMember: "udriver", wantActor: "uadmin",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event, ok := lc.buildDIVAV2LiveGroupEvent(tc.op)
			if !ok || event == nil {
				t.Fatal("expected group event")
			}
			if event.Version != 2 || event.EventType != "group_event" || event.Origin != divaOriginLive {
				t.Fatalf("unexpected envelope: %+v", event)
			}
			if event.EventID != "line:op:"+tc.op.Revision.String() {
				t.Fatalf("event_id = %q", event.EventID)
			}
			if event.Chat.ID != tc.wantChat || event.Chat.Type != "group" {
				t.Fatalf("chat = %+v", event.Chat)
			}
			if event.GroupEvent.Type != tc.wantType || event.GroupEvent.Member.Mid != tc.wantMember {
				t.Fatalf("group_event = %+v", event.GroupEvent)
			}
			if event.GroupEvent.Member.IsFromMe {
				t.Fatalf("remote member incorrectly marked from_me: %+v", event.GroupEvent.Member)
			}
			gotActor := ""
			if event.GroupEvent.Actor != nil {
				gotActor = event.GroupEvent.Actor.Mid
			}
			if gotActor != tc.wantActor {
				t.Fatalf("actor = %q, want %q", gotActor, tc.wantActor)
			}
			if event.GroupEvent.CreatedAt == nil {
				t.Fatal("created_at missing")
			}
		})
	}
}

func TestDIVAV2LiveGroupEventRejectsUnstableOrUnrelatedOps(t *testing.T) {
	lc := newDIVAV2TestClient(io.Discard)

	for _, op := range []line.Operation{
		{Type: int(OpNotifiedJoinChat), Param1: "cgroup", Param2: "udriver"},
		{Revision: "9", Type: int(OpNotifiedJoinChat), Param1: "bad-chat", Param2: "udriver"},
		{Revision: "10", Type: int(OpNotifiedDeleteOtherFromChat), Param1: "cgroup", Param2: "not-user", Param3: "udriver"},
		{Revision: "11", Type: int(OpNotifiedInviteIntoChat), Param1: "cgroup", Param2: "udriver"},
	} {
		if event, ok := lc.buildDIVAV2LiveGroupEvent(op); ok || event != nil {
			t.Fatalf("unexpected group event for %+v: %+v", op, event)
		}
	}
}

func TestHandleDIVAGroupOperationForwardsV2Only(t *testing.T) {
	received := startDIVATestWorker(t)
	t.Setenv("DIVA_CONTRACT_VERSION", "2")
	lc := newDIVAV2TestClient(io.Discard)

	op := line.Operation{
		Revision: "801", Type: int(OpNotifiedDeleteOtherFromChat),
		Param1: "cgroup", Param2: "uadmin", Param3: "udriver",
		CreatedTime: "1727654330000",
	}
	lc.handleDIVAGroupOperation(op)

	select {
	case body := <-received:
		var got map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("worker received invalid JSON: %v", err)
		}
		if got["event_type"] != "group_event" || got["event_id"] != "line:op:801" || got["origin"] != "live" {
			t.Fatalf("unexpected group event envelope: %v", got)
		}
		if _, exists := got["message"]; exists {
			t.Fatalf("group event must not fake a message: %v", got)
		}
		groupEvent, _ := got["group_event"].(map[string]any)
		member, _ := groupEvent["member"].(map[string]any)
		actor, _ := groupEvent["actor"].(map[string]any)
		if groupEvent["type"] != "member_removed" || member["mid"] != "udriver" || actor["mid"] != "uadmin" {
			t.Fatalf("unexpected group event body: %v", groupEvent)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("v2 group event was not forwarded to DIVA worker")
	}

	t.Setenv("DIVA_CONTRACT_VERSION", "1")
	lc.handleDIVAGroupOperation(line.Operation{
		Revision: "802", Type: int(OpNotifiedJoinChat), Param1: "cgroup", Param2: "udriver",
	})
	select {
	case body := <-received:
		t.Fatalf("v1 unexpectedly received group event: %s", body)
	case <-time.After(250 * time.Millisecond):
	}
}
