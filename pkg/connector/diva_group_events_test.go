package connector

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

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

func TestDIVAV2LiveGroupEventAdditionalShapes(t *testing.T) {
	lc := newDIVAV2TestClient(io.Discard)

	t.Run("normal leave shape", func(t *testing.T) {
		event, ok := lc.buildDIVAV2LiveGroupEvent(line.Operation{
			Revision: "704", Type: int(OpNotifiedLeaveChat),
			Param1: "cgroup", Param2: "udriver",
			CreatedTime: "1727654324000",
		})
		if !ok || event == nil {
			t.Fatal("expected normal leave shape")
		}
		if event.Chat.ID != "cgroup" || event.GroupEvent.Member.Mid != "udriver" ||
			event.GroupEvent.Actor == nil || event.GroupEvent.Actor.Mid != "udriver" {
			t.Fatalf("unexpected leave event: %+v", event)
		}
	})

	t.Run("own member marks is_from_me", func(t *testing.T) {
		event, ok := lc.buildDIVAV2LiveGroupEvent(line.Operation{
			Revision: "705", Type: int(OpNotifiedJoinChat),
			Param1: "cgroup", Param2: divaTestAccount,
		})
		if !ok || event == nil || !event.GroupEvent.Member.IsFromMe {
			t.Fatalf("own member event = %+v ok=%v", event, ok)
		}
	})

	t.Run("room chat type", func(t *testing.T) {
		event, ok := lc.buildDIVAV2LiveGroupEvent(line.Operation{
			Revision: "706", Type: int(OpNotifiedJoinChat),
			Param1: "rroom", Param2: "udriver",
		})
		if !ok || event == nil || event.Chat.Type != "room" {
			t.Fatalf("room event = %+v ok=%v", event, ok)
		}
	})
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

// errDIVATestPortalStore is returned by the test-only Matrix portal store. It
// makes bridgev2.QueueRemoteEvent stop at its portal lookup and log, so the
// real handleOperation membership branches run without a homeserver.
var errDIVATestPortalStore = errors.New("diva test: matrix portal store unavailable")

type divaUnavailableConnector struct{}

func (divaUnavailableConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errDIVATestPortalStore
}

func (divaUnavailableConnector) Driver() driver.Driver { return divaUnavailableDriver{} }

type divaUnavailableDriver struct{}

func (divaUnavailableDriver) Open(string) (driver.Conn, error) { return nil, errDIVATestPortalStore }

// divaMatrixQueueLog is what bridgev2.QueueRemoteEvent logs when the portal
// lookup fails. Each occurrence proves one Matrix/Beeper membership event was
// still handed to the bridge.
const divaMatrixQueueLog = "Failed to get portal to handle remote event"

type divaGroupOpHarness struct {
	lc       *LineClient
	logs     *divaSyncBuffer
	received <-chan []byte
	now      time.Time
}

func newDIVAGroupOpHarness(t *testing.T) *divaGroupOpHarness {
	t.Helper()
	received := startDIVATestWorker(t)
	t.Setenv("DIVA_CONTRACT_VERSION", "2")

	rawDB, err := dbutil.NewWithDB(sql.OpenDB(divaUnavailableConnector{}), "sqlite3")
	if err != nil {
		t.Fatalf("test portal store: %v", err)
	}
	logs := &divaSyncBuffer{}
	logger := zerolog.New(logs).Level(zerolog.DebugLevel)
	br := &bridgev2.Bridge{
		ID:            "diva-test",
		Log:           logger,
		Config:        &bridgeconfig.BridgeConfig{},
		DB:            database.New("diva-test", database.MetaTypes{}, rawDB),
		BackgroundCtx: context.Background(),
	}
	h := &divaGroupOpHarness{
		lc: &LineClient{
			Mid: divaTestAccount,
			UserLogin: &bridgev2.UserLogin{
				UserLogin: &database.UserLogin{ID: networkid.UserLoginID(divaTestAccount)},
				Bridge:    br,
				Log:       logger,
			},
		},
		logs:     logs,
		received: received,
		now:      time.Unix(1_790_000_000, 0),
	}
	previousNow := divaGroupEventNow
	divaGroupEventNow = func() time.Time { return h.now }
	t.Cleanup(func() { divaGroupEventNow = previousNow })
	return h
}

// op runs one SSE operation through the real handleOperation dispatcher.
func (h *divaGroupOpHarness) op(opType OperationType, revision, p1, p2, p3 string) {
	h.lc.handleOperation(context.Background(), line.Operation{
		Revision: json.Number(revision), Type: int(opType),
		Param1: p1, Param2: p2, Param3: p3,
		CreatedTime: "1727654330000",
	})
}

func (h *divaGroupOpHarness) advance(d time.Duration) { h.now = h.now.Add(d) }

func (h *divaGroupOpHarness) matrixEvents() int {
	return strings.Count(h.logs.String(), divaMatrixQueueLog)
}

// expectDIVA waits for exactly the wanted events. Delivery runs in one
// goroutine per event, so arrival order is not guaranteed and is not compared.
func (h *divaGroupOpHarness) expectDIVA(t *testing.T, want ...string) {
	t.Helper()
	got := make([]string, 0, len(want))
	for range want {
		select {
		case body := <-h.received:
			got = append(got, summarizeDIVAGroupEvent(t, body))
		case <-time.After(2 * time.Second):
			t.Fatalf("DIVA received %d events %v, want %v", len(got), got, want)
		}
	}
	select {
	case body := <-h.received:
		t.Fatalf("unexpected extra DIVA event %s; already got %v", summarizeDIVAGroupEvent(t, body), got)
	case <-time.After(250 * time.Millisecond):
	}
	sort.Strings(got)
	sorted := append([]string(nil), want...)
	sort.Strings(sorted)
	if !slices.Equal(got, sorted) {
		t.Fatalf("DIVA events = %v, want %v", got, sorted)
	}
}

// summarizeDIVAGroupEvent renders "<event_id> <type> <chat> <member>[*] actor=<actor>[*]"
// where * marks is_from_me=true.
func summarizeDIVAGroupEvent(t *testing.T, body []byte) string {
	t.Helper()
	var got struct {
		EventID    string              `json:"event_id"`
		EventType  string              `json:"event_type"`
		Chat       struct{ ID string } `json:"chat"`
		GroupEvent struct {
			Type   string             `json:"type"`
			Member divaV2GroupMember  `json:"member"`
			Actor  *divaV2GroupMember `json:"actor"`
		} `json:"group_event"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("worker received invalid JSON: %v", err)
	}
	if got.EventType != "group_event" {
		t.Fatalf("unexpected event_type in %s", body)
	}
	mark := func(m divaV2GroupMember) string {
		if m.IsFromMe {
			return m.Mid + "*"
		}
		return m.Mid
	}
	actor := ""
	if got.GroupEvent.Actor != nil {
		actor = mark(*got.GroupEvent.Actor)
	}
	return fmt.Sprintf("%s %s %s %s actor=%s",
		got.EventID, got.GroupEvent.Type, got.Chat.ID, mark(got.GroupEvent.Member), actor)
}

func TestDIVAGroupOperationJoinForwardsOnce(t *testing.T) {
	h := newDIVAGroupOpHarness(t)
	h.op(OpNotifiedJoinChat, "54124", "cgroup", "udriver", "")
	h.expectDIVA(t, "line:op:54124 member_joined cgroup udriver actor=")
	if got := h.matrixEvents(); got != 1 {
		t.Fatalf("matrix membership events = %d, want 1", got)
	}
}

func TestDIVAGroupOperationGenuineSelfLeaveForwardsOnce(t *testing.T) {
	h := newDIVAGroupOpHarness(t)
	h.op(OpNotifiedLeaveChat, "54200", "cgroup", "udriver", "")
	h.expectDIVA(t, "line:op:54200 member_left cgroup udriver actor=udriver")
	if strings.Contains(h.logs.String(), "suppressed trailing member_left") {
		t.Fatalf("genuine leave was logged as suppressed: %s", h.logs.String())
	}
}

func TestDIVAGroupOperationKickSuppressesTrailingLeave(t *testing.T) {
	h := newDIVAGroupOpHarness(t)
	// Production order for one kick: op 133 then op 61, same chat/member.
	h.op(OpNotifiedDeleteOtherFromChat, "54126", "cgroup", "uadmin", "udriver")
	h.op(OpNotifiedLeaveChat, "54127", "cgroup", "udriver", "")
	h.expectDIVA(t, "line:op:54126 member_removed cgroup udriver actor=uadmin")

	logs := h.logs.String()
	if !strings.Contains(logs, "suppressed trailing member_left") ||
		!strings.Contains(logs, `"left_revision":"54127"`) ||
		!strings.Contains(logs, `"matched_removed_revision":"54126"`) {
		t.Fatalf("suppression log missing revisions: %s", logs)
	}
	if got := h.matrixEvents(); got != 2 {
		t.Fatalf("matrix membership events = %d, want 2 (op 133 and op 61)", got)
	}
}

func TestDIVAGroupOperationKickWithReversedLeaveShape(t *testing.T) {
	h := newDIVAGroupOpHarness(t)
	h.op(OpNotifiedDeleteOtherFromChat, "54126", "cgroup", "uadmin", "udriver")
	h.op(OpNotifiedLeaveChat, "54127", "udriver", "cgroup", "")
	h.expectDIVA(t, "line:op:54126 member_removed cgroup udriver actor=uadmin")
}

func TestDIVAGroupOperationDifferentMemberNotSuppressed(t *testing.T) {
	h := newDIVAGroupOpHarness(t)
	h.op(OpNotifiedDeleteOtherFromChat, "54300", "cgroup", "uadmin", "udriver")
	h.op(OpNotifiedLeaveChat, "54301", "cgroup", "uother", "")
	h.expectDIVA(t,
		"line:op:54300 member_removed cgroup udriver actor=uadmin",
		"line:op:54301 member_left cgroup uother actor=uother",
	)
}

func TestDIVAGroupOperationDifferentGroupNotSuppressed(t *testing.T) {
	h := newDIVAGroupOpHarness(t)
	h.op(OpNotifiedDeleteOtherFromChat, "54400", "cgroup1", "uadmin", "udriver")
	h.op(OpNotifiedLeaveChat, "54401", "cgroup2", "udriver", "")
	h.expectDIVA(t,
		"line:op:54400 member_removed cgroup1 udriver actor=uadmin",
		"line:op:54401 member_left cgroup2 udriver actor=udriver",
	)
}

func TestDIVAGroupOperationLeaveWindow(t *testing.T) {
	t.Run("inside window is suppressed", func(t *testing.T) {
		h := newDIVAGroupOpHarness(t)
		h.op(OpNotifiedDeleteOtherFromChat, "54500", "cgroup", "uadmin", "udriver")
		h.advance(divaGroupLeaveWindow - time.Millisecond)
		h.op(OpNotifiedLeaveChat, "54501", "cgroup", "udriver", "")
		h.expectDIVA(t, "line:op:54500 member_removed cgroup udriver actor=uadmin")
	})
	t.Run("after window is forwarded", func(t *testing.T) {
		h := newDIVAGroupOpHarness(t)
		h.op(OpNotifiedDeleteOtherFromChat, "54510", "cgroup", "uadmin", "udriver")
		h.advance(11 * time.Second)
		h.op(OpNotifiedLeaveChat, "54511", "cgroup", "udriver", "")
		h.expectDIVA(t,
			"line:op:54510 member_removed cgroup udriver actor=uadmin",
			"line:op:54511 member_left cgroup udriver actor=udriver",
		)
	})
}

func TestDIVAGroupOperationRejoinClearsKickMarker(t *testing.T) {
	h := newDIVAGroupOpHarness(t)
	h.op(OpNotifiedDeleteOtherFromChat, "54600", "cgroup", "uadmin", "udriver")
	h.op(OpNotifiedJoinChat, "54601", "cgroup", "udriver", "")
	h.op(OpNotifiedLeaveChat, "54602", "cgroup", "udriver", "")
	h.expectDIVA(t,
		"line:op:54600 member_removed cgroup udriver actor=uadmin",
		"line:op:54601 member_joined cgroup udriver actor=",
		"line:op:54602 member_left cgroup udriver actor=udriver",
	)
}

func TestDIVAGroupOperationOwnAccountKicked(t *testing.T) {
	h := newDIVAGroupOpHarness(t)
	h.lc.groupMemberCache = map[string][]string{"cgroup": {divaTestAccount, "uadmin"}}
	h.op(OpNotifiedDeleteOtherFromChat, "54700", "cgroup", "uadmin", divaTestAccount)
	h.op(OpNotifiedLeaveChat, "54701", "cgroup", divaTestAccount, "")
	h.expectDIVA(t, "line:op:54700 member_removed cgroup "+divaTestAccount+"* actor=uadmin")

	// The suppressed op 61 still ran the upstream self-leave path.
	if got := h.matrixEvents(); got != 2 {
		t.Fatalf("matrix membership events = %d, want 2", got)
	}
	if members := h.lc.getCachedGroupMembers("cgroup"); len(members) != 0 {
		t.Fatalf("self leave did not clear the group cache: %v", members)
	}
}

func TestDIVAGroupOperationSuppressedLeaveStillRunsMatrixPath(t *testing.T) {
	h := newDIVAGroupOpHarness(t)
	h.lc.groupMemberCache = map[string][]string{"cgroup": {divaTestAccount, "uadmin", "udriver"}}
	h.op(OpNotifiedDeleteOtherFromChat, "54800", "cgroup", "uadmin", "udriver")
	if got := h.matrixEvents(); got != 1 {
		t.Fatalf("matrix membership events after op 133 = %d, want 1", got)
	}

	// Put the member back so the effect of handleMemberLeft on op 61 is
	// observable on its own, independent of the op 133 cache removal.
	h.lc.addGroupMembersToCache("cgroup", "udriver")
	h.op(OpNotifiedLeaveChat, "54801", "cgroup", "udriver", "")
	h.expectDIVA(t, "line:op:54800 member_removed cgroup udriver actor=uadmin")

	if got := h.matrixEvents(); got != 2 {
		t.Fatalf("matrix membership events after suppressed op 61 = %d, want 2", got)
	}
	if slices.Contains(h.lc.getCachedGroupMembers("cgroup"), "udriver") {
		t.Fatalf("suppressed op 61 skipped the Matrix member cache removal: %v", h.lc.getCachedGroupMembers("cgroup"))
	}
}

func TestDIVAGroupOperationReverseOrderForwardsBoth(t *testing.T) {
	h := newDIVAGroupOpHarness(t)
	h.op(OpNotifiedLeaveChat, "54900", "cgroup", "udriver", "")
	h.op(OpNotifiedDeleteOtherFromChat, "54901", "cgroup", "uadmin", "udriver")
	h.expectDIVA(t,
		"line:op:54900 member_left cgroup udriver actor=udriver",
		"line:op:54901 member_removed cgroup udriver actor=uadmin",
	)
	if !strings.Contains(h.logs.String(), "member_removed arrived after member_left; both forwarded") {
		t.Fatalf("reverse order warning missing: %s", h.logs.String())
	}
}
