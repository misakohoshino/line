package connector

import (
	"testing"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

func TestDIVAP4Membership(t *testing.T) {
	mids := &line.GetAllChatMidsResponse{
		MemberChatMids:  []string{"Cjoined"},
		InvitedChatMids: []string{"Cpending", "Cjoined"},
	}
	for _, tc := range []struct {
		group           string
		joined, pending bool
	}{
		{"Cjoined", true, false},
		{"Cpending", false, true},
		{"CsomeoneElse", false, false},
	} {
		member, invited := divaP4Membership(mids, tc.group)
		if member != tc.joined || invited != tc.pending {
			t.Errorf("group %s: got (%t,%t), want (%t,%t)", tc.group, member, invited, tc.joined, tc.pending)
		}
	}
	joined, pending := divaP4Membership(nil, "Cunknown")
	if joined || pending {
		t.Fatal("nil membership must never permit acceptance")
	}
}

func TestDIVAP4EventIDStablePerGroup(t *testing.T) {
	a := divaP4EventID("Cgroup-one")
	b := divaP4EventID("Cgroup-one")
	c := divaP4EventID("Cgroup-two")
	if a != b || a == c {
		t.Fatalf("group ledger key must be stable and group-specific: %q %q %q", a, b, c)
	}
	if len(a) < 5 || a[:3] != "p4/" {
		t.Fatalf("unexpected P4 event id: %q", a)
	}
}
