package connector

import (
	"testing"
)

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
