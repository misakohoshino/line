package connector

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/rs/zerolog"
)

// captureBridgeLog sends the bridge log of env to a buffer and returns a
// function that parses every JSON line written so far.
func captureBridgeLog(env *sendTestEnv) func() []map[string]any {
	var buf bytes.Buffer
	env.lc.UserLogin.Bridge.Log = zerolog.New(&buf).Level(zerolog.DebugLevel)
	return func() []map[string]any {
		var lines []map[string]any
		sc := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
		for sc.Scan() {
			var line map[string]any
			if json.Unmarshal(sc.Bytes(), &line) == nil {
				lines = append(lines, line)
			}
		}
		return lines
	}
}

func logLine(t *testing.T, lines []map[string]any, msg string) map[string]any {
	t.Helper()
	var found map[string]any
	for _, line := range lines {
		if line["message"] == msg {
			found = line
		}
	}
	if found == nil {
		t.Fatalf("no %q log line in %v", msg, lines)
	}
	return found
}

func requireLogField(t *testing.T, line map[string]any, field string, want any) {
	t.Helper()
	got, ok := line[field]
	if !ok {
		t.Fatalf("%q missing from %v", field, line)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%s = %v, want %v (line %v)", field, got, want, line)
	}
}

func TestMediaFlowDiagnosticLogs(t *testing.T) {
	env := newSendTestEnv(t, true)
	logs := captureBridgeLog(env)
	instance := fmt.Sprintf("%p", env.lc)

	// Learned line carries the client identity and cache size.
	env.lc.observeInboundMediaFlow(plainInboundImage("960", sendTestGroup), sendTestGroup, divaOriginLive)
	learned := logLine(t, logs(), "Learned plain media flow from inbound image")
	requireLogField(t, learned, "client_instance", instance)
	requireLogField(t, learned, "login_id", sendTestSelf)
	requireLogField(t, learned, "chat_mid", sendTestGroup)
	requireLogField(t, learned, "content_type", 1)
	requireLogField(t, learned, "flow", 1)
	requireLogField(t, learned, "cache_entry_count", 1)

	// A cache hit logs the lookup and still returns plain.
	if env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage)) {
		t.Fatal("learned plain flow no longer used")
	}
	hit := logLine(t, logs(), "Media flow cache lookup")
	requireLogField(t, hit, "client_instance", instance)
	requireLogField(t, hit, "login_id", sendTestSelf)
	requireLogField(t, hit, "chat_mid", sendTestGroup)
	requireLogField(t, hit, "content_type", 1)
	requireLogField(t, hit, "cache_present", true)
	requireLogField(t, hit, "cache_valid", true)
	requireLogField(t, hit, "flow_present", true)
	requireLogField(t, hit, "flow_value", 1)
	requireLogField(t, hit, "learned_only", true)
	requireLogField(t, hit, "decision", "cache_flow")
	if ms, ok := hit["ttl_ms_remaining"].(float64); !ok || ms <= 0 || ms > float64(defaultMediaFlowTTL.Milliseconds()) {
		t.Fatalf("ttl_ms_remaining = %v", hit["ttl_ms_remaining"])
	}
	if n := countMethod(env.fake, "determineMediaMessageFlow"); n != 0 {
		t.Fatalf("determineMediaMessageFlow called %d times", n)
	}

	// A learned-only entry asked for another type: the server answer replaces
	// it, and the stored line says so.
	if !env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentVideo)) {
		t.Fatal("server video flow 2 → want E2EE")
	}
	ask := logLine(t, logs(), "Media flow cache lookup")
	requireLogField(t, ask, "decision", "ask_server")
	requireLogField(t, ask, "flow_present", false)
	if _, ok := ask["flow_value"]; ok {
		t.Fatalf("flow_value logged without a flow: %v", ask)
	}
	stored := logLine(t, logs(), "Stored media flow from server")
	requireLogField(t, stored, "client_instance", instance)
	requireLogField(t, stored, "login_id", sendTestSelf)
	requireLogField(t, stored, "content_type", int(ContentVideo))
	requireLogField(t, stored, "flow_value", 2)
	requireLogField(t, stored, "ttl_ms", 60000)
	requireLogField(t, stored, "replaced_entry", true)
	requireLogField(t, stored, "replaced_learned_only", true)
}

func TestMediaFlowDiagnosticLogsOnServerFailure(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.fake.mediaFlowStatus = 500
	env.fake.mediaFlowBody = lineDetermineFlow99999
	logs := captureBridgeLog(env)
	instance := fmt.Sprintf("%p", env.lc)

	if !env.lc.shouldUseE2EEMediaFlow(sendTestGroup, int(ContentImage)) {
		t.Fatal("failure without evidence must keep the E2EE default")
	}
	lines := logs()
	lookup := logLine(t, lines, "Media flow cache lookup")
	requireLogField(t, lookup, "client_instance", instance)
	requireLogField(t, lookup, "cache_present", false)
	requireLogField(t, lookup, "cache_valid", false)
	requireLogField(t, lookup, "cache_entry_count", 0)
	requireLogField(t, lookup, "decision", "ask_server")
	if _, ok := lookup["ttl_ms_remaining"]; ok {
		t.Fatalf("ttl_ms_remaining logged without a cache entry: %v", lookup)
	}
	failed := logLine(t, lines, "Failed to determine media flow, defaulting to E2EE upload")
	requireLogField(t, failed, "client_instance", instance)
	requireLogField(t, failed, "login_id", sendTestSelf)
	requireLogField(t, failed, "chat_mid", sendTestGroup)
	requireLogField(t, failed, "content_type", 1)
	if failed["level"] != "warn" {
		t.Fatalf("level = %v, want warn", failed["level"])
	}
}
