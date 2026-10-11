package line

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestProfileCoverInspectOnlyReadsSelfAndSanitizesResult(t *testing.T) {
	client := NewClient("main-token")
	var calls []string
	client.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.Method+" "+req.URL.Path)
		switch {
		case strings.HasSuffix(req.URL.Path, "/TalkService/getProfile"):
			return obsResponse(200, `{"code":0,"data":{"mid":"USelf","regionCode":"JP"}}`), nil
		case strings.HasSuffix(req.URL.Path, "/ChannelService/issueChannelToken"):
			return obsResponse(200, `{"code":0,"data":{"channelAccessToken":"channel-token"}}`), nil
		case req.URL.Path == "/api/timeline/getCover":
			if req.Method != http.MethodGet ||
				req.URL.Query().Get("targetMid") != "USelf" ||
				req.URL.Query().Get("myMid") != "USelf" ||
				req.Header.Get("X-Hmac") == "" ||
				req.Header.Get("X-Line-ChannelToken") != "channel-token" {
				t.Fatal("cover inspection did not use signed self-only GET")
			}
			return obsResponse(200, `{"message":"OK","data":{"homeId":"USelf","coverObsInfo":{"oid":"private-oid","sid":"c"}}}`), nil
		default:
			t.Fatalf("unexpected method or endpoint %s %s", req.Method, req.URL.Path)
			return nil, nil
		}
	})}
	client.OBSClient = noRPCClient(t)
	info, err := client.InspectProfileCoverContext(context.Background(), "USelf")
	if err != nil || !info.HasCoverInfo || !info.HasOID || !info.HasSID || !info.HasHomeID || !info.ParsedSelfCover {
		t.Fatalf("cover schema not safely recognized: %+v, %v", info, err)
	}
	if len(calls) != 3 {
		t.Fatalf("expected exactly 3 read/auth calls, got %v", calls)
	}
	for _, entry := range calls {
		if strings.Contains(entry, "updateCover") || strings.Contains(entry, "upload") {
			t.Fatalf("inspection unexpectedly writes profile: %s", entry)
		}
	}
}

func TestProfileCoverInspectWrongSelfNeverRequestsChannelOrCover(t *testing.T) {
	client := NewClient("main-token")
	count := 0
	client.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		count++
		if !strings.HasSuffix(req.URL.Path, "/TalkService/getProfile") {
			t.Fatal("wrong self continued into cover")
		}
		return obsResponse(200, `{"code":0,"data":{"mid":"UOther","regionCode":"JP"}}`), nil
	})}
	client.OBSClient = noRPCClient(t)
	_, err := client.InspectProfileCoverContext(context.Background(), "USelf")
	if err == nil || count != 1 {
		t.Fatalf("cross-account probe was not refused: requests=%d, err=%v", count, err)
	}
}
