package line

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestChromeProfileCoverUploadAssociateThenFreshRead(t *testing.T) {
	client := NewClient("main-token")
	calls := []string{}
	var oid string
	client.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.Path)
		if req.URL.Host != "line-chrome-gw.line-apps.com" ||
			req.Header.Get("X-Line-Access") != "main-token" ||
			req.Header.Get("X-Line-Chrome-Version") != ExtensionVersion {
			t.Fatal("wrong gateway credentials")
		}
		switch {
		case strings.HasSuffix(req.URL.Path, "/TalkService/getProfile"):
			return obsResponse(200, `{"code":0,"data":{"mid":"USelf","regionCode":"JP"}}`), nil
		case strings.HasSuffix(req.URL.Path, "/ChannelService/issueChannelToken"):
			if req.Header.Get("X-Hmac") == "" {
				t.Fatal("channel token request unsigned")
			}
			return obsResponse(200, `{"code":0,"data":{"channelAccessToken":"channel-token"}}`), nil
		case req.URL.Path == "/api/timeline/updateCover":
			if req.Method != http.MethodPost ||
				req.Header.Get("X-Hmac") == "" ||
				req.Header.Get("X-Line-ChannelToken") != "channel-token" {
				t.Fatal("cover association not signed/scoped")
			}
			body, _ := io.ReadAll(req.Body)
			var m map[string]any
			_ = json.Unmarshal(body, &m)
			if m["myMid"] != "USelf" || m["myRegion"] != "JP" ||
				m["coverObjectId"] != oid || m["storyShare"] != false {
				t.Fatal("cover association does not match upload")
			}
			return obsResponse(200, `{"message":"OK","data":{}}`), nil
		case req.URL.Path == "/api/timeline/getCover":
			if req.Method != http.MethodGet || req.URL.Query().Get("targetMid") != "USelf" ||
				req.URL.Query().Get("myMid") != "USelf" || req.URL.Query().Get("myRegion") != "JP" ||
				req.Header.Get("X-Hmac") == "" || req.Header.Get("X-Line-ChannelToken") != "channel-token" {
				t.Fatal("cover read was not fresh self-scoped")
			}
			return obsResponse(200, `{"message":"OK","data":{"homeId":"USelf","coverObsInfo":{"objectId":"`+oid+`","serviceName":"myhome","obsNamespace":"c"}}}`), nil
		default:
			t.Fatalf("unexpected Chrome gateway request %s", req.URL.Path)
			return nil, nil
		}
	})}
	client.OBSClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.Path)
		if req.URL.Host != "obs.line-apps.com" ||
			!strings.HasPrefix(req.URL.Path, "/r/myhome/c/") ||
			req.Header.Get("X-Line-ChannelToken") != "channel-token" ||
			req.Header.Get("X-Line-Access") != "" || req.Header.Get("Content-Type") != "" {
			t.Fatal("myhome must use Chrome OBS channel credentials")
		}
		oid = strings.TrimPrefix(req.URL.Path, "/r/myhome/c/")
		p, err := base64.StdEncoding.DecodeString(req.Header.Get("X-Obs-Params"))
		if err != nil || !bytes.Contains(p, []byte(`"ver":"2.0"`)) {
			t.Fatal("missing Chrome OBS params")
		}
		body, _ := io.ReadAll(req.Body)
		if !bytes.Equal(body, encodeTestImage(t, "jpeg", 8, 8)) {
			t.Fatal("cover upload bytes changed")
		}
		return obsResponse(200, `{"ok":true}`), nil
	})}
	jpeg := encodeTestImage(t, "jpeg", 8, 8)
	uploaded, err := client.UploadProfileCoverContext(context.Background(), "USelf", jpeg)
	if err != nil || uploaded == nil || uploaded.ObjectID == "" || uploaded.ObjectID != oid {
		t.Fatalf("upload+association failed: %v", err)
	}
	confirmed, err := client.ReadProfileCoverContext(context.Background(), "USelf")
	if err != nil || confirmed != oid {
		t.Fatalf("fresh self cover read did not match: %v", err)
	}
	if len(calls) != 7 {
		t.Fatalf("unexpected extra calls %v", calls)
	}
}

func TestChromeCoverRejectsWrongSelfAndUnprovenReadback(t *testing.T) {
	for _, raw := range []string{
		`{}`,
		`{"coverObsInfo":{"objectId":"oid"},"homeId":"other"}`,
		`{"coverObsInfo":{"objectId":"../secret"},"homeId":"USelf"}`,
		`{"coverObsInfo":{"objectId":"oid","serviceName":"talk"}}`,
		`{"coverObsInfo":{}}`,
	} {
		if _, err := parseProfileCover([]byte(raw), "USelf"); err == nil {
			t.Fatalf("accepted unverified cover schema: %s", raw)
		}
	}
	if id, err := parseProfileCover([]byte(`{"coverObsInfo":{"objectId":"safe_oid"}}`), "USelf"); err != nil || id != "safe_oid" {
		t.Fatal("Chrome self-scoped response with object ID should be readable")
	}
	client := NewClient("main-token")
	client.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(req.URL.Path, "/TalkService/getProfile") {
			t.Fatal("must stop before upload")
		}
		return obsResponse(200, `{"code":0,"data":{"mid":"UOther","regionCode":"JP"}}`), nil
	})}
	client.OBSClient = noRPCClient(t)
	_, err := client.UploadProfileCoverContext(context.Background(), "USelf", encodeTestImage(t, "jpeg", 8, 8))
	if outcome, _ := ProfileWriteOutcomeOf(err); outcome != ProfileWriteNotSent {
		t.Fatalf("wrong self must not upload, outcome=%s", outcome)
	}
}

func TestChromeCoverUploadFailureStopsBeforeAssociation(t *testing.T) {
	for _, status := range []int{401, 403, 500, 307} {
		client := NewClient("main-token")
		client.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case strings.HasSuffix(req.URL.Path, "/TalkService/getProfile"):
				return obsResponse(200, `{"code":0,"data":{"mid":"USelf","regionCode":"JP"}}`), nil
			case strings.HasSuffix(req.URL.Path, "/ChannelService/issueChannelToken"):
				return obsResponse(200, `{"code":0,"data":{"channelAccessToken":"channel-token"}}`), nil
			default:
				t.Fatal("failed OBS upload must never associate cover")
				return nil, nil
			}
		})}
		count := 0
		client.OBSClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			count++
			r := obsResponse(status, "")
			if status == 307 {
				r.Header.Set("Location", "https://invalid.example")
			}
			return r, nil
		})}
		_, err := client.UploadProfileCoverContext(context.Background(), "USelf", encodeTestImage(t, "jpeg", 8, 8))
		outcome, _ := ProfileWriteOutcomeOf(err)
		if count != 1 || err == nil {
			t.Fatalf("must stop after one upload: %v", err)
		}
		if status == 401 || status == 403 {
			if outcome != ProfileWriteRejected {
				t.Fatal("auth rejection not classified")
			}
		} else if outcome != ProfileWriteUnknown {
			t.Fatal("uncertain upload not classified unknown")
		}
	}
}

func TestChromeCoverInvalidImageNeverSendsNetwork(t *testing.T) {
	client := NewClient("main-token")
	client.HTTPClient, client.OBSClient = noRPCClient(t), noRPCClient(t)
	_, err := client.UploadProfileCoverContext(context.Background(), "USelf", bytes.Repeat([]byte("x"), 10))
	if outcome, _ := ProfileWriteOutcomeOf(err); outcome != ProfileWriteNotSent {
		t.Fatal("invalid image sent")
	}
}
