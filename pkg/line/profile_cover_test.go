package line

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func profileApprovalClient(t *testing.T) *http.Client {
	t.Helper()
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		if req.URL.Host != "line-chrome-gw.line-apps.com" || !strings.HasSuffix(req.URL.Path, "/ChannelService/approveChannelAndIssueChannelToken") || string(body) != `["1341209850"]` || req.Header.Get("x-hmac") == "" || req.Header.Get("x-line-access") != "main-token" {
			t.Fatalf("unexpected approval request path/body/headers")
		}
		return obsResponse(200, `{"code":0,"data":{"channelAccessToken":"channel-token"}}`), nil
	})}
}

func TestProfileCoverUploadAssociationAndFreshRead(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		outcome ProfileWriteOutcome
	}{
		{"association accepted", 200, `{"code":0}`, ""},
		{"association rejected", 403, `{}`, ProfileWriteRejected},
		{"association unauthorized", 401, `{}`, ProfileWriteRejected},
		{"association server error", 500, `{}`, ProfileWriteUnknown},
		{"association missing code", 200, `{}`, ProfileWriteUnknown},
		{"association app rejected", 200, `{"code":1}`, ProfileWriteRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient("main-token")
			client.HTTPClient = profileApprovalClient(t)
			writes, reads := 0, 0
			var objectID string
			client.OBSClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Header.Get("X-Line-Access") != "main-token" || req.Header.Get("X-Line-ChannelToken") != "channel-token" || req.Header.Get("X-Line-Mid") != "USelf" {
					t.Fatal("mixed authorization")
				}
				switch req.URL.Host {
				case "gwz.line.naver.jp":
					if !strings.HasPrefix(req.URL.Path, "/oa/r/myhome/c/") || req.Method != "POST" || req.Header.Get("X-Talk-Meta") != "" {
						t.Fatal("wrong cover upload path")
					}
					objectID = strings.TrimPrefix(req.URL.Path, "/oa/r/myhome/c/")
					writes++
					resp := obsResponse(201, "")
					resp.Header.Set("x-obs-oid", objectID)
					resp.Header.Set("x-obs-hash", "hash")
					return resp, nil
				case "ga2.line.naver.jp":
					if req.URL.Path != "/hm/api/v1/home/cover.json" || req.Method != "POST" || req.Header.Get("X-LAP") != "5" || req.Header.Get("X-LPV") != "1" {
						t.Fatal("wrong association host or headers")
					}
					if req.Header.Get("X-LHM") == "GET" {
						reads++
						if req.URL.Query().Get("homeId") != "USelf" || req.Header.Get("Cache-Control") == "" {
							t.Fatal("not a fresh self read")
						}
						return obsResponse(200, `{"code":0,"result":{"homeId":"USelf","coverObsInfo":{"serviceName":"myhome","obsNamespace":"c","objectId":"`+objectID+`"}}}`), nil
					}
					raw, _ := io.ReadAll(req.Body)
					var data map[string]any
					_ = json.Unmarshal(raw, &data)
					if data["homeId"] != "USelf" || data["coverObjectId"] != objectID || data["storyShare"] != false {
						t.Fatal("association differs from uploaded object")
					}
					writes++
					return obsResponse(tc.status, tc.body), nil
				default:
					t.Fatalf("unexpected host %s", req.URL.Host)
					return nil, nil
				}
			})}
			jpeg := encodeTestImage(t, "jpeg", 8, 8)
			result, err := client.UploadProfileCoverContext(context.Background(), "USelf", jpeg)
			if writes != 2 || reads != 0 {
				t.Fatal("unexpected side effects or upload-only success")
			}
			if tc.outcome != "" {
				outcome, _ := ProfileWriteOutcomeOf(err)
				if outcome != tc.outcome {
					t.Fatalf("outcome=%s err=%v", outcome, err)
				}
				return
			}
			if err != nil || result.ObjectID != objectID {
				t.Fatalf("upload result: %v", err)
			}
			oid, err := client.ReadProfileCoverContext(context.Background(), "USelf")
			if err != nil || oid != objectID || reads != 1 {
				t.Fatal("fresh exact self read failed")
			}
		})
	}
}

func TestProfileCoverRejectsUnprovenSelfSchema(t *testing.T) {
	for _, raw := range []string{
		`{}`, `{"code":0,"result":{"coverObsInfo":{"serviceName":"myhome","obsNamespace":"c","objectId":"oid"}}}`,
		`{"code":0,"result":{"homeId":"UOther","coverObsInfo":{"serviceName":"myhome","obsNamespace":"c","objectId":"oid"}}}`,
		`{"code":0,"result":{"homeId":"USelf","coverObsInfo":{"serviceName":"talk","obsNamespace":"p","objectId":"oid"}}}`,
	} {
		if _, err := parseProfileCover([]byte(raw), "USelf"); err == nil {
			t.Fatal("unproven self accepted")
		}
	}
}

func TestProfileCandidateRedirectDoesNotLeakOrRetry(t *testing.T) {
	client := NewClient("main-token")
	client.HTTPClient = profileApprovalClient(t)
	calls := 0
	client.OBSClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls > 1 {
			t.Fatal("redirect followed")
		}
		r := obsResponse(307, "")
		r.Header.Set("Location", "https://example.invalid/steal")
		return r, nil
	})}
	_, err := client.UploadProfileImageContext(context.Background(), "USelf", encodeTestImage(t, "jpeg", 8, 8))
	if outcome, _ := ProfileWriteOutcomeOf(err); outcome != ProfileWriteUnknown || calls != 1 {
		t.Fatal("redirect must be unknown and never retried")
	}
}

func TestProfileCandidateUploadFailureNeverAssociates(t *testing.T) {
	for _, status := range []int{401, 403, 500} {
		client := NewClient("main-token")
		client.HTTPClient = profileApprovalClient(t)
		calls := 0
		client.OBSClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls++
			if req.URL.Host != "gwz.line.naver.jp" {
				t.Fatal("associated after failed upload")
			}
			return obsResponse(status, ""), nil
		})}
		_, err := client.UploadProfileCoverContext(context.Background(), "USelf", encodeTestImage(t, "jpeg", 8, 8))
		if err == nil || calls != 1 {
			t.Fatal("failed upload continued")
		}
	}
}

func TestProfileCoverInvalidImageHasNoApprovalOrUpload(t *testing.T) {
	client := NewClient("main-token")
	client.HTTPClient = noRPCClient(t)
	client.OBSClient = noRPCClient(t)
	_, err := client.UploadProfileCoverContext(context.Background(), "USelf", bytes.Repeat([]byte("x"), 10))
	if outcome, _ := ProfileWriteOutcomeOf(err); outcome != ProfileWriteNotSent {
		t.Fatal("invalid image sent")
	}
}
