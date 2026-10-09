package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

func TestDIVAOwnerJoinedMembershipNamesAndBatches(t *testing.T) {
	mids := []string{"c000", "rRoom", "uPerson", "c000"}
	for i := 1; i <= 45; i++ {
		mids = append(mids, fmt.Sprintf("c%03d", i))
	}
	calls := 0
	groups, err := readDIVAOwnerGroups(context.Background(), divaOwnerGroupReader{
		mids: func(context.Context) (*line.GetAllChatMidsResponse, error) {
			return &line.GetAllChatMidsResponse{MemberChatMids: mids, InvitedChatMids: []string{"cInvited"}}, nil
		},
		chats: func(_ context.Context, batch []string) (*line.GetChatsResponse, error) {
			calls++
			if len(batch) > 20 {
				t.Fatal("name batch exceeds 20")
			}
			if calls == 2 {
				return nil, errors.New("name lookup failed")
			}
			return &line.GetChatsResponse{Chats: []line.Chat{{ChatMid: batch[0], ChatName: "😀群"}, {ChatMid: "cInvited", ChatName: "not a member"}}}, nil
		},
	})
	if err != nil || len(groups) != 46 || calls != 3 {
		t.Fatalf("err=%v count=%d calls=%d", err, len(groups), calls)
	}
	for i, group := range groups {
		if group.GID != fmt.Sprintf("c%03d", i) {
			t.Fatalf("lost/deduped/wrong member at %d: %+v", i, group)
		}
	}
	if groups[0].Name != "😀群" || groups[20].Name != "" || groups[40].Name != "😀群" {
		t.Fatalf("name failures did not preserve member list: %+v", groups)
	}
}

func TestDIVAOwnerWholeMembershipFailureEmptyAndDeadline(t *testing.T) {
	for _, failure := range []bool{true, false} {
		groups, err := readDIVAOwnerGroups(context.Background(), divaOwnerGroupReader{
			mids: func(context.Context) (*line.GetAllChatMidsResponse, error) {
				if failure {
					return nil, errors.New("no list")
				}
				return &line.GetAllChatMidsResponse{}, nil
			},
			chats: func(context.Context, []string) (*line.GetChatsResponse, error) {
				t.Fatal("no name call expected")
				return nil, nil
			},
		})
		if failure && err == nil || !failure && (err != nil || groups == nil || len(groups) != 0) {
			t.Fatalf("failed membership must not mean empty success: groups=%v err=%v", groups, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	groups, err := readDIVAOwnerGroups(ctx, divaOwnerGroupReader{
		mids: func(context.Context) (*line.GetAllChatMidsResponse, error) {
			cancel()
			return &line.GetAllChatMidsResponse{MemberChatMids: []string{"cOne", "cTwo"}}, nil
		},
		chats: func(context.Context, []string) (*line.GetChatsResponse, error) {
			t.Fatal("deadline must stop name calls")
			return nil, nil
		},
	})
	if err != nil || len(groups) != 2 {
		t.Fatalf("complete membership must survive name deadline: %v %v", groups, err)
	}
}

func TestDIVAOwnerEndpointAuthAccountAndNoSend(t *testing.T) {
	calls := 0
	h := newDIVAControlHarness(t, divaControlConfig{
		listOwnerGroups: func(ctx context.Context, lc *LineClient) ([]divaJoinedGroup, error) {
			calls++
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing query deadline")
			}
			return []divaJoinedGroup{{GID: "cJoined", Name: "Group"}}, nil
		},
		send: func(context.Context, *LineClient, *lineOutboundRequest) (*lineOutboundResult, error) {
			t.Fatal("read query must never send")
			return nil, nil
		},
	})
	account := h.env.lc.midOrFallback()
	for _, tc := range []struct {
		method, query, token string
		status               int
	}{
		{http.MethodGet, "?account_mid=" + account, "", 401},
		{http.MethodGet, "", divaTestToken, 400},
		{http.MethodGet, "?account_mid=" + account + "&account_mid=" + account, divaTestToken, 400},
		{http.MethodGet, "?account_mid=" + account + "&rpc=leaveGroup", divaTestToken, 400},
		{http.MethodGet, "?account_mid=%zz", divaTestToken, 400},
		{http.MethodGet, "?account_mid=uMissing", divaTestToken, 503},
		{http.MethodPost, "?account_mid=" + account, divaTestToken, 405},
		{http.MethodGet, "?account_mid=" + account, divaTestToken, 200},
	} {
		r := httptest.NewRequest(tc.method, "/diva/v1/owner/joined-groups"+tc.query, nil)
		if tc.token != "" {
			r.Header.Set("Authorization", "Bearer "+tc.token)
		}
		w := httptest.NewRecorder()
		h.s.handler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("query=%s status=%d body=%s", tc.query, w.Code, w.Body.String())
		}
		if w.Code == 200 {
			var data struct {
				Complete bool              `json:"complete"`
				Total    int               `json:"total"`
				Groups   []divaJoinedGroup `json:"groups"`
			}
			if json.Unmarshal(w.Body.Bytes(), &data) != nil || !data.Complete || data.Total != 1 || data.Groups[0].GID != "cJoined" {
				t.Fatalf("bad response: %s", w.Body.String())
			}
		}
	}
	if calls != 1 {
		t.Fatalf("unauthorized/invalid queries reached LINE: %d", calls)
	}
	// Overload is bounded, does not wait or start another RPC.
	h.s.ownerQuerySem <- struct{}{}
	h.s.ownerQuerySem <- struct{}{}
	r := httptest.NewRequest(http.MethodGet, "/diva/v1/owner/joined-groups?account_mid="+account, nil)
	r.Header.Set("Authorization", "Bearer "+divaTestToken)
	w := httptest.NewRecorder()
	h.s.handler().ServeHTTP(w, r)
	if w.Code != 503 || calls != 1 {
		t.Fatal("busy query was not rejected")
	}
	<-h.s.ownerQuerySem
	<-h.s.ownerQuerySem
	h.s.cfg.listOwnerGroups = func(context.Context, *LineClient) ([]divaJoinedGroup, error) {
		return nil, errors.New("private token detail")
	}
	w = httptest.NewRecorder()
	h.s.handler().ServeHTTP(w, r)
	if w.Code != 503 || strings.Contains(w.Body.String(), "private token") || strings.Contains(w.Body.String(), `"groups":[]`) {
		t.Fatal("failed query leaked private error or empty success")
	}
}
