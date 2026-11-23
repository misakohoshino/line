package connector

// OWNER P3 退群（ServerB docs/03-1 §12.2；使用者 2026-11-23 定要做）：
// 先確認自己在群組裡才送、只送一次、讀回清單確認才算退出；非法請求不碰 LINE。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

type fakeLeaveLine struct {
	lists  []*line.GetAllChatMidsResponse // 依序回的群組清單；nil＝讀不到
	reads  int
	leaves int
	err    error
}

func (f *fakeLeaveLine) deps() divaLeaveDeps {
	return divaLeaveDeps{
		mids: func(context.Context) (*line.GetAllChatMidsResponse, error) {
			i := f.reads
			f.reads++
			if i >= len(f.lists) {
				i = len(f.lists) - 1
			}
			if f.lists[i] == nil {
				return nil, errors.New("unavailable")
			}
			return f.lists[i], nil
		},
		chats: func(_ context.Context, ids []string) (*line.GetChatsResponse, error) {
			return &line.GetChatsResponse{Chats: []line.Chat{{ChatMid: ids[0], ChatName: "測試群"}}}, nil
		},
		leave: func(context.Context, string) error {
			f.leaves++
			return f.err
		},
		wait: func(context.Context) error { return nil },
	}
}

func leaveList(gids ...string) *line.GetAllChatMidsResponse {
	return &line.GetAllChatMidsResponse{MemberChatMids: gids}
}

func TestDIVALeaveOnlyWhenMemberAndConfirmed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		lists  []*line.GetAllChatMidsResponse
		err    error
		result string
		leaves int
	}{
		{"not member: nothing sent", []*line.GetAllChatMidsResponse{leaveList("cOther")}, nil, divaLeaveNotMember, 0},
		{"list unavailable: nothing sent", []*line.GetAllChatMidsResponse{nil}, nil, divaLeaveUnverified, 0},
		{"left and read back", []*line.GetAllChatMidsResponse{leaveList("cTarget", "cOther"), leaveList("cOther")}, nil, divaLeaveLeft, 1},
		{"slow readback still confirms", []*line.GetAllChatMidsResponse{leaveList("cTarget"), leaveList("cTarget"), leaveList()}, nil, divaLeaveLeft, 1},
		{"still member after 3 reads", []*line.GetAllChatMidsResponse{leaveList("cTarget")}, errors.New("rpc"), divaLeaveStillMember, 1},
		{"readback unavailable", []*line.GetAllChatMidsResponse{leaveList("cTarget"), nil}, nil, divaLeaveSentUnverified, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeLeaveLine{lists: tc.lists, err: tc.err}
			out := runDIVALeave(context.Background(), f.deps(), "cTarget")
			if out.Result != tc.result || f.leaves != tc.leaves {
				t.Fatalf("result=%s leaves=%d, want %s %d", out.Result, f.leaves, tc.result, tc.leaves)
			}
			if tc.leaves == 1 && (!out.Sent || out.Name != "測試群") {
				t.Fatalf("sent leave must report name: %+v", out)
			}
			if out.LeaveError != (tc.err != nil) {
				t.Fatalf("leave error flag: %+v", out)
			}
		})
	}
}

func TestDIVAOwnerLeaveEndpointValidatesBeforeLINE(t *testing.T) {
	calls := 0
	h := newDIVAControlHarness(t, divaControlConfig{
		leaveOwnerGroup: func(ctx context.Context, lc *LineClient, gid string) divaLeaveOutcome {
			calls++
			if _, ok := ctx.Deadline(); !ok || gid != "CGroup_fake-171" {
				t.Fatalf("missing deadline or wrong gid %q", gid)
			}
			return divaLeaveOutcome{Result: divaLeaveLeft, Name: "Group", Sent: true}
		},
		send: func(context.Context, *LineClient, *lineOutboundRequest) (*lineOutboundResult, error) {
			t.Fatal("leave must never send a message")
			return nil, nil
		},
	})
	h.env.lc.Mid = "UAccount_fake-90"
	account := h.env.lc.midOrFallback()
	valid := `{"account_mid":"` + account + `","gid":"CGroup_fake-171"}`
	for _, tc := range []struct {
		method, body, token string
		status              int
	}{
		{http.MethodPost, valid, "", 401},
		{http.MethodGet, valid, divaTestToken, 405},
		{http.MethodPost, `{"account_mid":"` + account + `"}`, divaTestToken, 400},
		{http.MethodPost, `{"account_mid":"` + account + `","gid":"不是群組"}`, divaTestToken, 400},
		{http.MethodPost, `{"account_mid":"` + account + `","gid":"UPerson"}`, divaTestToken, 400},
		{http.MethodPost, `{"account_mid":"` + account + `","gid":"CGroup_fake-171","rpc":"x"}`, divaTestToken, 400},
		{http.MethodPost, `{"account_mid":"uMissing","gid":"CGroup_fake-171"}`, divaTestToken, 503},
		{http.MethodPost, valid, divaTestToken, 200},
	} {
		r := httptest.NewRequest(tc.method, "/diva/v1/owner/leave-group", bytes.NewBufferString(tc.body))
		if tc.token != "" {
			r.Header.Set("Authorization", "Bearer "+tc.token)
		}
		w := httptest.NewRecorder()
		h.s.handler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("body=%s status=%d resp=%s", tc.body, w.Code, w.Body.String())
		}
		if w.Code == 200 {
			var data struct {
				OK     bool   `json:"ok"`
				Result string `json:"result"`
				Name   string `json:"name"`
				Sent   bool   `json:"sent"`
			}
			if json.Unmarshal(w.Body.Bytes(), &data) != nil || !data.OK || data.Result != divaLeaveLeft || data.Name != "Group" || !data.Sent {
				t.Fatalf("bad response: %s", w.Body.String())
			}
		}
	}
	if calls != 1 {
		t.Fatalf("invalid requests reached LINE: %d", calls)
	}
}
