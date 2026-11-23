package connector

// OWNER P3「退群:群組編號」（ServerB docs/03-1 §12.2；使用者 2026-11-23 定要做，照 Server C）。
// Server A worker 已驗過是管理員、指令是完整群組編號；這裡只做 LINE 這一段：
//   1. 讀自己目前的群組清單（跟 LG 同一份、即時讀），確認真的在這個群組裡；不在 → NOT_MEMBER，什麼都不送。
//   2. 送一次 deleteSelfFromChat（退群；失敗或結果不明都不重試）。
//   3. 再讀清單（最多 3 次，間隔同自動入群），自己已不在 → LEFT；還在 → STILL_MEMBER；讀不到 → SENT_UNVERIFIED。
// 不用群名比對、不做其他 RPC；紀錄不寫群組編號、帳號、錯誤原文。
import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

const (
	divaLeaveNotMember      = "NOT_MEMBER"
	divaLeaveLeft           = "LEFT"
	divaLeaveStillMember    = "STILL_MEMBER"
	divaLeaveUnverified     = "UNVERIFIED"      // 送之前就讀不到清單：沒有送
	divaLeaveSentUnverified = "SENT_UNVERIFIED" // 已送出、讀不回結果
	divaLeaveBodyLimit      = 4096
)

type divaLeaveOutcome struct {
	Result     string `json:"result"`
	Name       string `json:"name"`
	Sent       bool   `json:"sent"`
	LeaveError bool   `json:"leave_error"`
}

type divaLeaveDeps struct {
	mids  func(context.Context) (*line.GetAllChatMidsResponse, error)
	chats func(context.Context, []string) (*line.GetChatsResponse, error)
	leave func(context.Context, string) error
	wait  func(context.Context) error
}

func divaSelfMember(mids *line.GetAllChatMidsResponse, gid string) bool {
	for _, m := range mids.MemberChatMids {
		if m == gid {
			return true
		}
	}
	return false
}

func runDIVALeave(ctx context.Context, deps divaLeaveDeps, gid string) divaLeaveOutcome {
	mids, err := deps.mids(ctx)
	if err != nil || mids == nil {
		return divaLeaveOutcome{Result: divaLeaveUnverified}
	}
	if !divaSelfMember(mids, gid) {
		return divaLeaveOutcome{Result: divaLeaveNotMember}
	}
	out := divaLeaveOutcome{Sent: true}
	if chats, fetchErr := deps.chats(ctx, []string{gid}); fetchErr == nil && chats != nil {
		for _, chat := range chats.Chats {
			if chat.ChatMid == gid {
				out.Name = chat.ChatName
			}
		}
	}
	out.LeaveError = deps.leave(ctx, gid) != nil
	out.Result = divaLeaveSentUnverified
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 && deps.wait(ctx) != nil {
			break
		}
		after, readErr := deps.mids(ctx)
		if readErr != nil || after == nil {
			continue
		}
		if !divaSelfMember(after, gid) {
			out.Result = divaLeaveLeft
			return out
		}
		out.Result = divaLeaveStillMember
	}
	return out
}

func leaveDIVAOwnerGroup(ctx context.Context, lc *LineClient, gid string) divaLeaveOutcome {
	client := lc.newClient()
	reader := newDIVAGroupReader(client)
	return runDIVALeave(ctx, divaLeaveDeps{
		mids:  reader.mids,
		chats: reader.chats,
		leave: func(ctx context.Context, chatMID string) error {
			return client.DeleteSelfFromChatContext(ctx, int64(lc.nextUntrackedReqSeq()), chatMID)
		},
		wait: waitDIVAGroup,
	}, gid)
}

func (s *divaControlServer) handleOwnerLeaveGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeDIVAError(w, http.StatusMethodNotAllowed, "", outboundInvalidRequest, "method not allowed")
		return
	}
	if !s.authorized(r) {
		writeDIVAError(w, http.StatusUnauthorized, "", outboundUnauthorized, "missing or invalid bearer token")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, divaLeaveBodyLimit+1))
	var req struct {
		AccountMID string `json:"account_mid"`
		GID        string `json:"gid"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err != nil || len(raw) > divaLeaveBodyLimit || decoder.Decode(&req) != nil ||
		!divaUserMIDPattern.MatchString(req.AccountMID) || !isDIVAGroupID(req.GID) {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "account_mid and one group gid are required")
		return
	}
	var logins []*bridgev2.UserLogin
	if s.cfg.logins != nil {
		logins = s.cfg.logins()
	}
	lc, selectionError := selectDIVALogin(logins, req.AccountMID)
	if selectionError != nil {
		writeDIVAError(w, selectionError.status, "", selectionError.code, selectionError.detail)
		return
	}
	select {
	case s.ownerQuerySem <- struct{}{}:
		defer func() { <-s.ownerQuerySem }()
	default:
		writeDIVAError(w, http.StatusServiceUnavailable, "", outboundInternal, "owner query busy")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	leave := s.cfg.leaveOwnerGroup
	if leave == nil {
		leave = leaveDIVAOwnerGroup
	}
	out := leave(ctx, lc, req.GID)
	s.cfg.log.Info().Str("result", out.Result).Bool("sent", out.Sent).Bool("leave_error", out.LeaveError).
		Msg("DIVA owner leave group")
	writeDIVAJSON(w, http.StatusOK, map[string]any{
		"version": 1, "ok": true, "result": out.Result, "name": out.Name, "sent": out.Sent, "leave_error": out.LeaveError,
	})
}
