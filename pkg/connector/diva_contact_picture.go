package connector

// #333 員工頭像（DIVA ServerB docs/21 §7「員工頭像現在就做」；AI_EXC #344 第 3 包補小修 2）：
// DC 訂單列表的員工單要顯示員工的 LINE 頭像。頭像路徑只在這個 Bridge 裡（聯絡人資料），
// Server A 收到員工在轉單群發的 // 時，問這裡一次（同一位員工一天最多一次，Server A 自己控制）。
//
//	Server A → POST /diva/v1/contact/picture {"mid"} → {"found", "picture_path"}
//
// 唯讀：先看聯絡人快取，沒有（或過期）才用既有的 getContact 問 LINE 一次（同一般收訊息時補名字的做法）。
// 不發訊息、不改任何資料；log 不記 LINE 內部編號、不記 LINE 錯誤內容。
// found=false：LINE 沒有這個人的資料（或查失敗）；found=true 且 picture_path=null：這個人沒有設頭像。

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

const (
	divaContactPictureMaxBodyBytes = 4 << 10
	divaContactPictureTimeout      = 5 * time.Second
)

// divaPicturePathPattern：LINE 頭像路徑（例 /0hXXXX…）。只放行看起來像路徑的字串，其他一律當沒有。
var divaPicturePathPattern = regexp.MustCompile(`^/[0-9A-Za-z._~\-/]{1,512}$`)

type divaContactPictureRequest struct {
	MID        string  `json:"mid"`
	AccountMID *string `json:"account_mid"`
}

// lookupDIVAContact 讀一位聯絡人（快取優先；過期或沒有才問 LINE，成功會寫回快取）。
func lookupDIVAContact(ctx context.Context, lc *LineClient, mid string) (line.Contact, error) {
	return lc.getContact(ctx, mid), nil
}

// divaContactPicture 把聯絡人資料整理成回應：LINE 沒有資料 → found=false；有資料但沒頭像 → picture_path=null。
func divaContactPicture(mid string, contact line.Contact) (bool, *string) {
	if contact.Mid == "" || (divaDisplayName(mid, contact.EffectiveDisplayName()) == nil && contact.PicturePath == "") {
		return false, nil
	}
	path := strings.TrimSpace(contact.PicturePath)
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if !divaPicturePathPattern.MatchString(path) {
		return true, nil
	}
	return true, &path
}

func (s *divaControlServer) handleContactPicture(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeDIVAError(w, http.StatusMethodNotAllowed, "", outboundInvalidRequest, "method not allowed")
		return
	}
	if !s.authorized(r) {
		writeDIVAError(w, http.StatusUnauthorized, "", outboundUnauthorized, "missing or invalid bearer token")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, divaContactPictureMaxBodyBytes))
	if err != nil {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "request body too large or unreadable")
		return
	}
	var req divaContactPictureRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if dec.Decode(&req) != nil || dec.More() {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "body must be {\"mid\"}")
		return
	}
	account := ""
	if req.AccountMID != nil {
		account = *req.AccountMID
	}
	if !divaUserMIDPattern.MatchString(req.MID) || (account != "" && !divaUserMIDPattern.MatchString(account)) {
		writeDIVAError(w, http.StatusBadRequest, "", outboundInvalidRequest, "mid must be a user")
		return
	}
	var logins []*bridgev2.UserLogin
	if s.cfg.logins != nil {
		logins = s.cfg.logins()
	}
	lc, selectionError := selectDIVALogin(logins, account)
	if selectionError != nil {
		writeDIVAError(w, selectionError.status, "", selectionError.code, selectionError.detail)
		return
	}
	// 跟查群成員共用同一個並行上限（都是少量、唯讀的 LINE 查詢），忙就直接回，不排隊。
	select {
	case s.memberCheckSem <- struct{}{}:
		defer func() { <-s.memberCheckSem }()
	default:
		writeDIVAError(w, http.StatusServiceUnavailable, "", outboundInternal, "contact lookup busy")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), divaContactPictureTimeout)
	defer cancel()
	lookup := s.cfg.contactLookup
	if lookup == nil {
		lookup = lookupDIVAContact
	}
	found, path := false, (*string)(nil)
	contact, lookupErr := lookup(ctx, lc, req.MID)
	if lookupErr == nil {
		found, path = divaContactPicture(req.MID, contact)
	}
	// 不記 LINE 內部編號、不記 LINE 錯誤內容。
	s.cfg.log.Debug().Bool("found", found).Bool("has_picture", path != nil).Bool("lookup_failed", lookupErr != nil).
		Msg("DIVA contact picture")
	writeDIVAJSON(w, http.StatusOK, map[string]any{"version": 1, "ok": true, "found": found, "picture_path": path})
}
