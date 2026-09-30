# DIVA Server A / LINE-1A 通用雙向 LINE Transport — Claude 唯讀 Review 回覆

日期：2026-09-30
性質：唯讀架構 Review，未修改任何 code
Review 對象：《DIVA_ServerA_LINE1A 通用雙向 LINE Transport 設計提案（Claude Review 任務書）》
Code 基準：`misakohoshino/line` main @ `a2a4998`（Add Go outbound reply path for DIVA smoke test）

---

## 0. Review 依據與限制

本次實際讀過的 code：

| 檔案 | 用途 |
|---|---|
| `pkg/connector/diva_adapter.go` | 目前 DIVA inbound forward 與 `sendDIVAText` |
| `pkg/connector/send_message.go` | bridge 既有 send engine（`HandleMatrixMessage`） |
| `pkg/connector/handle_message.go` | inbound 解密、`DIVA_RX` log、Matrix 轉換 |
| `pkg/connector/sync.go` | poll loop、backfill、own-echo 過濾 |
| `pkg/connector/client.go`、`auth_recovery.go`、`e2ee_keys.go`、`reaction.go` | `LineClient` 結構、token recovery、reqSeq 追蹤 |
| `pkg/line/errors.go`、`methods.go`、`structs.go` | LINE 錯誤判斷、`SendMessage`、`Message` struct |
| `diva_worker.py`、`diva_listener.py`、`docker-compose.yml`、`Dockerfile`、`scripts/prepare_diva_build.py`、`.github/workflows/deploy.yml` | 部署與 Python 端 |
| mautrix `bridgev2/matrix/connector.go`、`mxmain/main.go`、`appservice/*` | 確認框架有無 HTTP router 可承接 |

限制（請在討論時留意）：

1. 本 session 的 GitHub 存取範圍只有 `misakohoshino/line`。`DIVA-OA-ServerB-serverb` 的 docs 與 `DIVA-ServerA-perline/diva_worker.py` 我讀不到。Python 端的敘述以任務書第 2.1 節 + 本 repo 內的 `diva_worker.py`（v0.2 smoke 版）為準。
2. 本 repo 內也有一份 `diva_worker.py`，且 `docker-compose.yml` 是掛載本 repo 這份。它和 `DIVA-ServerA-perline` 那份的關係（哪份是 Production）需要你們確認，見 F-P1-4。
3. Review 容器沒有 libolm，無法跑完整 `go build`；用 `-tags goolm` 跑了 send path 的既有單元測試作為佐證（結果見附錄 B）。

---

## A. Overall verdict

**架構可施工。方向正確，沒有需要推翻設計的 blocker。**

但有 4 件事我列為「必改」，其中 3 件是現況就存在、與 LINE-1A 無關也該修的 Production 風險，1 件是 LINE-1A 一動工就必須遵守的原則：

| # | 必改項目 | 性質 |
|---|---|---|
| B-1 | DIVA forward 呼叫目前不在 source 裡，是 CI build 時用 Python 腳本文字替換注入 | 建置鏈脆弱、main 上的 code 與 Production image 不一致 |
| B-2 | 非文字訊息的 E2EE keyMaterial 正在以「text」欄位送到 Python，並以 Info 等級寫進 docker log | 金鑰外洩 + Python 收到垃圾 text |
| B-3 | inbound 沒有標示 live / backfill / decryption_failed / 自己發的訊息，Server A 無法分辨 | 未來 Dispatch 會被重播或誤判 |
| B-4 | `sendDIVAText` 已經是第二套（縮水版）group E2EE send engine，LINE-1A 必須把它刪掉並收斂進共用 core，而不是繼續長 | 任務書第 3.2 節原則 |

任務書本身的 contract 分層（inbound envelope、outbound command、result/error）我認為是合理的，只需補幾個欄位（`request_id`、`origin`、`account`、`is_from_me`、`decryption_failed`），細節在 D 節。

---

## B. 必改（不改會造成明確架構錯誤 / Production 風險）

### B-1. 把 DIVA forward 從 build-time 文字注入改回真正的 source

現況：

- `pkg/connector/handle_message.go:136-147` 只有 `[DIVA_RX]` log。
- `lc.forwardDIVAInbound(...)` 的呼叫 **不存在於 main 的 source**，是 `.github/workflows/deploy.yml:24-25` 在 build 前跑 `scripts/prepare_diva_build.py`，用字串比對把那一行插進 `handle_message.go` 之後才 docker build。
- 本機 `./build.sh`、`go test`、staticcheck、任何人 clone main 看到的都是「沒有 forward」的版本。

風險：

- 任何人重排、改字、加欄位到那段 log（例如 LINE-1A 要加 `content_type`），needle 就對不上，CI 直接 `SystemExit`；反過來若有人把 workflow 那一步拿掉，Production 會**無聲地**失去 inbound。
- `go vet` / 單元測試永遠測不到實際出貨的 code path。
- 對後續所有 LINE-1A 工作而言，這是第一個要拆的地雷，否則每個 PR 都要同時維護 needle。

建議：直接在 source 呼叫 `forwardDIVAInbound`。它本來就在 `DIVA_WEBHOOK_URL` 為空時 return（`diva_adapter.go:43-46`），上游行為不受影響。刪除 `scripts/prepare_diva_build.py` 與 workflow 那一步。這應是 LINE-1A 的第一個 commit（見 G）。

### B-2. 停止把非文字訊息的解密 payload 當 text 轉出去

現況：

- `queueIncomingMessage` 對所有 bridgeable content type（text / image / video / audio / file / sticker / location / contact / flex）都會走到 `decryptMessageBody`。
- 對 E2EE 圖片、影片、檔案、語音，解密後的 `bodyText` 是 `{"keyMaterial":"..."}`（或含 `fileName`）的 JSON；`unwrappedText` 只在有 `"text"` key 時才展開（`handle_message.go:364-372`），所以這些訊息的 `unwrappedText` 仍是**含 keyMaterial 的 JSON**。
- `[DIVA_RX]` log 與被注入的 `forwardDIVAInbound(unwrappedText, ...)` 只檢查 `ToType` 是 group/room，**不檢查 content type**。

後果：

- 群組裡每張 E2EE 圖片的 media 解密金鑰都以 Info 等級寫進 docker logs，並經 HTTP 送到 Python worker 當「text」。
- Python 端（若後面接 Dispatch）會把 `{"keyMaterial":...}` 當成司機說的話。

建議：

- v2 inbound 一定要依 content type 分 `content`，text 以外不要塞 `unwrappedText`。
- `metadata` 走白名單，永遠排除 `ENC_KM`、`keyMaterial` 等金鑰欄位。
- `[DIVA_RX]` Info log 至少改成 Debug、且不含 text；HTTP path 進 source 後，這行 log 就沒有存在必要（`diva_listener.py` 也要一起退場，任務書 14 節已說不擴充）。

### B-3. inbound 必須帶 origin / is_from_me / decryption_failed，否則 Server A 分不出「真的新事件」

我確認到三條路都會打到 `queueIncomingMessage`，因此都會 forward 到 DIVA：

| 路徑 | 位置 | 對 Server A 的意義 |
|---|---|---|
| 即時 poll（op 25 / 26） | `sync.go:2037-2042` | 正常 |
| 啟動時 backfill 最近訊息 | `sync.go:757-798`（`backfillRecentMessages`） | bridge 重啟 / DB 重置 / 新加群時，**舊訊息會重播給 Server A**。Dispatch 一旦上線，等於舊的報車訊息重新觸發流程 |
| 自己帳號從其他裝置（手機 LINE）發的訊息 | op 25，`msg.From == 自己 mid` | 真人調度員用手機在群裡講話也會被送到 Server A，且 `sender_id` 是機器人自己的 mid |

另外：

- `decryptionFailed == true` 的訊息目前照樣 forward，`text` 為空字串；`diva_worker.py` 的檢查只驗 `isinstance(str)`，空字串會過。舊的 `diva_listener.py` 反而有跳過 `decryption_failed`。HTTP 版把這個保護弄丟了。
- Bridge 自己透過 `sendDIVAText` 發的 `789` 會被 `sync.go:1793-1800` 的 `sentReqSeqs` 過濾掉，不會回送，這點 OK；但 reqSeq 是 `now % 1e9`（毫秒），**同一毫秒兩次送出會撞號**，第二封的 echo 會漏過過濾、以「自己 mid 發的 inbound」形式再送去 Server A。LINE-1A 開放主動 outbound 後併發變高，這個機率會變得真實（見 F-P1-2）。

建議欄位：`origin: "live" | "backfill"`、`sender.is_from_me`、`message.decryption_failed`。LINE-1A 階段 Go 可先預設**不 forward backfill**（或 forward 但標記），由 Python 決定丟棄。

### B-4. LINE-1A 動工時必須刪除 `sendDIVAText`，不能再長第二套 engine

`diva_adapter.go:124-220` 已經複製了 `HandleMatrixMessage` 裡 group E2EE 的：fetch group key → encrypt → 失敗 refetch → `markGroupNoE2EE` 降級 plaintext → 送出 → code 99 自動註冊 group key 再重送。這正是任務書 3.2 節禁止的形狀，而且已經開始漂移（沒有 mention、沒有 reply、沒有 1:1 peer key 判斷、沒有 blocked 檢查）。

它是 smoke 用途，當時這樣寫可以理解；但 LINE-1A 的 send core 抽出來後，`reply_text` 路徑與新的 outbound endpoint 都必須改走同一個 core，`sendDIVAText` 整個刪除。

---

## C. 建議改（提升維護性，非 blocker）

1. **`reply_text` 路徑列為過渡、給退場時間。** 它是「inbound 回應順便發訊」的隱性第二條 outbound，沒有 result、沒有 error taxonomy、沒有 request_id。outbound endpoint 上線並跑過一輪 smoke 後，Python 改為直接呼叫 outbound，再移除 `reply_text`。過渡期兩條都在時，`reply_text` 也要走共用 core。
2. **reqSeq 改用單調遞增。** `LineClient` 已有 `lastReqSeq` 欄位（`client.go:51`、`reaction.go:685`），加一個 `nextReqSeq()` 即可，同時解掉 B-3 的撞號問題。這是共用 core 的一部分，Matrix path 也受益。
3. **inbound HTTP client timeout 2 秒是對的方向**（`diva_adapter.go:20`），但 Python 之後會做更多事；保持「Go 不等 Python 做業務」的原則，Python 收到就先 200，業務走自己的 queue。不要為了 Python 變慢而把 Go 的 timeout 拉長。
4. **Python 端 v1/v2 判斷用 `version` 欄位，不要用 payload 形狀猜。** 這也是任務書 5.1 的問題，回答在 H-1。
5. **`docker-compose.yml` 的 `depends_on: diva-worker`** 可保留，但要知道它不是必要的：forward 是 async 且容錯，worker 掛了 bridge 照跑。反過來 outbound endpoint 上線後，Python 才是依賴 Go 的一方。
6. **命名一致性。** `diva_worker.py` docstring 寫 Server B，任務書寫 Server A；`DIVA_WEBHOOK_URL` 名稱與未來 `DIVA_CONTROL_*` 系列建議在 LINE-1A 一起定好 env 命名。
7. **`sender.display_name` 可以順手帶**（`lc.getContact` 有 cache，`userinfo.go:205`），屬 transport 層的身份原始資訊，不是司機判定；但要標成 best-effort、可為 null。

---

## D. Contract 建議版

只做到 contract 層；欄位名可再議，但**語意分層**建議照這樣。所有版本策略：Python 對未知欄位一律忽略；Go 只新增、不改語意；改語意才升 version。

### D-1. Inbound（Go → Server A，`POST /line/inbound`）

```json
{
  "version": 2,
  "event_id": "line:<message_id>",
  "event_type": "message",
  "origin": "live",
  "account": { "mid": "u<bridge 登入的 LINE 帳號 mid>" },
  "chat": {
    "id": "c1234...",
    "type": "group"
  },
  "sender": {
    "mid": "uabcd...",
    "is_from_me": false,
    "display_name": "阿明"
  },
  "message": {
    "id": "1234567890123",
    "content_type": 0,
    "created_at": 1727654321000,
    "decryption_failed": false,
    "e2ee": true
  },
  "content": {
    "type": "text",
    "text": "5分 白1234"
  },
  "relations": {
    "reply_to": { "message_id": "1234567890000" },
    "mentions": [
      { "mid": "uabcd...", "start": 0, "end": 3 }
    ],
    "mention_all": false
  },
  "metadata": {}
}
```

欄位說明與取值來源：

| 欄位 | 來源 / 規則 |
|---|---|
| `version` | 整數 2。v1 = 現在的四欄位 payload（無 `version` 欄位即視為 v1） |
| `event_id` | message 事件用 `line:<LINE message id>`；非 message 事件（未來 membership / call）由 Go 產生穩定 id。Python 用它 dedupe |
| `event_type` | LINE-1A 只會送 `message`；`membership`、`call`、`system` 保留給 1D |
| `origin` | `live` / `backfill`。LINE-1A 建議 Go 預設不送 backfill，或送但 Python 丟棄 |
| `account.mid` | `lc.Mid`。多帳號時 Server A 才知道要用哪個帳號回 |
| `chat.type` | 由 mid 前綴決定：`c` = group、`r` = room、`u` = direct（`guessToType`，`client.go:827`）。LINE-1A 維持只送 group/room，但 contract 要有 direct |
| `sender.is_from_me` | `msg.From == lc.Mid`（op 25） |
| `message.content_type` | LINE 原始數值（0 text、1 image、2 video、3 audio、7 sticker、13 contact、14 file、15 location…），保留原值方便 debug |
| `message.created_at` | LINE `createdTime`（ms），不是 Python 收到的時間 |
| `message.decryption_failed` / `e2ee` | `decryptMessageBody` 回傳值 / `len(msg.Chunks) > 0` |
| `content` | 依 type 分：`text` {text}；`image`/`video`/`audio`/`file` {file_name, file_size, duration, oid?}（**不含金鑰**，LINE-1A 只送 descriptor 不送 bytes）；`sticker` {package_id, sticker_id}；`location` {title, address, latitude, longitude}；`contact` {mid, display_name}；其他 → `{"type":"unsupported"}` |
| `relations.reply_to` | `RelatedMessageID` 且 `MessageRelationType == 3`（同 `resolveReplyRelatesTo` 的判斷，`handle_message.go` 末段） |
| `relations.mentions` | 解析 `ContentMetadata["MENTION"].MENTIONEES`：`M` → mid，`S`/`E` → UTF-16 offset；`A == "1"` → `mention_all: true`。這樣 Dispatch 不用從文字猜 `@阿明` 是不是真 mention |
| `metadata` | **白名單**：例如 `ORGCONTP`、`STKID`、`STKPKGID`。永遠排除 `ENC_KM`、`keyMaterial`、chunks |

### D-2. Outbound（Server A → Go，`POST /diva/v1/send`，route 名待定）

```json
{
  "version": 1,
  "request_id": "b2f6c0e4-...",
  "target": {
    "chat_id": "c1234...",
    "account_mid": null
  },
  "message_type": "text",
  "content": {
    "text": "@阿明 這張你出"
  },
  "relations": {
    "reply_to": { "message_id": "1234567890000" },
    "mentions": [
      { "mid": "uabcd...", "name": "阿明" }
    ],
    "mention_all": false
  },
  "options": {
    "reply_fallback": "fail",
    "timeout_ms": 15000
  }
}
```

| 欄位 | 規則 |
|---|---|
| `request_id` | **必填**，Server A 產生的 UUID。Go 以它做短期 dedupe（見 H-12） |
| `target.chat_id` | LINE mid。chat type 由 Go 用前綴推，不需要 Server A 給 |
| `target.account_mid` | 可選；單帳號時 null。Go 找不到對應 login → `NO_ACTIVE_LOGIN` |
| `message_type` | LINE-1A 只實作 `text`；其他 type 收到就回 `UNSUPPORTED_MESSAGE_TYPE`，但欄位結構先定 |
| `relations.mentions[].name` | Go 在 `content.text` 中尋找 `@<name>` 並算 UTF-16 offset（重用 `byteIndexToUTF16Offset` 與 `buildMentionMetadata` 的邏輯，`send_message.go:958`）。找不到 → `INVALID_REQUEST` 並指出哪個 mention。不要要求 Python 自己算 UTF-16 offset |
| `relations.reply_to` + `mentions` | 可同時存在。LINE `Message` struct 本身就是 `RelatedMessageID` 與 `ContentMetadata["MENTION"]` 兩個獨立欄位，沒有互斥問題 |
| `options.reply_fallback` | `fail`（預設） / `send_without_reply`。見 H-7 |
| `options.timeout_ms` | 上限由 Go 夾住（例如 30s） |

Media 的 `content` 格式（1C 再定）：建議以「Go 端可讀的 bytes 來源」為準，因為共用 core 需要的是 `[]byte`（`HandleMatrixMessage` 的每個 media case 都從 `DownloadMedia` 拿到 `data []byte` 後再走 encrypt/upload）。選項排序：(1) multipart 上傳 bytes 給 Go；(2) Go 可達的內網 URL。不要讓 Go 去讀 Server A 的本機路徑。

### D-3. Result / Error（Go → Server A 的 HTTP 回應）

成功：

```json
{
  "ok": true,
  "request_id": "b2f6c0e4-...",
  "message": {
    "id": "1234567890999",
    "chat_id": "c1234...",
    "created_at": 1727654400000,
    "e2ee": true
  },
  "fallbacks": [],
  "deduplicated": false
}
```

- `message.id` 來自 `client.SendMessage` 回傳的 `wrapper.Data.ID`（`methods.go:380-397`）。這是 server-assigned id，Server A 之後收到司機 reply 時 `relations.reply_to.message_id` 會對上這個值，**這是 1B 能做「引用派車訊息」的前提**。
- `fallbacks` 列出 transport 自己做過的降級：`"plaintext_no_e2ee"`（group 沒有可用 key）、`"reply_relation_dropped"`（僅在 `reply_fallback: send_without_reply` 時才會出現）、`"zip_wrapped"`（檔案被 LINE 拒收後包 zip 重送，1C）。
- `deduplicated: true` 表示同 `request_id` 之前已送成功，這次回的是快取結果。

失敗：

```json
{
  "ok": false,
  "request_id": "b2f6c0e4-...",
  "error": {
    "code": "REPLY_TARGET_NOT_FOUND",
    "retryable": false,
    "delivery": "not_sent",
    "detail": "TalkException code 5 not found (relatedMessageId=...)"
  }
}
```

`error.delivery` 三態很重要：`not_sent` / `sent` / `unknown`。`unknown` 專門給 timeout 類：Go 已把請求送出去但沒等到 LINE 回應，訊息可能已經到群組。Server A 對 `unknown` **不得**盲目 retry，應用同一 `request_id` 重送讓 Go dedupe，或交真人。

Transport-level error code 最小集合（對應到現有判斷函式）：

| code | HTTP | retryable | 對應現況 |
|---|---|---|---|
| `INVALID_REQUEST` | 400 | no | JSON / 必填欄位 / mention 找不到 |
| `UNAUTHORIZED` | 401 | no | shared secret 錯 |
| `UNSUPPORTED_MESSAGE_TYPE` | 501 | no | 1A 只支援 text |
| `NO_ACTIVE_LOGIN` | 503 | no（需人工） | bridge 沒有可用 LINE login，或 `account_mid` 對不上 |
| `LINE_SESSION_INVALID` | 503 | no（需人工重登） | `line.IsAuthError`、`sessionInvalidated`、forced logout |
| `TARGET_NOT_FOUND` | 404 | no | `IsNotAMemberError`（code 10）、chat 不存在 |
| `BLOCKED` | 403 | no | `isUserBlocked`（DM） |
| `E2EE_KEY_UNAVAILABLE` | 503 | no（需重連） | `e2ee.ErrMissingOwnPrivateKey`（`lineGroupE2EEReconnectRequiredError`） |
| `REPLY_TARGET_NOT_FOUND` | 409 | no | `shouldRetrySendWithoutReplyRelation` 為 true 的情境（`send_message.go:891`） |
| `UPLOAD_FAILED` | 502 | yes | OBS 上傳失敗（1C） |
| `LINE_TRANSIENT` | 502 | yes | HTTP 5xx / network / `request failed` |
| `TIMEOUT` | 504 | unknown | ctx deadline，`delivery: unknown` |
| `INTERNAL` | 500 | no | 其他 |

實作提醒：`pkg/line/errors.go` 全部靠 error string 比對（`"code":10051`、`talkexception` 等）。不建議在 LINE-1A 重寫成 typed error；建議加一個 `line.Classify(err) ErrorClass`，內部就是把既有 `IsXxx` 函式排順序呼叫，並用 `errors_test.go` 裡的真實字串做 table test。

---

## E. send core 重構建議

### E-1. 現況分析

`HandleMatrixMessage`（`send_message.go:60-880`）是單一 820 行函式，結構是：

```
[1] Matrix 型別解析（60-170）
      portal mid、blocked 檢查、plainText 判斷（E2EE nil / group noE2EE cache / 1:1 peer key probe）
      MsgFile 依 mime 改判成 image/video/audio、media flow 判斷
[2] 依 msgType 準備 payload（170-563）
      text → payload / plainTextBody
      image/file/video/audio → DownloadMedia → encrypt → UploadOBS → contentMetadata
[3] mention metadata（565-579）→ 依 Matrix ghost MXID 反查 mid
[4] E2EE 加密（581-655）group：fetch key / encrypt / refetch / 降級 plaintext；1:1：peer key encrypt
[5] 組 line.Message（657-676）、reply relation（678-696，查 bridge DB by MXID）
[6] 送出 + 三種重試（698-812）zip 重送 / code 99 註冊 group key 重送 / reply target 不存在重送
[7] plain media 後上傳（817-845）
[8] 回 MatrixMessageResponse（847-853）
```

其中 [2]、[4]、[5] 後半、[6]、[7] 與 Matrix 無關，只依賴 `lc`（E2EE manager、caches、`callLineResultUsing`）。[1] 前半、[3]、[5] 前半、[8] 是 Matrix-specific。

### E-2. 建議切法（最小安全邊界）

引入一個 **內部 request struct**，不對外暴露、不是 DIVA contract：

```go
// 純 Go 值物件，不含 bridgev2 / event 型別
type lineOutboundRequest struct {
    ChatMID       string
    ContentType   ContentType
    Text          string            // text
    Media         *outboundMedia    // 1C：Data []byte, MimeType, FileName, Duration
    MentionMeta   map[string]string // 已算好的 ContentMetadata["MENTION"]
    ReplyToID     string            // LINE server message id，空字串 = 無
    ReplyFallback bool              // reply target 不存在時是否去掉 relation 重送
}

type lineOutboundResult struct {
    Sent           *line.Message
    SentPlaintext  bool
    ReplyDropped   bool
    ZipWrapped     bool
}

func (lc *LineClient) sendLineOutbound(ctx context.Context, req *lineOutboundRequest) (*lineOutboundResult, error)
```

- **共用 core（`sendLineOutbound`）**：[1] 的 plainText 判斷（blocked / E2EE nil / group cache / peer key probe / media flow）、[2] 的 encrypt + upload（**輸入改為 `[]byte`**，不再自己 `DownloadMedia`）、[4]、[5] 組 `line.Message`（reply 直接用 `ReplyToID`）、[6] 全部重試、[7]。
- **Matrix adapter（`HandleMatrixMessage` 變薄）**：解析 `msg.Content`、`DownloadMedia` 取 bytes、`buildMentionMetadata`（依 ghost MXID）、`msg.ReplyTo` / `GetPartByMXID` 查 LINE message id、組 `lineOutboundRequest`、呼叫 core、把結果包成 `MatrixMessageResponse`、把錯誤包成 `bridgev2.WrapErrorInStatus`。`ReplyFallback` 固定為 `true`，維持現況。
- **DIVA adapter**：驗 request → 用 `mentions[].name` 在 text 中定位算 offset 產生 `MentionMeta` → 組 `lineOutboundRequest`（`ReplyFallback` 依 `options`）→ 呼叫 core → 依 D-3 映射 result / error。

### E-3. 不應抽進 DIVA contract 或 core 的 Matrix-specific 邏輯

| 邏輯 | 位置 | 為什麼留在 Matrix adapter |
|---|---|---|
| `bridgev2.MatrixMessage`、`event.MessageEventContent`、`msg.Portal` | [1] | Portal 是 Matrix room 對應，DIVA 沒有 room |
| `DownloadMedia(msg.Content.URL, msg.Content.File)` | [2] | 從 homeserver 下載，DIVA 的 bytes 另有來源 |
| `MsgFile` 依 mime 轉型 | [1] | Matrix client 拖拉檔案的行為補償；DIVA 直接給明確 type |
| `ParseGhostMXID` / `GetExistingGhostByID` / `mentionLinkRegex` | [3] | 從 Matrix formatted body 反查 mid |
| `@room` / `@all` / `@everyone` 文字偵測 | [3] | Matrix 慣例；DIVA 用明確 `mention_all` |
| `msg.ReplyTo` / `DB.Message.GetPartByMXID` | [5] | bridge DB 以 MXID 索引 |
| `bridgev2.WrapErrorInStatus(...).WithSendNotice(...)` | 多處 | Matrix message status 語意，DIVA 要的是 D-3 的 taxonomy |
| `MatrixMessageResponse{DB: ...}` | [8] | 寫 bridge DB |

### E-4. 為什麼不要偽造 `bridgev2.MatrixMessage`

- `MatrixMessage` 內含 `Portal *bridgev2.Portal`（DB row）、`Event *event.Event`、`ReplyTo *database.Message`（`networkinterface.go:1429`）。要偽造就得先有一個真實 portal，沒有 portal 的 LINE 群（bridge 尚未建立 Matrix room）就送不了。
- media 要先上傳到 homeserver 拿 mxc URL 再讓 core 下載回來，繞一圈。
- mention 要先有 ghost MXID。
- 回傳的 `MatrixMessageResponse` 框架會拿去寫 DB；DIVA 呼叫若不走框架，會有半套 side effect。
- 錯誤語意是 Matrix status，不是 transport taxonomy。

結論：不要偽造，抽 core 是正確且成本可控的路。

### E-5. 重構風險控制

- 兩個 commit：第一個**純搬移**（`HandleMatrixMessage` 行為不變，只是內部呼叫 core），第二個才接 DIVA。
- 純搬移那個 commit 要有 golden test：給定 request → 檢查組出來的 `line.Message`（`Text` vs `Chunks`、`ContentMetadata["MENTION"]`、`RelatedMessageID`、`MessageRelationType == 3`）。LINE 呼叫可用 `httptest.Server` 假 `callRPC`。
- 三種重試（zip / code 99 / reply）都靠閉包裡的 `chunks`、`payload`、`plainMediaData` 等狀態，搬進 core 時要一起搬，不要拆成多個函式改變狀態共享方式。

---

## F. 安全 / 相容風險（P0 / P1 / P2）

### P0（上線前必須處理）

- **F-P0-1 outbound endpoint 必須 fail-closed。** 沒有設定 shared secret 就不啟動 listener；驗證用 `crypto/subtle.ConstantTimeCompare`。「主動發 LINE」是對真人的外部 side effect，任何人打得到這個 port 就能以公司帳號在司機群發話。
- **F-P0-2 只綁 Docker 內網。** compose 用 `expose` 不用 `ports`；Go 端 listen 位址由 env 指定，預設 `0.0.0.0:<port>` 只在 compose network 內可達。若 Server A Python 與 bridge 不在同一台，走 VPN / 內網，不開公網。
- **F-P0-3 keyMaterial 外洩**（B-2）。
- **F-P0-4 backfill 重播**（B-3）。Dispatch 上線前必須有 `origin` 或 Go 端不 forward backfill。

### P1（LINE-1A 內處理）

- **F-P1-1 build-time patch**（B-1）。
- **F-P1-2 reqSeq 撞號**（C-2）。併發 outbound 下會讓自己的 echo 漏過過濾、變成 inbound 送到 Server A。
- **F-P1-3 timeout 後的重送 = 重複派車。** contract 必須有 `request_id` 與 `delivery: unknown`（H-12）。
- **F-P1-4 兩份 `diva_worker.py`。** 本 repo 有一份且 compose 掛載它；任務書說 runtime 在 `DIVA-ServerA-perline`。若 Production 其實跑的是本 repo 這份，那任務書 2.1 說的 feature flag cache 不在其中；若跑的是另一份，本 repo 這份與 compose 應清掉或標明僅供本機 smoke。請確認。
- **F-P1-5 多 login。** bridgev2 允許多個 UserLogin；`LineConnector` 沒有「哪個 login 是 DIVA 用的」概念。endpoint 需要一個決定規則（只有一個 login 就用它；多個且未指定 `account_mid` → `NO_ACTIVE_LOGIN`）。
- **F-P1-6 per-chat 序列化。** 同一群連續兩則派車訊息若並行送出，LINE 端順序不保證；core 建議加 per-chat mutex，全域再加 concurrency 上限（例如 4）。
- **F-P1-7 request body 上限。** inbound 端 Python 已限 1 MiB；Go endpoint text 也限 1 MiB，media 1C 再另定。

### P2（記錄，後續 phase）

- **F-P2-1 `[DIVA_RX]` Info log 含完整訊息文字**，所有群組對話進 docker logs。HTTP path 進 source 後改 Debug 並去掉 text。
- **F-P2-2 `reply_text` 隱性 outbound** 的退場（C-1）。
- **F-P2-3 DM 未 forward。** 目前只 forward group/room。是否要 DM 屬營運決定（docs 裡若有定案就照 docs），contract 已預留 `chat.type: direct`。
- **F-P2-4 Python worker 的 HTTP server 是 `ThreadingHTTPServer`**，無 request 上限與 timeout；LINE-1A 不改它，但 Server A 正式化時要換。

---

## G. 建議施工順序（最小可回滾 commit / PR）

每個 PR 單獨可 revert，且前一個沒 merge 也不會讓 Production 倒退。

| # | PR | 內容 | 驗證 | 回滾方式 |
|---|---|---|---|---|
| 0 | **forward 進 source** | 在 `queueIncomingMessage` 直接呼叫 `forwardDIVAInbound`；刪 `scripts/prepare_diva_build.py` 與 workflow step；`[DIVA_RX]` 改 Debug 且不含 text；text 以外 content type 暫時不 forward | 既有 smoke（`123456654` → `789`）不變 | revert PR（image 回到 patch 版） |
| 1 | **inbound v2** | Go 依 `DIVA_CONTRACT_VERSION`（預設 `1`）決定送 v1 或 v2；v2 含 `origin`、`is_from_me`、`decryption_failed`、typed `content`、`relations`；backfill 預設不 forward | Go 單元測試：golden JSON（text / image descriptor / reply / mention / decryption_failed）。Python 先部署「v1 + v2 都接受」再把 env 切 `2` | env 切回 `1`，不需 rebuild |
| 2 | **send core 抽出（純重構）** | `sendLineOutbound` + `lineOutboundRequest`；`HandleMatrixMessage` 改呼叫 core；`sendDIVAText` 刪除，`reply_text` 路徑改呼叫 core；`nextReqSeq()` | golden test 組出的 `line.Message` 與重構前一致；既有 `send_message_test.go` 全過；Beeper 手動驗一次 group E2EE 文字 / 圖片 / reply | revert PR |
| 3 | **error classify + result** | `line.Classify(err)`；connector 端 D-3 映射；`fallbacks` 記錄 | table test 用 `errors_test.go` 的真實錯誤字串 | revert PR（尚無外部呼叫者） |
| 4 | **outbound endpoint** | 獨立 `net/http` listener（`DIVA_CONTROL_LISTEN`、`DIVA_CONTROL_TOKEN`，未設 token 不啟動）；`request_id` in-memory dedupe（TTL 10 分鐘）；只支援 `text` + `mentions` + `reply_to`；per-chat mutex；compose `expose` port | 單元測試：401 / 400 / dedupe / mention offset；手動 `curl` 對測試群發一則、確認回傳 `message.id` 與之後 inbound `reply_to` 對得上 | 不設 `DIVA_CONTROL_TOKEN` 即關閉 |
| 5 | **Python 改走 outbound**（另一 repo） | `789` 改由 Python 主動呼叫 endpoint；`reply_text` 保留一個 release 後移除 | 同一 smoke | Python 端 revert |

PR 2 是風險最高的一個（動到 Beeper 路徑），所以放在 inbound v2 之後、endpoint 之前：inbound v2 不依賴它，可先上；endpoint 依賴它，不能先上。

---

## H. 第 17 節 16 題逐項回答

**1. Inbound v2 envelope 的分層是否合理？**
合理。`chat / sender / message / content / relations / metadata` 六層對應 LINE `Message` struct 的自然切分（`To`+`ToType`、`From`、`ID`+`CreatedTime`+`ContentType`、`Text`/`Location`/metadata、`RelatedMessageID`+`MENTION`、其餘 `ContentMetadata`），Go 端組裝成本低。需補：`event_id`、`origin`、`account`、`sender.is_from_me`、`message.decryption_failed`。`version` 用單一整數即可，envelope / content 分開版本是過度設計；用「`content.type` 決定形狀 + 只新增不改語意」的政策就夠。

**2. 是否有欄位過度設計或不足？**
不足的見上題。過度的：`relations` 裡「其他 LINE relation」目前 code 只認 `MessageRelationType == 3`（reply），先不要預留 forward / quote 等未實作的 relation。`content` 對 media 只送 descriptor（檔名、大小、時長、content_type），不送 bytes、不送金鑰，bytes 留到 1C 決定要不要由 Go 代下載。

**3. v1 / v2 相容策略應選哪種？**
方案 A 變體：Python 先接受 v1 + v2（用 `version` 欄位判斷，無此欄位即 v1），Go 用 env `DIVA_CONTRACT_VERSION` 切換（預設 1），部署順序 Python → 切 env。v2 **不**帶 legacy 頂層欄位，同意任務書偏好。回滾只需切 env，不需 rebuild image。不要用 payload 形狀猜版本。

**4. Outbound command 的 target / content / relations / options 分層是否合理？**
合理。補充：頂層加 `request_id`（必填）與 `version`；`target` 只要 `chat_id`（type 由 Go 推），可選 `account_mid`；`relations.mentions[]` 用 `{mid, name}` 讓 Go 算 offset；`options` 目前只放 `reply_fallback` 與 `timeout_ms`。reply 與 mention 可並存，LINE `Message` 是兩個獨立欄位，沒有二選一問題。

**5. Outbound result 是否應回 LINE message ID？**
必須。`client.SendMessage` 已回傳 server-assigned `Message`（`methods.go:380-397`），`sentMsg.ID` 現成可用，`HandleMatrixMessage` 也是拿它寫 bridge DB。沒有它，1B 的「司機引用派車訊息」無法對應。另外回 `created_at`、`e2ee`、`fallbacks[]`、`deduplicated`。

**6. Error taxonomy 應至少有哪些 transport-level 類別？**
見 D-3 表，13 個 code，每個都對應到 `pkg/line/errors.go` 既有判斷函式或 connector 現有分支。額外要求 `delivery: not_sent | sent | unknown`，這比 `retryable` 更關鍵。

**7. Reply fallback 應如何？**
建議「預設不 fallback（回 `REPLY_TARGET_NOT_FOUND`，未送出），Server A 可用 `options.reply_fallback: send_without_reply` 明確開啟；開啟時結果 `fallbacks` 含 `reply_relation_dropped`」。理由：送失敗可重試、上層可決定改發；但誤送出去的「@阿明 這張你出」沒有引用、真人已經看到，撤回只有 24 小時且會留痕，是不可逆的業務錯誤。Matrix path 維持現況（自動 fallback），所以 fallback 必須是 core 的參數，不是寫死行為。

**8. `HandleMatrixMessage(...)` 應如何最小化重構？**
見 E-2。一句話：把 `[]byte` media、mention metadata、reply LINE id 當輸入，其餘（plainText 判斷、encrypt、upload、組 `line.Message`、三種重試、plain media 後上傳）整段搬進 `sendLineOutbound`。純搬移一個 commit、接 DIVA 另一個 commit。

**9. 是否能避免 DIVA 偽造 Matrix message？**
能，且應該。原因見 E-4：需要真實 Portal、ghost MXID、mxc URL，且回傳會寫 bridge DB。

**10. DIVA outbound 最適合 HTTP endpoint / 既有內部 command path / 其他 IPC？**
HTTP endpoint，但要**自己開 listener**。查證結果：mautrix 的 `MatrixConnector.GetRouter()` 只有在 `appservice.public_address` 有設時才回傳 router（`bridgev2/matrix/connector.go:294-299`），而 Beeper / bbctl 部署通常走 websocket 模式（`homeserver.websocket: true`），此時根本沒有 appservice HTTP server（`connector.go:199-214`）。所以「掛在既有 router 上」在 Production 多半不成立。既有 command path（Matrix bot command、provisioning API）都以 Matrix user 為主體，不適合。做法：在 `LineConnector.Start`（`connector.go:44`）或 `mxmain.PostStart` 起一個最小 `net/http.Server`，只註冊 `/diva/v1/send` 與 `/diva/v1/health`。備選是「Go 定期 long-poll Python 的 outbox」，優點是 Go 不開任何新 port，缺點是 result 要另一條 callback，不建議 1A 採用。

**11. Outbound endpoint 如何限制在安全 internal network？**
F-P0-1、F-P0-2：env 指定 listen 位址；compose `expose` 而非 `ports`；`Authorization: Bearer <DIVA_CONTROL_TOKEN>` 常數時間比對；token 未設則不啟動；body 1 MiB；server `ReadHeaderTimeout` / `ReadTimeout`；per-request ctx timeout 由 `options.timeout_ms` 夾在 30 s 內；per-chat mutex + 全域 concurrency 上限；每次 request 記 `request_id`、`chat_id`、結果 code（不記訊息內容）。

**12. LINE-1A 是否應先預留 idempotency key？**
應現在納入，`request_id` 必填。Go 端只做 in-memory LRU（`request_id → result`，TTL 約 10 分鐘），不做 durable DB。理由：timeout 後 Python retry 是最常見的重複發送來源，這個窗口以分鐘計，in-memory 已能擋掉絕大多數；且 contract 沒有位子的話，之後加就是 breaking change。inbound 對稱地帶 `event_id`。

**13. 哪些現有 Production path 最容易因本次重構倒退？**
依機率排序：(a) Beeper 群組 E2EE 文字 / 圖片 / reply 送出（PR 2 動到 `HandleMatrixMessage`）；(b) group key 相關重試（code 99 自動註冊、noE2EE 降級）；(c) 檔案 zip 重送與 plain media 後上傳；(d) 現行 smoke `reply_text`（PR 2 刪 `sendDIVAText` 時要確保改走 core）；(e) CI needle（PR 0 前任何人動 log 行）；(f) `docker-compose.yml` 加 port / env 時把 `DIVA_WEBHOOK_URL` 弄掉。`/health` 與 feature flag 在 Python 端，Go 側改動不會碰到，但 F-P1-4 的兩份 worker 問題要先釐清。

**14. 哪些測試屬於必要，不要重複做無意義測試？**
必要：inbound v2 golden JSON（5 種 case）；send core golden `line.Message`（text plain / text E2EE chunks 存在 / mention metadata / reply relation）；`line.Classify` table test（真實錯誤字串）；endpoint 401 / 400 / dedupe / mention offset / reply_fallback 兩種；reqSeq 單調性；Python parser v1 + v2；一條 docker-compose E2E smoke（沿用 `123456654` → `789`，改為經 outbound endpoint）。不要做：重測 E2EE 加解密正確性（`e2ee_keys_test.go` 已有）、重測 mautrix 框架、對真 LINE server 做自動化測試（帳號會被鎖）、mock 整個 `HandleMatrixMessage` 的 Matrix 部分。

**15. 是否有任何本提案會提前踩進 Dispatch / Driver / DB scope？**
提案本身沒有。要守住的邊界：`sender.display_name` 是 contact 原始名稱，不是「司機名」；`chat.type` 是 mid 前綴，不是「司機群」；error code 只到 transport taxonomy，不對應 Alert Office 動作；`request_id` dedupe 只在記憶體、不建表；`origin: backfill` 的取捨（丟棄或入庫）是 Server A 的事、也不在 1A 做 15 天 backup。唯一要提醒的是 F-P2-3 DM 是否 forward，這牽涉營運規則，照 docs 定案、不要在 transport 層決定。

**16. 依風險排序，建議實際施工 commit 順序為何？**
見 G：PR 0 forward 進 source → PR 1 inbound v2（env 切換）→ PR 2 send core 純重構 → PR 3 error classify → PR 4 outbound endpoint → PR 5 Python 改走 outbound。PR 0、1 風險最低且立即解掉 B-1 / B-2 / B-3；PR 2 風險最高但被 PR 4 依賴，所以放中間且獨立驗證。

---

## 附錄 A. 本次查證到的關鍵 code 事實

| 事實 | 位置 |
|---|---|
| DIVA forward 呼叫由 CI 注入，不在 source | `scripts/prepare_diva_build.py`、`.github/workflows/deploy.yml:24-25` |
| `[DIVA_RX]` 只檢查 ToType，不檢查 content type | `pkg/connector/handle_message.go:136-147` |
| 非文字 E2EE 訊息的 `unwrappedText` 為含 keyMaterial 的 JSON | `pkg/connector/handle_message.go:364-372` |
| backfill 也走 `queueIncomingMessage` | `pkg/connector/sync.go:757-798` |
| own-echo 靠 `sentReqSeqs` 過濾；reqSeq = `now % 1e9`（ms） | `pkg/connector/sync.go:1793-1800`、`send_message.go:698`、`diva_adapter.go:186` |
| `sendDIVAText` 複製 group E2EE send 流程 | `pkg/connector/diva_adapter.go:124-220` |
| `HandleMatrixMessage` 820 行，含三種重試 | `pkg/connector/send_message.go:60-853` |
| reply fallback 判斷 | `pkg/connector/send_message.go:798-812`、`:891-903` |
| mention offset 用 UTF-16 | `pkg/connector/send_message.go:958`、`text_offsets.go:53` |
| `SendMessage` 回傳 server-assigned `Message`（含 ID） | `pkg/line/methods.go:380-397` |
| 錯誤判斷全部是字串比對 | `pkg/line/errors.go` |
| `GetRouter()` 只有設 `public_address` 才非 nil；websocket 模式無 HTTP server | mautrix `bridgev2/matrix/connector.go:199-214`、`:294-299` |
| `mxmain` 有 `PostInit` / `PostStart` hook | mautrix `bridgev2/matrix/mxmain/main.go:70-72` |
| `LineConnector.Start` 是可以起 listener 的位置 | `pkg/connector/connector.go:44-50` |

## 附錄 B. 測試執行

Review 容器缺 libolm，`go build ./...` 在 `crypto/libolm` 停住（環境問題，非 code 問題）。改用 pure-Go olm 跑 send path 既有測試：

```
go test -tags goolm ./pkg/connector -run 'TestBuildMention|TestShouldRetry|Reply' -count=1
ok  	github.com/highesttt/matrix-line-messenger/pkg/connector	0.012s
```

mention UTF-16 offset 與 reply fallback 判斷的既有測試在 main @ `a2a4998` 全過，可作為 PR 2 純重構的 baseline。
